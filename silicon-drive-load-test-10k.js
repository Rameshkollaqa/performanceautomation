// silicon-drive-load-test-10k.js  -  k6 version of silicon-drive-load-test-10k.jmx
//
// Each "pair" = one driver + one vendor (same as one JMeter thread):
//   D0 driver login (request OTP)  -> D1 verify OTP (token + driver id from the response)
//   -> D1.5 go online -> D1.6 clear stale trip state
//   -> V4 vendor login (request OTP) -> V5 verify OTP (token + vendor id)
//   then TRIPS_PER_PAIR times:
//     wait RELEASE_WAIT_MS (driver release) -> 6 create trip + dispatch (vendor, to this driver)
//     -> 6b trip overview (employee ids, up to 3 tries)
//     -> D1.7 stay online -> D2 accept (direct load-test call)   [or poll the offer if WAIT_FOR_OFFER=true]
//     -> D3 start to pickup -> D4 arrived -> D5/D5b employee 1+2 pickup -> D6 start to dropoff
//     -> D7/D7b employee 1+2 dropoff -> D7c end trip -> D8 complete trip
//   Every driver step waits STEP_WAIT_MS first (JMeter timers run BEFORE their request).
//
// JMeter "threads + ramp-up" = k6 arrival rate: PAIRS pairs are started evenly over
// RAMP_SECONDS (PAIRS / RAMP_SECONDS per second). Finished pairs free their VU, so
// 10,000 pairs need roughly 1,500-3,000 VUs at once, not 10,000.
//
// DATA FILES (in DATA_DIR, same names and column order as the JMX; first line = header, skipped):
//   driver_data_10k.csv : driver_mobile, driver_otp, driver_country_code   (pair i uses row i)
//   vendor_data_10k.csv : mobileNumber, otp_login, vendorId, vendor_first_name,
//                         vendor_company_name, vendor_profile_photo, email (pair i uses row i, recycled)
//   trip_data_10k.csv   : pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type_id, pickUpTime
//
// ENV (defaults = JMX "Test Data" values):
//   HOST (api.silicondrive.com)  PAIRS (5)  TRIPS_PER_PAIR (1)  RAMP_SECONDS (300)
//   OFFER_POLLS (60)  STEP_WAIT_MS (3000)  RELEASE_WAIT_MS (5000)  WAIT_FOR_OFFER (false)
//   DATA_DIR (.)  MOBILE_COUNTRY_CODE (+91)
// Extra (not in the JMX): LOGIN_WAIT_MS (2000), DETAIL_WAIT_MS (2000), POLL_WAIT_MS (1000),
//   NORMALIZE_COUNTRY_CODE (true: adds a leading + to the driver country code),
//   PICKUP_LEAD_MIN (30), PICKUP_TZ_OFFSET_MIN (330 = IST), PRE_VUS, MAX_VUS, GRACEFUL_STOP, PROTOCOL
//
// Run:  k6 run --console-output=failures.log silicon-drive-load-test-10k.js
// 10k:  k6 run -e PAIRS=10000 -e RAMP_SECONDS=300 -e DATA_DIR=/home/ubuntu/k6/data \
//         --console-output=failures.log silicon-drive-load-test-10k.js

import http from 'k6/http';
import { sleep, check } from 'k6';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter, Trend } from 'k6/metrics';

// ---- Config ("Test Data (edit before run)" + "HTTP Request Defaults") --------
const HOST = __ENV.HOST || 'api.silicondrive.com';
const BASE_URL = `${__ENV.PROTOCOL || 'https'}://${HOST}`;
const MOBILE_COUNTRY_CODE = __ENV.MOBILE_COUNTRY_CODE || '+91';
const DEVICE_TOKEN = 'abcd1234';
const FCM_TOKEN = 'fcm_token_value';
const DATA_DIR = __ENV.DATA_DIR || '.';

const PAIRS = Number(__ENV.PAIRS || 5);
const TRIPS_PER_PAIR = Number(__ENV.TRIPS_PER_PAIR || 1);
const RAMP_SECONDS = Number(__ENV.RAMP_SECONDS || 60);
const OFFER_POLLS = Number(__ENV.OFFER_POLLS || 60);
const STEP_WAIT_MS = Number(__ENV.STEP_WAIT_MS || 3000);
const RELEASE_WAIT_MS = Number(__ENV.RELEASE_WAIT_MS || 5000);
const WAIT_FOR_OFFER = (__ENV.WAIT_FOR_OFFER || 'false') === 'true';

const LOGIN_WAIT_MS = Number(__ENV.LOGIN_WAIT_MS || 2000); // "Wait 2s" timers in the login requests
const DETAIL_WAIT_MS = Number(__ENV.DETAIL_WAIT_MS || 2000); // "Wait 2s" before each 6b try
const POLL_WAIT_MS = Number(__ENV.POLL_WAIT_MS || 1000); // "Poll every 1s"
const NORMALIZE_COUNTRY_CODE = (__ENV.NORMALIZE_COUNTRY_CODE || 'true') !== 'false';

// pickUpTime = now + 30 min as "yyyy-MM-dd HH:mm:ss" (the JMX: __timeShift(...,PT30M)).
// JMeter used the local time of the machine that ran it; 330 = IST.
const PICKUP_LEAD_MIN = Number(__ENV.PICKUP_LEAD_MIN || 30);
const PICKUP_TZ_OFFSET_MIN = Number(__ENV.PICKUP_TZ_OFFSET_MIN || 330);

const ACCEPT = 'application/json, text/plain, */*'; // "Accept Header (global)"

// ---- Test data (CSV Data Set equivalents, column order as in the JMX) --------
// Small built-in CSV reader (no internet / jslib needed): handles quoted fields,
// "" escapes, CRLF or LF line endings, a BOM and empty lines.
function parseCsv(text) {
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  const endRow = () => {
    row.push(field);
    field = '';
    if (!(row.length === 1 && row[0] === '')) rows.push(row); // skip empty lines
    row = [];
  };
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ',') {
      row.push(field);
      field = '';
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      endRow();
    } else {
      field += c;
    }
  }
  if (field !== '' || row.length > 0) endRow();
  return rows;
}

function loadCsv(file, names) {
  const rows = parseCsv(open(`${DATA_DIR}/${file}`)).slice(1); // ignoreFirstLine = true
  return rows.map((r) => {
    const o = {};
    names.forEach((n, i) => {
      o[n] = r[i] === undefined ? '' : String(r[i]).trim();
    });
    return o;
  });
}

const driverData = new SharedArray('drivers', () =>
  loadCsv('driver_data_10k.csv', ['driver_mobile', 'driver_otp', 'driver_country_code'])
);
const vendorData = new SharedArray('vendors', () =>
  loadCsv('vendor_data_10k.csv', [
    'mobileNumber',
    'otp_login',
    'vendorId',
    'vendor_first_name',
    'vendor_company_name',
    'vendor_profile_photo',
    'email',
  ])
);
const tripData = new SharedArray('trips', () =>
  loadCsv('trip_data_10k.csv', [
    'pickup_lat',
    'pickup_lng',
    'dropoff_lat',
    'dropoff_lng',
    'vehicle_type_id',
    'pickUpTime',
  ])
);

// ---- Metrics ---------------------------------------------------------------
const pairsStarted = new Counter('pairs_started');
const pairsLoginFailed = new Counter('pairs_login_failed');
const pairsNoDriverRow = new Counter('pairs_no_driver_row');
const tripsCreated = new Counter('trips_created');
const tripsAccepted = new Counter('trips_accepted');
const tripsCompleted = new Counter('trips_completed');
const tripCreateFailed = new Counter('trip_create_failed');
const offerNotReceived = new Counter('offer_not_received');
const tripFlowDuration = new Trend('trip_flow_duration', true); // create -> complete, includes waits

// ---- Load profile ------------------------------------------------------------
// Rough life of one pair (seconds), used only to size the VU pool.
const LOGIN_SEC = (5 * LOGIN_WAIT_MS) / 1000 + 3;
const TRIP_SEC =
  RELEASE_WAIT_MS / 1000 + DETAIL_WAIT_MS / 1000 + (8 * STEP_WAIT_MS) / 1000 + (WAIT_FOR_OFFER ? 10 : 0) + 4;
const LIFETIME_SEC = LOGIN_SEC + TRIPS_PER_PAIR * TRIP_SEC;
const RATE_PER_SEC = PAIRS / RAMP_SECONDS;
const PRE_VUS = Number(__ENV.PRE_VUS || Math.ceil(RATE_PER_SEC * LIFETIME_SEC * 1.2) + 2);
const MAX_VUS = Number(__ENV.MAX_VUS || Math.max(PRE_VUS, Math.ceil(RATE_PER_SEC * LIFETIME_SEC * 2) + 10));

export const options = {
  scenarios: {
    pairs: {
      // PAIRS iterations (= pairs) started evenly over RAMP_SECONDS
      executor: 'constant-arrival-rate',
      rate: PAIRS,
      timeUnit: `${RAMP_SECONDS}s`,
      duration: `${RAMP_SECONDS}s`,
      preAllocatedVUs: PRE_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: __ENV.GRACEFUL_STOP || `${Math.ceil(LIFETIME_SEC * 3 + 60)}s`,
    },
  },
  thresholds: {
    // at least 99% of the planned trips must complete
    trips_completed: [`count>=${Math.floor(PAIRS * TRIPS_PER_PAIR * 0.99)}`],
    // no pair may be dropped because k6 ran out of VUs
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate<0.05'],
    checks: ['rate>0.95'],
  },
};

// ---- Helpers -----------------------------------------------------------------
function waitMs(ms) {
  if (ms > 0) sleep(ms / 1000);
}

function parse(res) {
  try {
    return res.json();
  } catch (e) {
    return null;
  }
}

function followPath(node, path) {
  let cur = node;
  for (const p of path) {
    if (cur === null || cur === undefined || typeof cur !== 'object') return undefined;
    cur = cur[p];
  }
  return cur;
}

// like JSONPath "$..a.b" : first place in the document (depth first) where the path resolves
function deepFindPath(node, path) {
  if (node === null || typeof node !== 'object') return undefined;
  const direct = followPath(node, path);
  if (direct !== undefined && direct !== null) return direct;
  const kids = Array.isArray(node) ? node : Object.keys(node).map((k) => node[k]);
  for (const c of kids) {
    const r = deepFindPath(c, path);
    if (r !== undefined) return r;
  }
  return undefined;
}

function isUuid(v) {
  return typeof v === 'string' && /^[0-9a-fA-F-]{36}$/.test(v);
}

function pickUpTime() {
  const d = new Date(Date.now() + PICKUP_LEAD_MIN * 60000 + PICKUP_TZ_OFFSET_MIN * 60000);
  const iso = d.toISOString(); // shifted so the UTC fields show the target local time
  return `${iso.substring(0, 10)} ${iso.substring(11, 19)}`;
}

function driverCountryCode(raw) {
  const c = String(raw || MOBILE_COUNTRY_CODE).trim();
  return NORMALIZE_COUNTRY_CODE && !c.startsWith('+') ? `+${c}` : c;
}

// ---- Failure logging (--console-output=failures.log) --------------------------
const failLogCount = {};
function logFailure(name, res, extra) {
  failLogCount[name] = (failLogCount[name] || 0) + 1;
  if (failLogCount[name] > 3) return; // max 3 lines per request name per VU
  console.error(
    `[${name}] pair=${exec.scenario.iterationInTest} status=${res.status} error=${res.error || '-'} ` +
      `${extra || ''} body=${String(res.body).substring(0, 400)}`
  );
}

// One request + check. expect: undefined = HTTP success (status 200-399, like JMeter),
// { status: 201 } = exact code, { contains: 'x' } = HTTP success AND body contains text.
function call(name, method, path, body, headers, expect) {
  const res = http.request(method, `${BASE_URL}${path}`, body === null ? null : JSON.stringify(body), {
    headers,
    tags: { name },
    timeout: '60s',
  });
  let ok = res.status >= 200 && res.status < 400;
  let label = `${name}: HTTP ok`;
  if (expect && expect.status !== undefined) {
    ok = res.status === expect.status;
    label = `${name}: HTTP ${expect.status}`;
  }
  if (expect && expect.contains) {
    ok = ok && String(res.body).indexOf(expect.contains) !== -1;
    label = `${name}: HTTP ok + "${expect.contains}"`;
  }
  check(res, { [label]: () => ok });
  if (!ok) logFailure(name, res);
  return { res, ok };
}

const jsonHeaders = { Accept: ACCEPT, 'Content-Type': 'application/json' };
function driverHeaders(token) {
  return {
    Accept: ACCEPT,
    'Content-Type': 'application/json',
    'X-SD-Lang': 'en',
    Authorization: `Bearer ${token}`,
  };
}

// employee id from the trip overview: $..employees[i].id, fallback
// $.data.employees.employee[i].id, then the JMX regexes on the raw text
function employeeId(json, bodyText, idx) {
  let v = deepFindPath(json, ['employees', idx, 'id']);
  if (v) return String(v);
  v = followPath(json, ['data', 'employees', 'employee', idx, 'id']);
  if (v) return String(v);
  const re =
    idx === 0
      ? /"employee"\s*:\s*\[\s*\{[^}]*"id"\s*:\s*"([^"]+)"/
      : /"employee"\s*:\s*\[[^\]]*\{[^}]*"id"\s*:\s*"[^"]+"[^}]*\}\s*,\s*\{[^}]*"id"\s*:\s*"([^"]+)"/;
  const m = String(bodyText).match(re);
  return m ? m[1] : '';
}

function createTripBody(trip, vendorRow, vendorId, driverId) {
  const pickup = { lat: Number(trip.pickup_lat), lng: Number(trip.pickup_lng) };
  return {
    riderId: '',
    pickup,
    dropoff: { lat: Number(trip.dropoff_lat), lng: Number(trip.dropoff_lng) },
    paymentMethodId: '',
    status: 'requested',
    vehicleType: 'Mini - AC',
    vehicle_type_id: trip.vehicle_type_id,
    noOfEmployee: '2',
    pickupLocation: 'Nextbrain Bangalore, Whitefield, Bengaluru',
    pickUpTime: pickUpTime(),
    dropLocation: 'ITPL, Kadugodi, Bengaluru',
    tripAmount: '500',
    trip_type: 'Pickup',
    isSecurity: false,
    paymentStatus: 'pending',
    cancelReason: '',
    totalDistanceKm: '10',
    totalDurationSec: '1200',
    driverId: '',
    vendorId,
    vendor_first_name: vendorRow.vendor_first_name,
    vendor_company_name: vendorRow.vendor_company_name,
    vendor_phone_number: `${MOBILE_COUNTRY_CODE}${vendorRow.mobileNumber}`,
    vendor_profile_photo: vendorRow.vendor_profile_photo,
    fleetId: '',
    adminId: '',
    createdBy: '',
    driverIds: [driverId],
    employees: [
      {
        fullname: 'Employee 1',
        location: 'Whitefield, Bengaluru',
        phoneNumber: '',
        isSecurity: false,
        pickuplocation: pickup,
        droplocation: null,
        status: 'yet to start',
        emp_pickup_location_name: 'Whitefield',
        emp_drop_location_name: null,
      },
      {
        fullname: 'Employee 2',
        location: '',
        phoneNumber: '',
        isSecurity: false,
        pickuplocation: null,
        droplocation: null,
        status: 'yet to start',
        emp_pickup_location_name: null,
        emp_drop_location_name: null,
      },
    ],
    journey_mode: 'One_way_trip',
    type: 'debit',
    pickup_location_name: 'Whitefield',
    drop_location_name: 'Kadugodi',
  };
}

// ---- One pair (= one JMeter thread) ----------------------------------------------
function runPair(pairIdx, drv, ven) {
  pairsStarted.add(1);

  // D0 - Driver Login - Request OTP
  waitMs(LOGIN_WAIT_MS);
  call(
    'D0 Driver Login - Request OTP',
    'POST',
    '/api/driver-service/drivers/login',
    { country_code: driverCountryCode(drv.driver_country_code), mobile: drv.driver_mobile },
    jsonHeaders
  );

  // D1 - Driver Login - Verify OTP (driver_auth_token = $.data.accessToken, driver_id = $.data.driver.id)
  waitMs(LOGIN_WAIT_MS);
  const d1 = call(
    'D1 Driver Login - Verify OTP',
    'POST',
    '/api/driver-service/drivers/verify-otp',
    { mobile: drv.driver_mobile, otp: drv.driver_otp, deviceToken: DEVICE_TOKEN, fcmToken: FCM_TOKEN },
    jsonHeaders
  );
  const j1 = parse(d1.res);
  const driverToken = (j1 && j1.data && j1.data.accessToken) || 'DRIVER_TOKEN_NOT_FOUND';
  const driverId = (j1 && j1.data && j1.data.driver && j1.data.driver.id) || '';
  const dHeaders = driverHeaders(driverToken);
  const onlinePath = `/api/driver-service/drivers/online/${driverId}`;
  const onlineBody = { drivers_details: { is_online: true } };

  // D1.5 - Driver Go Online
  waitMs(LOGIN_WAIT_MS);
  call('D1.5 Driver Go Online', 'PATCH', onlinePath, onlineBody, dHeaders);

  // D1.6 - Clear stale trip state (stateCleared = request succeeded)
  const d16 = call('D1.6 Clear stale trip state', 'GET', '/api/ride-service/mobile-driver/trip-requests?limit=1', null, dHeaders);
  const stateCleared = d16.res.status >= 200 && d16.res.status < 400;

  // V4 - Vendor Login - Request OTP
  waitMs(LOGIN_WAIT_MS);
  call(
    'V4 Vendor Login - Request OTP',
    'POST',
    '/api/vendor-service/web-vendor/auth/mobile-login?method=login',
    { mobileCountryCode: MOBILE_COUNTRY_CODE, mobileNumber: ven.mobileNumber },
    jsonHeaders
  );

  // V5 - Vendor Login - Verify OTP (auth_token = $..access_token, vendorId = $..user.id)
  waitMs(LOGIN_WAIT_MS);
  const v5 = call(
    'V5 Vendor Login - Verify OTP',
    'POST',
    '/api/vendor-service/web-vendor/auth/verify-mobile-otp',
    {
      mobileCountryCode: MOBILE_COUNTRY_CODE,
      mobileNumber: ven.mobileNumber,
      otp: ven.otp_login,
      deviceToken: DEVICE_TOKEN,
      fcmToken: FCM_TOKEN,
      device_token: DEVICE_TOKEN,
      fcm_token: FCM_TOKEN,
    },
    jsonHeaders
  );
  const j5 = parse(v5.res);
  const authToken = String(deepFindPath(j5, ['access_token']) || 'TOKEN_NOT_FOUND');
  const authTokenDirect = String((j5 && j5.access_token) || '');
  const vendorId = String(deepFindPath(j5, ['user', 'id']) || ven.vendorId || '');
  const vendorBearer = authToken !== 'TOKEN_NOT_FOUND' && authToken !== '' ? authToken : authTokenDirect;

  // If driver + vendor logged in
  const loginOk =
    isUuid(driverId) && isUuid(vendorId) && driverToken !== 'DRIVER_TOKEN_NOT_FOUND' && stateCleared;
  check(true, { 'Pair login ok (driver + vendor)': () => loginOk });
  if (!loginOk) {
    pairsLoginFailed.add(1);
    console.error(
      `[Login failed - pair skipped] pair=${pairIdx} driver_id=${driverId || 'MISSING'} vendorId=${vendorId || 'MISSING'} ` +
        `driverToken=${driverToken === 'DRIVER_TOKEN_NOT_FOUND' ? 'MISSING' : 'ok'} stateCleared=${stateCleared} - trips skipped`
    );
    return;
  }

  // tripsPerPair full trips
  for (let t = 0; t < TRIPS_PER_PAIR; t++) {
    const trip = tripData[(pairIdx * TRIPS_PER_PAIR + t) % tripData.length];
    const tripStarted = Date.now();

    // 6 - Load Test Create Trip + Dispatch (timer "Wait for driver release" runs before it)
    waitMs(RELEASE_WAIT_MS);
    const c = call(
      '6 Load Test Create Trip + Dispatch',
      'POST',
      '/api/ride-service/load-test/trips/create-and-dispatch',
      createTripBody(trip, ven, vendorId, driverId),
      jsonHeaders,
      { status: 201 }
    );
    const jc = parse(c.res);
    const tripId = (jc && jc.tripId) || '';
    const requestTime = (jc && jc.requestedAt) || '';
    if (!c.ok || !tripId) {
      // (the JMX would carry on with TRIP_ID_NOT_FOUND and fail every later step; skipped here)
      tripCreateFailed.add(1);
      continue;
    }
    tripsCreated.add(1);
    const q = `?requestedAt=${encodeURIComponent(requestTime)}`;

    // 6b - Trip Overview (employee IDs), up to 3 tries, "Wait 2s" before each try
    let emp1 = '';
    let emp2 = '';
    let detailTries = 0;
    while (!(emp1 && emp2) && detailTries < 3) {
      detailTries++;
      waitMs(DETAIL_WAIT_MS);
      const o = call(
        '6b Trip Overview (employee IDs)',
        'GET',
        `/api/ride-service/web-vendor/trips/overview/${tripId}${q}`,
        null,
        { Accept: ACCEPT, Authorization: `Bearer ${vendorBearer}` }
      );
      const jo = parse(o.res);
      emp1 = emp1 || employeeId(jo, o.res.body, 0);
      emp2 = emp2 || employeeId(jo, o.res.body, 1);
    }

    let accepted = false;
    if (!WAIT_FOR_OFFER) {
      // D1.7 - Driver Stay Online, D2 - Driver Accept Trip (direct)
      call('D1.7 Driver Stay Online', 'PATCH', onlinePath, onlineBody, dHeaders);
      const a = call(
        'D2 Driver Accept Trip (direct)',
        'POST',
        '/api/ride-service/load-test/trips/accept',
        { tripId, driverId },
        dHeaders,
        { contains: 'trip_accepted' }
      );
      accepted = String(a.res.body).indexOf('trip_accepted') !== -1;
    } else {
      // D2a - poll the driver's trip requests until the offer shows up (up to OFFER_POLLS)
      let requestId = '';
      let offerTries = 0;
      while (!requestId && offerTries < OFFER_POLLS) {
        waitMs(POLL_WAIT_MS);
        const r = call(
          'D2a Driver Trip Requests (offer)',
          'GET',
          `/api/ride-service/mobile-driver/trip-requests?tripId=${tripId}&limit=1`,
          null,
          dHeaders
        );
        offerTries++;
        const jr = parse(r.res);
        const items = (jr && jr.items) || (jr && jr.data && jr.data.items);
        const rid = items && items[0] && items[0].requestId;
        if (rid) requestId = String(rid);
      }
      check(true, { 'D2 offer received': () => requestId !== '' });
      if (requestId) {
        const a = call(
          'D2 Driver Accept Trip (mobile-driver)',
          'POST',
          '/api/ride-service/mobile-driver/dispatch/trip-offer-response',
          { tripId, driverId, requestId, action: 'accept' },
          dHeaders,
          { contains: 'trip_accepted' }
        );
        accepted = String(a.res.body).indexOf('trip_accepted') !== -1;
      } else {
        offerNotReceived.add(1);
        console.error(`[D2 Dispatch offer NOT received] pair=${pairIdx} driver ${driverId} trip ${tripId}`);
      }
    }
    if (!accepted) continue;
    tripsAccepted.add(1);

    // If accepted - pickup to complete (every step waits STEP_WAIT_MS first)
    const progress = `/api/ride-service/mobile-driver/trip-progress-status/${tripId}${q}`;
    const empPath = (id) => `/api/ride-service/mobile-driver/trips/${tripId}/employee-status/${id}${q}`;
    const pickup = { lat: Number(trip.pickup_lat), lng: Number(trip.pickup_lng) };
    const dropoff = { lat: Number(trip.dropoff_lat), lng: Number(trip.dropoff_lng) };
    const steps = [
      ['D3 Start to Pickup', progress, { status: 'start-to-pickup', is_forced: false }],
      ['D4 Arrived at Pickup', progress, { status: 'arrived-at-pickup', is_forced: false, location: pickup }],
      ['D5 Employee 1 Pickup', empPath(emp1), { status: 'pickup', location: pickup }],
      ['D5b Employee 2 Pickup', empPath(emp2), { status: 'pickup', location: pickup }],
      ['D6 Start to Dropoff', progress, { status: 'start-to-dropoff', is_forced: false }],
      ['D7 Employee 1 Dropoff', empPath(emp1), { status: 'dropoff', is_forced: false, location: dropoff }],
      ['D7b Employee 2 Dropoff', empPath(emp2), { status: 'dropoff', is_forced: false, location: dropoff }],
      ['D7c End Trip', progress, { status: 'ended-trip', is_forced: false, location: dropoff }],
    ];
    for (const s of steps) {
      waitMs(STEP_WAIT_MS);
      call(s[0], 'PATCH', s[1], s[2], dHeaders);
    }

    // D8 - Complete Trip (no wait; asserts HTTP 200)
    const d8 = call(
      'D8 Complete Trip',
      'PATCH',
      `/api/ride-service/mobile-driver/completedtrips/${tripId}${q}`,
      { status: 'completed', is_forced: false },
      dHeaders,
      { status: 200 }
    );
    if (d8.ok) {
      tripsCompleted.add(1);
      tripFlowDuration.add(Date.now() - tripStarted);
    }
  }
}

export default function () {
  // pair number = iteration number (pair i uses driver row i, like JMeter's shared CSV)
  const pairIdx = exec.scenario.iterationInTest;
  if (pairIdx >= PAIRS) return;

  const drv = driverData[pairIdx];
  if (!drv) {
    // JMeter: driver CSV has recycle=false + stopThread=true -> the thread simply ends
    pairsNoDriverRow.add(1);
    if (pairIdx === driverData.length) {
      console.error(`driver_data_10k.csv has only ${driverData.length} rows but PAIRS=${PAIRS}; extra pairs are skipped.`);
    }
    return;
  }
  const ven = vendorData[pairIdx % vendorData.length]; // vendor CSV recycles
  runPair(pairIdx, drv, ven);
}