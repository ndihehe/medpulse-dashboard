const express = require('express');
const mqtt = require('mqtt');
const WebSocket = require('ws');
const mysql = require('mysql2');
const cors = require('cors');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 5000;
const APP_TIME_ZONE = process.env.APP_TIME_ZONE || 'Asia/Ho_Chi_Minh';
const MQTT_ROOT_TOPIC = process.env.MQTT_ROOT_TOPIC || 'medpulse_duy';
const BLE_PROTOCOL_VERSION = 1;
const BLE_DEVICE_SEEN_STALE_MS = Number(process.env.BLE_DEVICE_SEEN_STALE_MS || 45000);
const BLE_ALERT_TIMEOUT_MS = Number(process.env.BLE_ALERT_TIMEOUT_MS || 180000);
const STATION_OFFLINE_TIMEOUT_MS = Number(process.env.STATION_OFFLINE_TIMEOUT_MS || 90000);
const BLE_DEVICE_STATUSES = new Set(['OFFLINE', 'ONLINE', 'GRACE', 'ALERT', 'STATION_OFFLINE']);
const FIRMWARE_STATUSES = new Set(['IDLE', 'FALLING', 'IMPACT', 'MOTIONLESS', 'ALERT']);
const FIRMWARE_STATUS_META = {
    IDLE:       { alertLevel: 'safe',    riskWeight: 0,  fall: false, safe: true  },
    FALLING:    { alertLevel: 'warning', riskWeight: 18, fall: false, safe: false },
    IMPACT:     { alertLevel: 'warning', riskWeight: 28, fall: false, safe: false },
    MOTIONLESS: { alertLevel: 'warning', riskWeight: 36, fall: false, safe: false },
    ALERT:      { alertLevel: 'danger',  riskWeight: 46, fall: true,  safe: false },
};

// =========================================================================
// 1. KẾT NỐI MYSQL POOL
// =========================================================================
const db = mysql.createPool({
    host:             process.env.DB_HOST,
    user:             process.env.DB_USER,
    password:         process.env.DB_PASSWORD,
    database:         process.env.DB_DATABASE || process.env.DB_NAME,
    port:             process.env.DB_PORT,
    waitForConnections: true,
    connectionLimit:  10,
    queueLimit:       0,
    enableKeepAlive:  true,
    keepAliveInitialDelay: 10000,
    connectTimeout:   20000,
    ssl: { rejectUnauthorized: false }
});

db.getConnection((err, connection) => {
    if (err) {
        console.error(' Lỗi kết nối cơ sở dữ liệu MySQL:', err.message);
    } else {
        console.log('🔹 Cơ sở dữ liệu MySQL đã kết nối thành công!');
        connection.release();
        // Bảo đảm DB cũ có các cột truy vết nguồn đo trước khi load dữ liệu.
        ensureVitalsSourceColumns(loadPatientProfiles);
        initializeBlePersistence();
    }
});

function ensureVitalsSourceColumns(callback = () => {}) {
    db.query(`
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'vitals_log'
    `, (error, rows = []) => {
        if (error) {
            console.error(' Lỗi kiểm tra schema vitals_log:', error.message);
            callback();
            return;
        }
        const existing = new Set(rows.map(row => row.COLUMN_NAME));
        const additions = [];
        if (!existing.has('measurement_source')) additions.push("ADD COLUMN measurement_source VARCHAR(20) NOT NULL DEFAULT 'DIRECT_MQTT' AFTER risk_score");
        if (!existing.has('source_device_id')) additions.push('ADD COLUMN source_device_id VARCHAR(24) NULL AFTER measurement_source');
        if (!existing.has('source_station_id')) additions.push('ADD COLUMN source_station_id VARCHAR(50) NULL AFTER source_device_id');
        if (!additions.length) {
            callback();
            return;
        }
        db.query(`ALTER TABLE vitals_log ${additions.join(', ')}`, alterError => {
            if (alterError) console.error(' Lỗi nâng cấp schema vitals_log:', alterError.message);
            else console.log(' Đã bổ sung cột truy vết nguồn đo cho vitals_log');
            callback();
        });
    });
}

// =========================================================================
// 2. QUẢN LÝ TRẠNG THÁI BỆNH NHÂN IN-MEMORY
// =========================================================================
const patientsState = {};
const lastSeenByPatient = new Map();
const registeredPatientIds = new Set();
const pendingRegistrations = new Map();
const bleDeviceStates = new Map();
const stationStates = new Map();
const totalPatientIds = Array.from({ length: 16 }, (_, i) => `RFID-${1001 + i}`);

function createDefaultPatientState(id) {
    return {
        id,
        // Thông tin hồ sơ — sẽ được ghi đè bởi loadPatientProfiles()
        name:              null,
        age:               null,
        gender:            null,
        room:              null,
        bed:               null,
        date_of_birth:     null,
        condition_summary: null,
        // Sinh hiệu mặc định
        heartRate:    75,
        spo2:         98,
        temp:         36.5,
        battery:      100,
        rssi:         -55,
        safe:         true,
        fall:         false,
        signalLost:   false,
        ble:          'Chưa có dữ liệu trạm',
        riskScore:    4,
        status:       'IDLE',
        current_status: 'IDLE',
        alertLevel:   'safe',
        statusHistory: [{ status: 'IDLE', at: new Date().toISOString() }],
        alert:        false,
        lastUpdate:   new Date(),
        hasRealData:  false,
        lastMeasurementAt: null,
        measurementSource: null,
        sourceDeviceId: null,
        sourceStationId: null,
    };
}

function ensurePatientState(id) {
    if (!patientsState[id]) {
        patientsState[id] = createDefaultPatientState(id);
    }
    return patientsState[id];
}

function getRegisteredPatientStates() {
    return Object.values(patientsState)
        .filter(patient => registeredPatientIds.has(patient.id));
}

function serializePendingRegistration(entry) {
    return {
        id: entry.id,
        detectedAt: entry.detectedAt,
        lastSeenAt: entry.lastSeenAt,
        hasMeasurement: entry.patient.hasRealData === true,
        latestData: serializePatientState(entry.patient),
    };
}

totalPatientIds.forEach(id => ensurePatientState(id));

// =========================================================================
// [FIX] Load hồ sơ bệnh nhân từ bảng patients vào patientsState
// =========================================================================
function loadPatientProfiles() {
    db.query('SELECT * FROM patients', (err, rows) => {
        if (err) {
            console.error(' Lỗi load hồ sơ bệnh nhân từ DB:', err.message);
            return;
        }
        rows.forEach(row => {
            // Đảm bảo slot RAM tồn tại dù id này chưa trong totalPatientIds
            const state = ensurePatientState(row.id);
            registeredPatientIds.add(row.id);
            pendingRegistrations.delete(row.id);
            state.name              = row.name              || null;
            state.age               = row.age               ?? null;
            state.gender            = row.gender            || null;
            state.room              = row.room              || null;
            state.bed               = row.bed               || null;
            state.date_of_birth     = row.date_of_birth      || null;
            state.condition_summary = row.condition_summary || null;
        });
        console.log(` Đã load ${rows.length} hồ sơ bệnh nhân từ bảng patients`);

        loadLatestVitalsFromDatabase(() => {
            // Sau khi load xong profile và lần đo gần nhất, broadcast INITIAL_SYNC lại để web nhận ngay
            broadcastToDashboards({
                type: 'INITIAL_SYNC',
                data: getRegisteredPatientStates().map(serializePatientState),
            });
            broadcastToDashboards({
                type: 'PENDING_REGISTRATIONS',
                data: Array.from(pendingRegistrations.values()).map(serializePendingRegistration),
            });
        });
    });
}

// =========================================================================
// HELPER FUNCTIONS
// =========================================================================
function normalizeFirmwareStatus(value) {
    const status = String(value || '').trim().toUpperCase();
    return FIRMWARE_STATUSES.has(status) ? status : null;
}

function getFirmwareStatusMeta(value) {
    const status = normalizeFirmwareStatus(value) || 'IDLE';
    return FIRMWARE_STATUS_META[status] || FIRMWARE_STATUS_META.IDLE;
}

function applyFirmwareStatusToPatient(patient, firmwareStatus) {
    const status = normalizeFirmwareStatus(firmwareStatus) || 'IDLE';
    const meta   = getFirmwareStatusMeta(status);
    patient.status         = status;
    patient.current_status = status;
    patient.fall           = meta.fall;
    patient.safe           = meta.safe;
    return meta;
}

function syncFallSafeState(patient, metric, value) {
    if (metric === 'fall') {
        if (value !== 0 && value !== 1) return false;
        patient.fall = value === 1;
        patient.safe = value === 0;
        return true;
    }
    if (metric === 'safe') {
        if (value !== 0 && value !== 1) return false;
        patient.safe = value === 1;
        patient.fall = value === 0;
        return true;
    }
    if (metric === 'status') {
        const meta   = applyFirmwareStatusToPatient(patient, value);
        patient.fall = meta.fall;
        patient.safe = meta.safe;
        return true;
    }
    return false;
}

function hasMeasurementPayload(liveData) {
    return Number.isFinite(liveData?.heartRate)
        || Number.isFinite(liveData?.spo2)
        || Number.isFinite(liveData?.temp);
}

// [FIX] serializePatientState — đảm bảo name/age/gender/room luôn có mặt
function serializePatientState(patient) {
    return {
        ...patient,
        current_status:    patient.current_status || patient.status || 'IDLE',
        name:              patient.name              || patient.id,
        age:               patient.age              ?? null,
        gender:            patient.gender           || null,
        room:              patient.room             || null,
        bed:               patient.bed              || null,
        condition_summary: patient.condition_summary || null,
        hasRealData:       patient.hasRealData === true,
        lastMeasurementAt: patient.lastMeasurementAt || null,
    };
}

function applyLatestVitalsToPatientState(patient, vitals) {
    if (!patient || !vitals) return patient;
    patient.heartRate = Number(vitals.heartRate);
    patient.spo2 = Number(vitals.spo2);
    patient.temp = Number(vitals.temp);
    patient.battery = Number(vitals.battery ?? patient.battery ?? 100);
    patient.rssi = Number(vitals.rssi ?? patient.rssi ?? -55);
    patient.fall = Boolean(vitals.fall);
    patient.safe = vitals.safe !== false;
    patient.status = normalizeFirmwareStatus(vitals.status) || patient.status || 'IDLE';
    patient.current_status = normalizeFirmwareStatus(vitals.current_status) || patient.status;
    patient.alertLevel = vitals.alertLevel || patient.alertLevel || 'safe';
    patient.riskScore = Number(vitals.riskScore ?? patient.riskScore ?? 4);
    patient.lastUpdate = vitals.time ? new Date(vitals.time) : patient.lastUpdate;
    patient.lastMeasurementAt = vitals.time ? new Date(vitals.time).toISOString() : patient.lastMeasurementAt;
    patient.hasRealData = true;
    patient.measurementSource = vitals.measurementSource || vitals.measurement_source || patient.measurementSource || 'DIRECT_MQTT';
    patient.sourceDeviceId = vitals.sourceDeviceId || vitals.source_device_id || null;
    patient.sourceStationId = vitals.sourceStationId || vitals.source_station_id || null;
    patient.ble = patient.signalLost ? 'Không có gói tin mới'
        : patient.battery < 20 ? 'Pin trạm yếu'
        : 'Đã nhận dữ liệu trạm';
    return patient;
}

function applyStoredVitalsAndRecalculate(patient, vitals) {
    applyLatestVitalsToPatientState(patient, vitals);
    const measurementUpdate = patient.lastUpdate;
    const measurementAt = patient.lastMeasurementAt;
    patient.signalLost = false;
    processPatientMetricsCalculation(patient.id);
    patient.lastUpdate = measurementUpdate;
    patient.lastMeasurementAt = measurementAt;
    return patient;
}

function resetPatientMeasurementState(patient) {
    if (!patient) return patient;
    patient.heartRate = 75;
    patient.spo2 = 98;
    patient.temp = 36.5;
    patient.battery = 100;
    patient.rssi = -55;
    patient.safe = true;
    patient.fall = false;
    patient.signalLost = false;
    patient.ble = 'Chưa có dữ liệu trạm';
    patient.riskScore = 4;
    patient.status = 'IDLE';
    patient.current_status = 'IDLE';
    patient.alertLevel = 'safe';
    patient.alert = false;
    patient.hasRealData = false;
    patient.lastMeasurementAt = null;
    patient.measurementSource = null;
    patient.sourceDeviceId = null;
    patient.sourceStationId = null;
    return patient;
}

function queryLatestVitals(callback) {
    const query = `
        SELECT vl.patient_id AS id,
               vl.heart_rate AS heartRate,
               vl.spo2,
               vl.temp,
               vl.battery,
               vl.rssi,
               vl.fall_status AS fall,
               vl.is_safe AS safe,
               vl.device_status AS status,
               vl.current_status,
               vl.status_level AS alertLevel,
               vl.risk_score AS riskScore,
               vl.measurement_source AS measurementSource,
               vl.source_device_id AS sourceDeviceId,
               vl.source_station_id AS sourceStationId,
               vl.recorded_at AS time
        FROM vitals_log vl
        INNER JOIN (
            SELECT patient_id, MAX(recorded_at) AS latest_time
            FROM vitals_log
            GROUP BY patient_id
        ) latest
            ON latest.patient_id = vl.patient_id
           AND latest.latest_time = vl.recorded_at
    `;
    db.query(query, callback);
}

function loadLatestVitalsFromDatabase(callback = () => {}) {
    queryLatestVitals((err, rows) => {
        if (err) {
            console.error(' Lỗi load lần đo gần nhất từ DB:', err.message);
            callback(err);
            return;
        }
        Object.values(patientsState).forEach(resetPatientMeasurementState);
        rows.forEach(row => {
            applyStoredVitalsAndRecalculate(ensurePatientState(row.id), row);
        });
        console.log(` Đã load ${rows.length} lần đo gần nhất từ vitals_log`);
        callback(null, rows);
    });
}

// =========================================================================
// 3. THUẬT TOÁN TÍNH ĐIỂM RỦI RO
// =========================================================================
function processPatientMetricsCalculation(patientId) {
    const patient        = patientsState[patientId];
    const hr             = patient.heartRate;
    const spo2           = patient.spo2;
    const temp           = patient.temp;
    const isSignalLost   = patient.signalLost;
    const firmwareStatus = normalizeFirmwareStatus(patient.status) || 'IDLE';
    const statusMeta     = applyFirmwareStatusToPatient(patient, firmwareStatus);
    const isSafe         = patient.safe;
    const isFall         = patient.fall;

    let score = (100 - spo2) * 1.2
              + Math.abs(hr - 78) * 1.1
              + Math.max(temp - 37.3, 0) * 18
              + statusMeta.riskWeight;

    patient.riskScore = Math.max(4, Math.min(99, Math.round(score)));

    if (statusMeta.alertLevel === 'danger' || isFall || spo2 < 92 || hr > 125) {
        patient.alertLevel = 'danger';
    } else if (statusMeta.alertLevel !== 'safe' || !isSafe || spo2 < 94 || hr > 110 || hr < 55 || temp > 37.8) {
        patient.alertLevel = 'warning';
    } else {
        patient.alertLevel = 'safe';
    }

    patient.alert      = (firmwareStatus !== 'IDLE' || patient.alertLevel !== 'safe');
    patient.lastUpdate = new Date();

    patient.statusHistory = Array.isArray(patient.statusHistory) ? patient.statusHistory : [];
    const lastStatus = patient.statusHistory[patient.statusHistory.length - 1];
    if (!lastStatus || lastStatus.status !== firmwareStatus) {
        patient.statusHistory = [
            ...patient.statusHistory,
            { status: firmwareStatus, at: patient.lastUpdate.toISOString() },
        ].slice(-25);
    }

    patient.ble = isSignalLost ? 'Không có gói tin mới'
                : patient.battery < 20 ? 'Pin trạm yếu'
                : 'Đang nhận dữ liệu trạm';

    return patient;
}

function publishPatientUpdate(patientId, options = {}) {
    const { saveToDatabase = true, markAsMeasurement = saveToDatabase } = options;
    const data = processPatientMetricsCalculation(patientId);
    if (markAsMeasurement) {
        data.hasRealData = true;
        data.lastMeasurementAt = data.lastUpdate.toISOString();
    }
    broadcastToDashboards({ type: 'PATIENT_UPDATE', data: serializePatientState(data) });
    if (saveToDatabase) {
        saveVitalsToDatabase(data);
    }
}

function markPatientOffline(patientId) {
    const patient = patientsState[patientId];
    if (!patient || patient.signalLost) return;
    patient.signalLost  = true;
    patient.ble         = 'Không có gói tin mới';
    patient.lastUpdate  = new Date();
    if (!registeredPatientIds.has(patientId)) {
        const pending = pendingRegistrations.get(patientId);
        if (pending) {
            pending.lastSeenAt = patient.lastUpdate.toISOString();
            pending.patient = patient;
        }
        return;
    }
    publishPatientUpdate(patientId, { saveToDatabase: false, markAsMeasurement: false });
}

// Không suy luận mất BLE từ khoảng cách giữa hai lần đo sinh hiệu.
// Trạng thái thiết bị đeo được quản lý độc lập bằng state machine BLE bên dưới.

// =========================================================================
// 4. BLE DEVICE + STATION STATE MACHINE
// =========================================================================
function toIsoOrNull(value) {
    if (!value) return null;
    const date = value instanceof Date ? value : new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function localDateKey(value = new Date()) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone: APP_TIME_ZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    }).formatToParts(value instanceof Date ? value : new Date(value));
    const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day}`;
}

function isValidProtocolIdentifier(value, maxLength) {
    return typeof value === 'string'
        && value.length >= 1
        && value.length <= maxLength
        && /^[A-Za-z0-9_-]+$/.test(value);
}

function isBleDeviceSeenToday(device, now = new Date()) {
    return Boolean(device.firstSeenToday)
        && localDateKey(device.firstSeenToday) === localDateKey(now);
}

function createBleDeviceState(deviceId) {
    return {
        deviceId,
        patientId: null,
        stationId: null,
        status: 'OFFLINE',
        sessionActive: false,
        sessionBlocked: false,
        firstSeenToday: null,
        lastBleSeen: null,
        disconnectedAt: null,
        alertStartedAt: null,
        rssi: null,
        battery: null,
        lastVitalsSequence: null,
        lastMeasurementAt: null,
        lastEvent: null,
        lastUpdated: new Date(),
    };
}

function createStationState(stationId) {
    return {
        stationId,
        status: 'OFFLINE',
        lastSeenAt: null,
        wifiRssi: null,
        bleScanner: false,
        uptimeMs: null,
        lastUpdated: new Date(),
    };
}

function serializeBleDeviceState(device) {
    const patient = device.patientId ? patientsState[device.patientId] : null;
    return {
        deviceId: device.deviceId,
        patientId: device.patientId,
        patientName: patient?.name || null,
        stationId: device.stationId,
        status: BLE_DEVICE_STATUSES.has(device.status) ? device.status : 'OFFLINE',
        sessionActive: device.sessionActive === true,
        sessionBlocked: device.sessionBlocked === true,
        seenToday: isBleDeviceSeenToday(device),
        firstSeenToday: toIsoOrNull(device.firstSeenToday),
        lastBleSeen: toIsoOrNull(device.lastBleSeen),
        disconnectedAt: toIsoOrNull(device.disconnectedAt),
        alertStartedAt: toIsoOrNull(device.alertStartedAt),
        rssi: Number.isFinite(device.rssi) ? device.rssi : null,
        battery: Number.isFinite(device.battery) && device.battery >= 0 ? device.battery : null,
        lastVitalsSequence: Number.isInteger(device.lastVitalsSequence) ? device.lastVitalsSequence : null,
        lastMeasurementAt: toIsoOrNull(device.lastMeasurementAt),
        lastEvent: device.lastEvent,
        lastUpdated: toIsoOrNull(device.lastUpdated),
    };
}

function serializeStationState(station) {
    return {
        stationId: station.stationId,
        status: station.status,
        lastSeenAt: toIsoOrNull(station.lastSeenAt),
        wifiRssi: Number.isFinite(station.wifiRssi) ? station.wifiRssi : null,
        bleScanner: station.bleScanner === true,
        uptimeMs: Number.isFinite(station.uptimeMs) ? station.uptimeMs : null,
        lastUpdated: toIsoOrNull(station.lastUpdated),
    };
}

function persistBleDeviceState(device) {
    const query = `
        INSERT INTO ble_devices
        (device_id, patient_id, station_id, ble_status, session_active, session_blocked,
         first_seen_today, last_ble_seen, disconnected_at, alert_started_at,
         rssi, battery, last_event)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            patient_id = VALUES(patient_id),
            station_id = VALUES(station_id),
            ble_status = VALUES(ble_status),
            session_active = VALUES(session_active),
            session_blocked = VALUES(session_blocked),
            first_seen_today = VALUES(first_seen_today),
            last_ble_seen = VALUES(last_ble_seen),
            disconnected_at = VALUES(disconnected_at),
            alert_started_at = VALUES(alert_started_at),
            rssi = VALUES(rssi),
            battery = VALUES(battery),
            last_event = VALUES(last_event)
    `;
    db.query(query, [
        device.deviceId,
        device.patientId,
        device.stationId,
        device.status,
        device.sessionActive ? 1 : 0,
        device.sessionBlocked ? 1 : 0,
        device.firstSeenToday,
        device.lastBleSeen,
        device.disconnectedAt,
        device.alertStartedAt,
        device.rssi,
        device.battery,
        device.lastEvent,
    ], err => {
        if (err) console.error(` Lỗi lưu trạng thái BLE ${device.deviceId}:`, err.message);
    });
}

function persistStationState(station) {
    const query = `
        INSERT INTO stations
        (station_id, station_status, last_seen_at, wifi_rssi, ble_scanner, uptime_ms)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            station_status = VALUES(station_status),
            last_seen_at = VALUES(last_seen_at),
            wifi_rssi = VALUES(wifi_rssi),
            ble_scanner = VALUES(ble_scanner),
            uptime_ms = VALUES(uptime_ms)
    `;
    db.query(query, [
        station.stationId,
        station.status,
        station.lastSeenAt,
        station.wifiRssi,
        station.bleScanner ? 1 : 0,
        station.uptimeMs,
    ], err => {
        if (err) console.error(` Lỗi lưu trạng thái trạm ${station.stationId}:`, err.message);
    });
}

function broadcastBleDeviceState(device, eventType = 'BLE_DEVICE_UPDATE') {
    broadcastToDashboards({ type: eventType, data: serializeBleDeviceState(device) });
}

function resetBleDeviceForNewDay(device, now = new Date()) {
    device.status = 'OFFLINE';
    device.sessionActive = false;
    device.sessionBlocked = false;
    device.firstSeenToday = null;
    device.lastBleSeen = null;
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'DAILY_RESET';
    device.lastUpdated = now;
    persistBleDeviceState(device);
    broadcastBleDeviceState(device);
}

function loadBleDeviceStates() {
    db.query('SELECT * FROM ble_devices', (err, rows) => {
        if (err) {
            console.error(' Lỗi load thiết bị BLE:', err.message);
            return;
        }
        const now = new Date();
        rows.forEach(row => {
            const device = createBleDeviceState(row.device_id);
            device.patientId = row.patient_id || null;
            device.stationId = row.station_id || null;
            device.status = BLE_DEVICE_STATUSES.has(row.ble_status) ? row.ble_status : 'OFFLINE';
            device.sessionActive = Boolean(row.session_active);
            device.sessionBlocked = Boolean(row.session_blocked);
            device.firstSeenToday = row.first_seen_today ? new Date(row.first_seen_today) : null;
            device.lastBleSeen = row.last_ble_seen ? new Date(row.last_ble_seen) : null;
            device.disconnectedAt = row.disconnected_at ? new Date(row.disconnected_at) : null;
            device.alertStartedAt = row.alert_started_at ? new Date(row.alert_started_at) : null;
            device.rssi = row.rssi === null ? null : Number(row.rssi);
            device.battery = row.battery === null ? null : Number(row.battery);
            device.lastEvent = row.last_event || null;
            device.lastUpdated = row.last_updated ? new Date(row.last_updated) : now;
            bleDeviceStates.set(device.deviceId, device);
            if (device.firstSeenToday && !isBleDeviceSeenToday(device, now)) {
                resetBleDeviceForNewDay(device, now);
            }
        });
        console.log(` Đã load ${rows.length} thiết bị BLE`);
        broadcastToDashboards({
            type: 'BLE_DEVICES_SYNC',
            data: Array.from(bleDeviceStates.values()).map(serializeBleDeviceState),
        });
    });
}

function loadStationStates() {
    db.query('SELECT * FROM stations', (err, rows) => {
        if (err) {
            console.error(' Lỗi load trạng thái trạm:', err.message);
            return;
        }
        rows.forEach(row => {
            const station = createStationState(row.station_id);
            station.status = 'OFFLINE';
            station.lastSeenAt = row.last_seen_at ? new Date(row.last_seen_at) : null;
            station.wifiRssi = row.wifi_rssi === null ? null : Number(row.wifi_rssi);
            station.bleScanner = Boolean(row.ble_scanner);
            station.uptimeMs = row.uptime_ms === null ? null : Number(row.uptime_ms);
            station.lastUpdated = row.last_updated ? new Date(row.last_updated) : new Date();
            stationStates.set(station.stationId, station);
        });
        console.log(` Đã load ${rows.length} trạm BLE`);
    });
}

function initializeBlePersistence() {
    const stationTable = `
        CREATE TABLE IF NOT EXISTS stations (
            station_id VARCHAR(50) PRIMARY KEY,
            station_status VARCHAR(20) NOT NULL DEFAULT 'OFFLINE',
            last_seen_at DATETIME(3) NULL,
            wifi_rssi INT NULL,
            ble_scanner TINYINT(1) NOT NULL DEFAULT 0,
            uptime_ms BIGINT UNSIGNED NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    `;
    const deviceTable = `
        CREATE TABLE IF NOT EXISTS ble_devices (
            device_id VARCHAR(24) PRIMARY KEY,
            patient_id VARCHAR(50) NULL,
            station_id VARCHAR(50) NULL,
            ble_status VARCHAR(20) NOT NULL DEFAULT 'OFFLINE',
            session_active TINYINT(1) NOT NULL DEFAULT 0,
            session_blocked TINYINT(1) NOT NULL DEFAULT 0,
            first_seen_today DATETIME(3) NULL,
            last_ble_seen DATETIME(3) NULL,
            disconnected_at DATETIME(3) NULL,
            alert_started_at DATETIME(3) NULL,
            rssi INT NULL,
            battery INT NULL,
            last_event VARCHAR(30) NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_ble_patient (patient_id),
            INDEX idx_ble_station (station_id),
            INDEX idx_ble_status (ble_status)
        ) ENGINE=InnoDB
    `;
    db.query(stationTable, err => {
        if (err) console.error(' Lỗi tạo bảng stations:', err.message);
        else loadStationStates();
    });
    db.query(deviceTable, err => {
        if (err) console.error(' Lỗi tạo bảng ble_devices:', err.message);
        else loadBleDeviceStates();
    });
}

function getOrCreateStationState(stationId) {
    if (!stationStates.has(stationId)) stationStates.set(stationId, createStationState(stationId));
    return stationStates.get(stationId);
}

function getOrCreateBleDeviceState(deviceId) {
    if (!bleDeviceStates.has(deviceId)) bleDeviceStates.set(deviceId, createBleDeviceState(deviceId));
    return bleDeviceStates.get(deviceId);
}

function validateStationHeartbeatPayload(payload, topicStationId) {
    if (!payload || payload.protocolVersion !== BLE_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (payload.event !== 'STATION_HEARTBEAT' || payload.online !== true || payload.bleScanner !== true) return 'heartbeat không đúng giao thức';
    if (!Number.isFinite(payload.wifiRssi) || payload.wifiRssi < -127 || payload.wifiRssi > 20) return 'wifiRssi không hợp lệ';
    if (!Number.isFinite(payload.uptimeMs) || payload.uptimeMs < 0) return 'uptimeMs không hợp lệ';
    return null;
}

function validateBlePresencePayload(payload, topicStationId) {
    if (!payload || payload.protocolVersion !== BLE_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (!isValidProtocolIdentifier(payload.deviceId, 24)) return 'deviceId không hợp lệ';
    const validPair = (payload.event === 'SEEN' && payload.bleStatus === 'CONNECTED')
        || (payload.event === 'LOST' && payload.bleStatus === 'DISCONNECTED');
    if (!validPair) return 'event và bleStatus không đồng bộ';
    if (!Number.isFinite(payload.rssi) || payload.rssi < -127 || payload.rssi > 20) return 'rssi không hợp lệ';
    if (!Number.isFinite(payload.battery) || payload.battery < -1 || payload.battery > 100) return 'battery không hợp lệ';
    if (!Number.isFinite(payload.stationUptimeMs) || payload.stationUptimeMs < 0) return 'stationUptimeMs không hợp lệ';
    if (!Number.isFinite(payload.lastSeenAgoMs) || payload.lastSeenAgoMs < 0 || payload.lastSeenAgoMs > 86400000) return 'lastSeenAgoMs không hợp lệ';
    return null;
}

function validateGatewayVitalsPayload(payload, topicStationId, topicDeviceId) {
    if (!payload || payload.protocolVersion !== BLE_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (!isValidProtocolIdentifier(topicDeviceId, 24) || payload.deviceId !== topicDeviceId) return 'deviceId không khớp topic';
    if (!Number.isInteger(payload.sequence) || payload.sequence < 0 || payload.sequence > 255) return 'sequence không hợp lệ';
    if (!Number.isFinite(payload.heartRate) || payload.heartRate < 25 || payload.heartRate > 240) return 'heartRate không hợp lệ';
    if (!Number.isFinite(payload.spo2) || payload.spo2 < 50 || payload.spo2 > 100) return 'spo2 không hợp lệ';
    if (!Number.isFinite(payload.temp) || payload.temp < 25 || payload.temp > 50) return 'temp không hợp lệ';
    if (!Number.isFinite(payload.battery) || payload.battery < 0 || payload.battery > 100) return 'battery không hợp lệ';
    if (!Number.isFinite(payload.rssi) || payload.rssi < -127 || payload.rssi > 20) return 'rssi không hợp lệ';
    if (!Number.isFinite(payload.stationUptimeMs) || payload.stationUptimeMs < 0) return 'stationUptimeMs không hợp lệ';
    if (typeof payload.charging !== 'boolean' || typeof payload.fallDetected !== 'boolean') return 'flags không hợp lệ';
    if (!normalizeFirmwareStatus(payload.firmwareStatus)) return 'firmwareStatus không hợp lệ';
    return null;
}

function markStationOnlineFromPayload(stationId, payload, now = new Date()) {
    const station = getOrCreateStationState(stationId);
    const wasOffline = station.status !== 'ONLINE';
    station.status = 'ONLINE';
    station.lastSeenAt = now;
    station.wifiRssi = Number.isFinite(payload.wifiRssi) ? Math.round(payload.wifiRssi) : station.wifiRssi;
    station.bleScanner = payload.bleScanner !== false;
    station.uptimeMs = Number.isFinite(payload.uptimeMs) ? Math.round(payload.uptimeMs) : station.uptimeMs;
    station.lastUpdated = now;
    persistStationState(station);
    broadcastToDashboards({ type: 'STATION_UPDATE', data: serializeStationState(station) });

    if (wasOffline) {
        for (const device of bleDeviceStates.values()) {
            if (device.stationId !== stationId || device.status !== 'STATION_OFFLINE') continue;
            if (device.sessionActive && isBleDeviceSeenToday(device, now)) {
                device.status = 'GRACE';
                device.disconnectedAt = now;
                device.lastEvent = 'STATION_RECOVERED_WAITING_BLE';
            } else {
                device.status = 'OFFLINE';
            }
            device.lastUpdated = now;
            persistBleDeviceState(device);
            broadcastBleDeviceState(device);
        }
    }
    return station;
}

function handleStationHeartbeatPayload(payload, stationId) {
    const now = new Date();
    markStationOnlineFromPayload(stationId, payload, now);
}

function handleBlePresencePayload(payload, stationId) {
    const now = new Date();
    markStationOnlineFromPayload(stationId, {
        uptimeMs: payload.stationUptimeMs,
        bleScanner: true,
    }, now);

    const device = getOrCreateBleDeviceState(payload.deviceId);
    device.stationId = stationId;
    device.rssi = Math.round(payload.rssi);
    device.battery = payload.battery >= 0 ? Math.round(payload.battery) : device.battery;
    device.lastEvent = payload.event;
    device.lastUpdated = now;

    if (payload.event === 'SEEN') {
        if (!isBleDeviceSeenToday(device, now)) device.firstSeenToday = now;
        device.lastBleSeen = now;
        device.disconnectedAt = null;
        device.alertStartedAt = null;
        if (device.sessionBlocked) {
            device.sessionActive = false;
            device.status = 'OFFLINE';
            device.lastEvent = 'SEEN_SESSION_BLOCKED';
        } else {
            device.sessionActive = true;
            device.status = 'ONLINE';
        }
    } else if (!device.patientId || !device.sessionActive || !isBleDeviceSeenToday(device, now)) {
        device.status = 'OFFLINE';
        device.disconnectedAt = null;
        device.alertStartedAt = null;
    } else {
        const seenAgoMs = Math.min(Math.max(payload.lastSeenAgoMs, 0), 86400000);
        const reportedLastSeen = new Date(now.getTime() - seenAgoMs);
        if (!device.lastBleSeen || reportedLastSeen > device.lastBleSeen) device.lastBleSeen = reportedLastSeen;
        device.status = 'GRACE';
        device.disconnectedAt = now;
        device.alertStartedAt = null;
    }

    persistBleDeviceState(device);
    broadcastBleDeviceState(device, payload.event === 'SEEN' ? 'BLE_DEVICE_SEEN' : 'BLE_DEVICE_LOST');
}

function applyIncomingVitals(patientId, liveData, source = {}) {
    ensurePatientState(patientId);
    lastSeenByPatient.set(patientId, Date.now());

    const patient = patientsState[patientId];
    const isMeasurementPayload = hasMeasurementPayload(liveData);
    if (Number.isFinite(liveData.heartRate)) patient.heartRate = Math.round(liveData.heartRate);
    if (Number.isFinite(liveData.spo2)) patient.spo2 = Math.round(liveData.spo2);
    if (Number.isFinite(liveData.temp)) patient.temp = Number(liveData.temp.toFixed(1));
    if (Number.isFinite(liveData.battery)) patient.battery = Number(liveData.battery.toFixed(1));
    if (Number.isFinite(liveData.rssi)) patient.rssi = Math.round(liveData.rssi);
    patient.signalLost = liveData.signalLost === true || liveData.signalLost === 1;
    patient.measurementSource = source.type || 'DIRECT_MQTT';
    patient.sourceDeviceId = source.deviceId || null;
    patient.sourceStationId = source.stationId || null;

    const normalizedStatus = normalizeFirmwareStatus(liveData.firmwareStatus || 'IDLE');
    if (normalizedStatus) syncFallSafeState(patient, 'status', normalizedStatus);

    // Chỉ luồng RFID/MQTT trực tiếp được phép tạo thông báo đăng ký hồ sơ.
    if (!registeredPatientIds.has(patientId)) {
        if (source.type === 'BLE_GATEWAY') return false;
        if (isMeasurementPayload) {
            patient.hasRealData = true;
            patient.lastUpdate = new Date();
            patient.lastMeasurementAt = patient.lastUpdate.toISOString();
        }
        const processed = processPatientMetricsCalculation(patientId);
        const existing = pendingRegistrations.get(patientId);
        const pending = {
            id: patientId,
            detectedAt: existing?.detectedAt || new Date().toISOString(),
            lastSeenAt: new Date().toISOString(),
            patient: processed,
        };
        pendingRegistrations.set(patientId, pending);
        if (!existing) {
            broadcastToDashboards({ type: 'UNKNOWN_PATIENT_DETECTED', data: serializePendingRegistration(pending) });
            console.log(` Phát hiện RFID chưa đăng ký: ${patientId}`);
        }
        return false;
    }

    if (!patient.signalLost) {
        publishPatientUpdate(patientId, {
            saveToDatabase: isMeasurementPayload,
            markAsMeasurement: isMeasurementPayload,
        });
    } else {
        const updated = processPatientMetricsCalculation(patientId);
        broadcastToDashboards({ type: 'PATIENT_UPDATE', data: serializePatientState(updated) });
    }
    return true;
}

function handleGatewayVitalsPayload(payload, stationId, deviceId) {
    const now = new Date();
    markStationOnlineFromPayload(stationId, {
        uptimeMs: payload.stationUptimeMs,
        bleScanner: true,
    }, now);

    const device = getOrCreateBleDeviceState(deviceId);
    device.stationId = stationId;
    device.rssi = Math.round(payload.rssi);
    device.battery = Math.round(payload.battery);
    device.lastBleSeen = now;
    device.lastUpdated = now;

    if (!device.patientId) {
        device.lastEvent = 'VITALS_REJECTED_UNASSIGNED';
        persistBleDeviceState(device);
        broadcastBleDeviceState(device, 'BLE_VITALS_REJECTED');
        return;
    }
    if (!device.sessionActive || device.sessionBlocked) {
        device.lastEvent = 'VITALS_REJECTED_SESSION_INACTIVE';
        persistBleDeviceState(device);
        broadcastBleDeviceState(device, 'BLE_VITALS_REJECTED');
        return;
    }
    if (device.lastVitalsSequence === payload.sequence) return;

    device.lastVitalsSequence = payload.sequence;
    device.lastMeasurementAt = now;
    device.lastEvent = 'VITALS_ACCEPTED';
    device.status = 'ONLINE';
    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_VITALS_ACCEPTED');

    applyIncomingVitals(device.patientId, {
        ...payload,
        signalLost: false,
    }, {
        type: 'BLE_GATEWAY',
        deviceId,
        stationId,
    });
}

function evaluateBleStateMachine() {
    const now = new Date();
    const nowMs = now.getTime();

    for (const station of stationStates.values()) {
        if (station.status === 'ONLINE' && station.lastSeenAt
            && nowMs - station.lastSeenAt.getTime() >= STATION_OFFLINE_TIMEOUT_MS) {
            station.status = 'OFFLINE';
            station.lastUpdated = now;
            persistStationState(station);
            broadcastToDashboards({ type: 'STATION_UPDATE', data: serializeStationState(station) });
        }
    }

    for (const device of bleDeviceStates.values()) {
        if (device.firstSeenToday && !isBleDeviceSeenToday(device, now)) {
            resetBleDeviceForNewDay(device, now);
            continue;
        }
        if (!device.patientId || !device.sessionActive || !isBleDeviceSeenToday(device, now)) continue;

        const station = device.stationId ? stationStates.get(device.stationId) : null;
        const stationOnline = station?.status === 'ONLINE';
        if (!stationOnline) {
            if (device.status !== 'STATION_OFFLINE') {
                device.status = 'STATION_OFFLINE';
                device.disconnectedAt = null;
                device.alertStartedAt = null;
                device.lastEvent = 'STATION_OFFLINE';
                device.lastUpdated = now;
                persistBleDeviceState(device);
                broadcastBleDeviceState(device);
            }
            continue;
        }

        if (device.status === 'ONLINE' && device.lastBleSeen
            && nowMs - device.lastBleSeen.getTime() >= BLE_DEVICE_SEEN_STALE_MS) {
            device.status = 'GRACE';
            device.disconnectedAt = now;
            device.lastEvent = 'SEEN_TIMEOUT';
            device.lastUpdated = now;
            persistBleDeviceState(device);
            broadcastBleDeviceState(device, 'BLE_DEVICE_LOST');
        }

        if (device.status === 'GRACE' && device.disconnectedAt
            && nowMs - device.disconnectedAt.getTime() >= BLE_ALERT_TIMEOUT_MS) {
            device.status = 'ALERT';
            device.alertStartedAt = now;
            device.lastEvent = 'BLE_ALERT_TIMEOUT';
            device.lastUpdated = now;
            persistBleDeviceState(device);
            broadcastBleDeviceState(device, 'BLE_DEVICE_ALERT');
        }
    }
}

setInterval(evaluateBleStateMachine, 5000);

// =========================================================================
// 5. WEBSOCKET SERVER
// =========================================================================
const server = app.listen(PORT, () =>
    console.log(` MedPulse Backend Service đang chạy trên Port: ${PORT}`)
);
const wss = new WebSocket.Server({ server });

function broadcastToDashboards(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

wss.on('connection', (ws) => {
    console.log('🔌 Dashboard Client đã kết nối WebSocket.');
    // Gửi toàn bộ state hiện tại (đã có profile) cho client mới
    ws.send(JSON.stringify({
        type: 'INITIAL_SYNC',
        data: getRegisteredPatientStates().map(serializePatientState),
    }));
    ws.send(JSON.stringify({
        type: 'PENDING_REGISTRATIONS',
        data: Array.from(pendingRegistrations.values()).map(serializePendingRegistration),
    }));
    ws.send(JSON.stringify({
        type: 'BLE_DEVICES_SYNC',
        data: Array.from(bleDeviceStates.values()).map(serializeBleDeviceState),
    }));
    ws.send(JSON.stringify({
        type: 'STATIONS_SYNC',
        data: Array.from(stationStates.values()).map(serializeStationState),
    }));
    ws.on('close', () => console.log(' Một Dashboard Client đã đóng kết nối.'));
});

// =========================================================================
// 5. MQTT CLIENT
// =========================================================================
const mqttOptions = {
    clientId:       `medpulse_backend_${Math.random().toString(16).substr(2, 8)}`,
    clean:          true,
    connectTimeout: 4000,
    reconnectPeriod: 1000,
};

if (process.env.MQTT_USER)     mqttOptions.username = process.env.MQTT_USER;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;

const brokerUrl = process.env.MQTT_BROKER_URL || 'ws://broker.hivemq.com:8000/mqtt';
if (brokerUrl.startsWith('ws://') || brokerUrl.startsWith('wss://')) {
    mqttOptions.path = '/mqtt';
}

const mqttClient = mqtt.connect(brokerUrl, mqttOptions);

mqttClient.on('connect', () => {
    console.log(' Đã kết nối HiveMQ Broker!');
    mqttClient.subscribe([
        `${MQTT_ROOT_TOPIC}/+/vitals`,
        `${MQTT_ROOT_TOPIC}/stations/+/ble`,
        `${MQTT_ROOT_TOPIC}/stations/+/heartbeat`,
        `${MQTT_ROOT_TOPIC}/stations/+/devices/+/vitals`,
    ]);
    console.log(' Đang lắng nghe vitals + BLE presence + station heartbeat');
});

mqttClient.on('error', (err) =>
    console.error(' Lỗi kết nối MQTT:', err.message)
);

mqttClient.on('message', (topic, message, packet) => {
    try {
        const parts = topic.split('/');
        if (packet?.retain) {
            console.log(` Bỏ qua retained MQTT trên topic: ${topic}`);
            return;
        }

        const liveData = JSON.parse(message.toString().trim());

        if (parts.length === 6 && parts[0] === MQTT_ROOT_TOPIC && parts[1] === 'stations'
            && parts[3] === 'devices' && parts[5] === 'vitals') {
            const stationId = parts[2];
            const deviceId = parts[4];
            const validationError = validateGatewayVitalsPayload(liveData, stationId, deviceId);
            if (validationError) {
                console.warn(` [MQTT gateway vitals reject] ${topic}: ${validationError}`);
                return;
            }
            handleGatewayVitalsPayload(liveData, stationId, deviceId);
            return;
        }

        if (parts.length === 4 && parts[0] === MQTT_ROOT_TOPIC && parts[1] === 'stations') {
            const stationId = parts[2];
            const channel = parts[3];
            const validationError = channel === 'ble'
                ? validateBlePresencePayload(liveData, stationId)
                : channel === 'heartbeat'
                    ? validateStationHeartbeatPayload(liveData, stationId)
                    : 'channel không được hỗ trợ';
            if (validationError) {
                console.warn(` [MQTT protocol reject] ${topic}: ${validationError}`);
                return;
            }
            if (channel === 'ble') handleBlePresencePayload(liveData, stationId);
            else handleStationHeartbeatPayload(liveData, stationId);
            return;
        }

        if (parts.length !== 3 || parts[0] !== MQTT_ROOT_TOPIC || parts[2] !== 'vitals') return;

        const patientId = parts[1];
        applyIncomingVitals(patientId, liveData, { type: 'DIRECT_MQTT' });

    } catch (error) {
        console.error(' Lỗi xử lý MQTT JSON:', error.message);
    }
});

// =========================================================================
// 6. LƯU VITALS VÀO DB
// =========================================================================
function saveVitalsToDatabase(p) {
    // [FIX] Kiểm tra cột tồn tại bằng INSERT an toàn — dùng ON DUPLICATE KEY để tránh crash
    const query = `
        INSERT INTO vitals_log
        (patient_id, heart_rate, spo2, temp, battery, rssi,
         fall_status, is_safe, device_status, current_status,
         status_level, status_history, risk_score,
         measurement_source, source_device_id, source_station_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const values = [
        p.id,
        p.heartRate,
        p.spo2,
        p.temp,
        p.battery,
        p.rssi    || -55,
        p.fall    ? 1 : 0,
        p.safe    ? 1 : 0,
        p.status  || 'IDLE',
        p.current_status || p.status || 'IDLE',
        p.alertLevel     || 'safe',
        JSON.stringify(p.statusHistory || []),
        p.riskScore      || 4,
        p.measurementSource || 'DIRECT_MQTT',
        p.sourceDeviceId || null,
        p.sourceStationId || null,
    ];

    
        db.query(query, values, (err, results) => {
    if (err) {
        // Nếu phát hiện mất kết nối
        if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET') {
            console.log(` Mất kết nối DB khi lưu dữ liệu cho ${values[0]}. Đang xin một kết nối mới để thử lại...`);
            
            // Ép Pool phải tạo hoặc cấp một kết nối mới hoàn toàn sạch sẽ
            db.getConnection((connErr, connection) => {
                if (connErr) {
                    console.error(` Không thể tạo kết nối mới cho ${values[0]}:`, connErr.message);
                    return;
                }
                
                // Dùng kết nối mới này để thực thi lại câu lệnh SQL
                connection.query(query, values, (retryErr, retryResults) => {
                    // Sau khi dùng xong phải giải phóng kết nối trả lại cho Pool
                    connection.release(); 
                    
                    if (retryErr) {
                        console.error(` Thử lại bằng kết nối mới vẫn thất bại cho ${values[0]}:`, retryErr.message);
                    } else {
                        console.log(` Thử lại THÀNH CÔNG bằng kết nối mới! Đã lưu dữ liệu cho: ${values[0]}`);
                    }
                });
            });
            return;
        }
        
        console.error(` Lỗi lưu DB bệnh nhân ${values[0]}:`, err.message);
        console.error('   SQL values:', JSON.stringify(values));
        return;
    }
    console.log(` Đã cập nhật + lưu DB thành công cho: ${values[0]}`);
});
}

// =========================================================================
// 7. HTTP API
// =========================================================================

function calculateAgeFromDateOfBirth(dateOfBirth) {
    if (!dateOfBirth) return null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateOfBirth)) return null;
    const birthDate = new Date(`${dateOfBirth}T00:00:00Z`);
    if (Number.isNaN(birthDate.getTime()) || birthDate.toISOString().slice(0, 10) !== dateOfBirth || birthDate > new Date()) return null;
    const today = new Date();
    let age = today.getUTCFullYear() - birthDate.getUTCFullYear();
    const monthDelta = today.getUTCMonth() - birthDate.getUTCMonth();
    if (monthDelta < 0 || (monthDelta === 0 && today.getUTCDate() < birthDate.getUTCDate())) age--;
    return age >= 0 && age <= 130 ? age : null;
}

function insertPatientProfile(profile, callback) {
    const withDateOfBirth = `
        INSERT INTO patients
        (id, name, age, date_of_birth, gender, room, bed, condition_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const values = [
        profile.id, profile.name, profile.age, profile.dateOfBirth,
        profile.gender, profile.room, profile.bed, profile.conditionSummary,
    ];

    db.query(withDateOfBirth, values, (err, result) => {
        if (!err || err.code !== 'ER_BAD_FIELD_ERROR') return callback(err, result, true);

        // Railway chưa chạy migration date_of_birth: vẫn đăng ký được và lưu tuổi đã tính.
        const legacyQuery = `
            INSERT INTO patients
            (id, name, age, gender, room, bed, condition_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `;
        const legacyValues = [
            profile.id, profile.name, profile.age,
            profile.gender, profile.room, profile.bed, profile.conditionSummary,
        ];
        db.query(legacyQuery, legacyValues, (legacyErr, legacyResult) =>
            callback(legacyErr, legacyResult, false)
        );
    });
}

app.get('/api/pending-registrations', (req, res) => {
    res.json(Array.from(pendingRegistrations.values()).map(serializePendingRegistration));
});

app.get('/api/ble/devices', (req, res) => {
    res.json(Array.from(bleDeviceStates.values()).map(serializeBleDeviceState));
});

app.get('/api/stations', (req, res) => {
    res.json(Array.from(stationStates.values()).map(serializeStationState));
});

app.get('/api/patients/:id/device', (req, res) => {
    const patientId = String(req.params.id || '').trim();
    if (!registeredPatientIds.has(patientId)) return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    const device = Array.from(bleDeviceStates.values()).find(item => item.patientId === patientId);
    res.json(device ? serializeBleDeviceState(device) : null);
});

app.post('/api/patients/:id/device', (req, res) => {
    const patientId = String(req.params.id || '').trim();
    const deviceId = String(req.body?.deviceId || '').trim();
    if (!registeredPatientIds.has(patientId)) return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    if (!isValidProtocolIdentifier(deviceId, 24)) return res.status(400).json({ error: 'deviceId không hợp lệ' });

    const target = bleDeviceStates.get(deviceId);
    if (!target) return res.status(404).json({ error: 'Thiết bị chưa từng được trạm phát hiện' });

    for (const existing of bleDeviceStates.values()) {
        if (existing.deviceId === deviceId || existing.patientId !== patientId) continue;
        existing.patientId = null;
        existing.sessionActive = false;
        existing.sessionBlocked = false;
        existing.status = 'OFFLINE';
        existing.disconnectedAt = null;
        existing.alertStartedAt = null;
        existing.lastEvent = 'UNLINKED';
        existing.lastUpdated = new Date();
        persistBleDeviceState(existing);
        broadcastBleDeviceState(existing);
    }

    target.patientId = patientId;
    target.sessionBlocked = false;
    target.lastEvent = 'LINKED_TO_PATIENT';
    target.lastUpdated = new Date();
    persistBleDeviceState(target);
    broadcastBleDeviceState(target);
    res.json({ message: 'Đã liên kết thiết bị với bệnh nhân', device: serializeBleDeviceState(target) });
});

app.post('/api/ble/devices/:deviceId/end-session', (req, res) => {
    const deviceId = String(req.params.deviceId || '').trim();
    const device = bleDeviceStates.get(deviceId);
    if (!device) return res.status(404).json({ error: 'Không tìm thấy thiết bị BLE' });

    device.sessionActive = false;
    device.sessionBlocked = true;
    device.status = 'OFFLINE';
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'END_SESSION';
    device.lastUpdated = new Date();
    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_SESSION_ENDED');
    res.json({ message: 'Đã kết thúc phiên theo dõi', device: serializeBleDeviceState(device) });
});

app.post('/api/ble/devices/:deviceId/start-session', (req, res) => {
    const deviceId = String(req.params.deviceId || '').trim();
    const device = bleDeviceStates.get(deviceId);
    if (!device) return res.status(404).json({ error: 'Không tìm thấy thiết bị BLE' });
    if (!device.patientId) return res.status(409).json({ error: 'Thiết bị chưa liên kết với bệnh nhân' });

    const now = new Date();
    device.sessionBlocked = false;
    device.sessionActive = isBleDeviceSeenToday(device, now);
    const station = device.stationId ? stationStates.get(device.stationId) : null;
    const recentlySeen = device.lastBleSeen
        && now.getTime() - device.lastBleSeen.getTime() < BLE_DEVICE_SEEN_STALE_MS;
    device.status = device.sessionActive && station?.status === 'ONLINE' && recentlySeen ? 'ONLINE' : 'OFFLINE';
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'START_SESSION';
    device.lastUpdated = now;
    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_SESSION_STARTED');
    res.json({ message: 'Đã bắt đầu phiên theo dõi', device: serializeBleDeviceState(device) });
});

app.delete('/api/patients/:id/device', (req, res) => {
    const patientId = String(req.params.id || '').trim();
    const device = Array.from(bleDeviceStates.values()).find(item => item.patientId === patientId);
    if (!device) return res.status(404).json({ error: 'Bệnh nhân chưa liên kết thiết bị' });

    device.patientId = null;
    device.sessionActive = false;
    device.sessionBlocked = false;
    device.status = 'OFFLINE';
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'UNLINKED';
    device.lastUpdated = new Date();
    persistBleDeviceState(device);
    broadcastBleDeviceState(device);
    res.json({ message: 'Đã gỡ liên kết thiết bị', device: serializeBleDeviceState(device) });
});

app.post('/api/patients/:id/register', (req, res) => {
    const id = String(req.params.id || '').trim();
    const pending = pendingRegistrations.get(id);
    if (!pending) return res.status(404).json({ error: 'RFID này không còn trong hàng chờ đăng ký' });
    if (registeredPatientIds.has(id)) return res.status(409).json({ error: 'Bệnh nhân đã được đăng ký' });

    const name = String(req.body?.name || '').trim();
    const dateOfBirth = String(req.body?.dateOfBirth || '').trim() || null;
	const rawAge = req.body?.age;
	const hasSuppliedAge = rawAge !== null && rawAge !== undefined && String(rawAge).trim() !== '';
	const suppliedAge = hasSuppliedAge ? Number(rawAge) : Number.NaN;
    const calculatedAge = calculateAgeFromDateOfBirth(dateOfBirth);
    const age = calculatedAge ?? (Number.isInteger(suppliedAge) && suppliedAge >= 0 && suppliedAge <= 130 ? suppliedAge : null);
    const gender = String(req.body?.gender || '').trim().slice(0, 20) || null;
    const room = String(req.body?.room || '').trim().slice(0, 20) || null;
    const bed = String(req.body?.bed || '').trim().slice(0, 20) || null;
    const conditionSummary = String(req.body?.conditionSummary || '').trim().slice(0, 1000) || null;

    if (name.length < 2 || name.length > 100) {
        return res.status(400).json({ error: 'Tên bệnh nhân phải có từ 2 đến 100 ký tự' });
    }
    if (dateOfBirth && calculatedAge === null) {
        return res.status(400).json({ error: 'Ngày sinh không hợp lệ' });
    }
    if (age === null) {
        return res.status(400).json({ error: 'Cần nhập ngày sinh hoặc tuổi hợp lệ' });
    }

    const profile = { id, name, age, dateOfBirth, gender, room, bed, conditionSummary };
    insertPatientProfile(profile, (err, result, storedDateOfBirth) => {
        if (err) {
            if (err.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'RFID đã tồn tại trong patients' });
            console.error(` Lỗi đăng ký bệnh nhân ${id}:`, err.message);
            return res.status(500).json({ error: 'Không thể lưu hồ sơ bệnh nhân', details: err.message });
        }

        const patient = pending.patient;
        patient.name = name;
        patient.age = age;
        patient.date_of_birth = storedDateOfBirth ? dateOfBirth : null;
        patient.gender = gender;
        patient.room = room;
        patient.bed = bed;
        patient.condition_summary = conditionSummary;
        registeredPatientIds.add(id);
        pendingRegistrations.delete(id);

        const updated = processPatientMetricsCalculation(id);
        if (updated.hasRealData) saveVitalsToDatabase(updated);
        broadcastToDashboards({ type: 'PATIENT_UPDATE', data: serializePatientState(updated) });
        broadcastToDashboards({
            type: 'PENDING_REGISTRATIONS',
            data: Array.from(pendingRegistrations.values()).map(serializePendingRegistration),
        });

        res.status(201).json({
            patient: serializePatientState(updated),
            storedDateOfBirth,
            message: storedDateOfBirth
                ? 'Đăng ký bệnh nhân thành công'
                : 'Đăng ký thành công; cần chạy migration date_of_birth để lưu ngày sinh',
        });
    });
});

// [FIX] API /api/patients — join thêm profile từ DB để chắc chắn có name/age/gender
app.get('/api/patients', (req, res) => {
    db.query('SELECT * FROM patients', (err, rows) => {
        const profileMap = {};
        if (!err && rows) {
            rows.forEach(r => { profileMap[r.id] = r; });
        }

        queryLatestVitals((latestErr, latestRows) => {
            if (!latestErr && latestRows) {
                Object.values(patientsState).forEach(resetPatientMeasurementState);
                latestRows.forEach(row => applyStoredVitalsAndRecalculate(ensurePatientState(row.id), row));
            }

            const result = Object.values(patientsState)
                .filter(p => Boolean(profileMap[p.id]))
                .map(p => {
                const profile = profileMap[p.id] || {};
                return {
                    ...serializePatientState(p),
                    name:              profile.name              || p.name    || p.id,
                    age:               profile.age               ?? p.age    ?? null,
                    gender:            profile.gender            || p.gender  || null,
                    room:              profile.room              || p.room    || null,
                    bed:               profile.bed               || p.bed     || null,
                    condition_summary: profile.condition_summary || p.condition_summary || null,
                };
            });

            res.json(result);
        });
    });
});

// [FIX] API /api/patients/:id — join profile từ DB
app.get('/api/patients/:id', (req, res) => {
    const id      = req.params.id;
    const patient = patientsState[id];
    if (!patient || !registeredPatientIds.has(id)) return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });

    db.query('SELECT * FROM patients WHERE id = ?', [id], (err, rows) => {
        const profile = (!err && rows && rows[0]) ? rows[0] : {};
        const latestQuery = `
            SELECT patient_id AS id,
                   heart_rate AS heartRate,
                   spo2,
                   temp,
                   battery,
                   rssi,
                   fall_status AS fall,
                   is_safe AS safe,
                   device_status AS status,
                   current_status,
                   status_level AS alertLevel,
                   risk_score AS riskScore,
                   measurement_source AS measurementSource,
                   source_device_id AS sourceDeviceId,
                   source_station_id AS sourceStationId,
                   recorded_at AS time
            FROM vitals_log
            WHERE patient_id = ?
            ORDER BY recorded_at DESC
            LIMIT 1
        `;
        db.query(latestQuery, [id], (latestErr, latestRows) => {
            if (!latestErr && latestRows && latestRows[0]) {
                applyStoredVitalsAndRecalculate(patient, latestRows[0]);
            }
            res.json({
                ...serializePatientState(patient),
                name:              profile.name              || patient.name    || id,
                age:               profile.age               ?? patient.age    ?? null,
                gender:            profile.gender            || patient.gender  || null,
                room:              profile.room              || patient.room    || null,
                bed:               profile.bed               || patient.bed     || null,
                condition_summary: profile.condition_summary || patient.condition_summary || null,
            });
        });
    });
});

// API lịch sử vitals
app.get('/api/patients/:id/history', (req, res) => {
    const patientId = req.params.id;
    if (!registeredPatientIds.has(patientId)) {
        return res.status(404).json({ error: 'Bệnh nhân chưa được đăng ký' });
    }
    const requestedLimit = Number.parseInt(req.query.limit, 10);
    const limit = Math.min(Math.max(Number.isInteger(requestedLimit) ? requestedLimit : 500, 1), 1000);
    const rangeHours = { '24h': 24, '7d': 24 * 7, '30d': 24 * 30 }[req.query.range] || 24 * 7;

    const query = `
        SELECT heart_rate AS heartRate, spo2, temp,
               device_status AS status, current_status,
               status_level AS alertLevel, status_history AS statusHistory,
               risk_score AS riskScore,
               measurement_source AS measurementSource,
               source_device_id AS sourceDeviceId,
               source_station_id AS sourceStationId,
               recorded_at AS time
        FROM vitals_log
        WHERE patient_id = ?
          AND recorded_at >= DATE_SUB(NOW(), INTERVAL ${rangeHours} HOUR)
        ORDER BY recorded_at DESC
        LIMIT ?
    `;

    db.query(query, [patientId, limit], (err, results) => {
        if (err) return res.status(500).json({ error: 'Lỗi truy vấn DB', details: err.message });
        res.json(results.reverse());
    });
});

// Serve web.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'web.html'));
});
