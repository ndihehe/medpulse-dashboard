const express = require('express');
const mqtt = require('mqtt');
const WebSocket = require('ws');
const mysql = require('mysql2');
const cors = require('cors');
const path = require('path');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const dns = require('dns');
const net = require('net');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
app.use(cors());
app.use(express.json());

const NODE_ENV = process.env.NODE_ENV || 'development';
const IS_PRODUCTION = NODE_ENV === 'production';
const DEFAULT_MQTT_ROOT_TOPIC = 'medpulse_duy';
const DEFAULT_MQTT_BROKER_URL = 'ws://broker.hivemq.com:8000/mqtt';
const PORT = process.env.PORT || 5000;
const APP_TIME_ZONE = process.env.APP_TIME_ZONE || 'Asia/Ho_Chi_Minh';
const MQTT_ROOT_TOPIC = process.env.MQTT_ROOT_TOPIC || DEFAULT_MQTT_ROOT_TOPIC;
const ROLE_LEVELS = Object.freeze({ STAFF_NURSE: 1, HEAD_NURSE: 2, MANAGER: 3, ADMIN: 3 });
const DELETE_OTP_TTL_MS = 5 * 60 * 1000;
const DELETE_OTP_RESEND_MS = 60 * 1000;
const deleteOtpChallenges = new Map();
const deleteOtpRequestWindows = new Map();
const DELETE_OTP_WINDOW_MS = 10 * 60 * 1000;
const DELETE_OTP_MAX_REQUESTS_PER_WINDOW = 5;
const AUTH_SESSION_TTL_MS = Number(process.env.AUTH_SESSION_TTL_MS || 8 * 60 * 60 * 1000);
const DEMO_LOGIN_ENABLED = String(process.env.ALLOW_DEMO_LOGIN ?? (NODE_ENV === 'production' ? 'false' : 'true')).toLowerCase() === 'true';
const ENFORCE_PRODUCTION_CONFIG = String(process.env.ENFORCE_PRODUCTION_CONFIG || 'false').toLowerCase() === 'true';
const DEMO_LOGIN_USER = 'nurse.le';
const DEMO_LOGIN_PASSWORD = 'demo123';
const authSessions = new Map();
const BLE_PROTOCOL_VERSION = 1;
const COMMAND_PROTOCOL_VERSION = 2;
const DIRECT_DEVICE_TIMEOUT_MS = Number(process.env.DIRECT_DEVICE_TIMEOUT_MS || 90000);
const BLE_DEVICE_SEEN_STALE_MS = Number(process.env.BLE_DEVICE_SEEN_STALE_MS || 45000);
const BLE_GATT_READY_STALE_MS = Number(process.env.BLE_GATT_READY_STALE_MS || 45000);
const DEVICE_COMMAND_ACK_TIMEOUT_MS = Number(process.env.DEVICE_COMMAND_ACK_TIMEOUT_MS || 20000);
const DEVICE_COMMAND_RETRY_INTERVAL_MS = Number(process.env.DEVICE_COMMAND_RETRY_INTERVAL_MS || 5000);
const BLE_ALERT_TIMEOUT_MS = Number(process.env.BLE_ALERT_TIMEOUT_MS || 180000);
const STATION_OFFLINE_TIMEOUT_MS = Number(process.env.STATION_OFFLINE_TIMEOUT_MS || 90000);
const BLE_DEVICE_STATUSES = new Set(['OFFLINE', 'ONLINE', 'GRACE', 'ALERT', 'STATION_OFFLINE']);
const GATT_EVENT_TYPES = new Set([
    'HEARTBEAT', 'PREALERT', 'FALL_ALERT', 'RECOVERED', 'HARDWARE_FAULT', 'HARDWARE_RECOVERED',
]);
const FIRMWARE_STATUSES = new Set(['IDLE', 'FALLING', 'IMPACT', 'MOTIONLESS', 'ALERT']);
const FIRMWARE_STATUS_META = {
    IDLE:       { alertLevel: 'safe',    riskWeight: 0,  fall: false, safe: true  },
    FALLING:    { alertLevel: 'warning', riskWeight: 18, fall: false, safe: false },
    IMPACT:     { alertLevel: 'warning', riskWeight: 28, fall: false, safe: false },
    MOTIONLESS: { alertLevel: 'warning', riskWeight: 36, fall: false, safe: false },
    ALERT:      { alertLevel: 'danger',  riskWeight: 46, fall: true,  safe: false },
};

const smtpConfigured = Boolean(process.env.SMTP_USER && process.env.SMTP_APP_PASSWORD);
const resendConfigured = Boolean(process.env.RESEND_API_KEY);
const brokerUrl = process.env.MQTT_BROKER_URL || DEFAULT_MQTT_BROKER_URL;

function parseBooleanEnv(value, fallback) {
    if (value === undefined || value === null || value === '') return fallback;
    return String(value).toLowerCase() === 'true';
}

function validateProductionConfiguration() {
    if (!IS_PRODUCTION) return;
    const unsafe = [];
    if (!process.env.MQTT_BROKER_URL || brokerUrl === DEFAULT_MQTT_BROKER_URL) {
        unsafe.push('MQTT_BROKER_URL phải trỏ tới broker riêng, không dùng public HiveMQ mặc định');
    }
    if (!process.env.MQTT_ROOT_TOPIC || MQTT_ROOT_TOPIC === DEFAULT_MQTT_ROOT_TOPIC) {
        unsafe.push('MQTT_ROOT_TOPIC phải đổi khỏi medpulse_duy khi chạy production');
    }
    if (DEMO_LOGIN_ENABLED) unsafe.push('ALLOW_DEMO_LOGIN phải false khi chạy production');
    if (unsafe.length) {
        const message = ` Cấu hình production không an toàn:\n- ${unsafe.join('\n- ')}`;
        if (ENFORCE_PRODUCTION_CONFIG) {
            console.error(message);
            process.exit(1);
        }
        console.warn(`${message}\nĐang tiếp tục start vì ENFORCE_PRODUCTION_CONFIG=false.`);
    }
}

validateProductionConfiguration();

async function createSmtpTransport() {
    if (!smtpConfigured) return null;
    const smtpHostname = process.env.SMTP_HOST || 'smtp.gmail.com';
    const ipv4Addresses = net.isIPv4(smtpHostname) ? [smtpHostname] : await dns.promises.resolve4(smtpHostname);
    if (!ipv4Addresses.length) throw new Error(`Không phân giải được IPv4 cho ${smtpHostname}`);
    const smtpIpv4 = ipv4Addresses[crypto.randomInt(0, ipv4Addresses.length)];
    return nodemailer.createTransport({
        host: smtpIpv4,
        port: Number(process.env.SMTP_PORT || 465),
        secure: String(process.env.SMTP_SECURE || 'true').toLowerCase() !== 'false',
        auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_APP_PASSWORD },
        connectionTimeout: Number(process.env.SMTP_CONNECTION_TIMEOUT_MS || 10000),
        greetingTimeout: Number(process.env.SMTP_GREETING_TIMEOUT_MS || 10000),
        socketTimeout: Number(process.env.SMTP_SOCKET_TIMEOUT_MS || 15000),
        tls: { servername: smtpHostname },
    });
}

async function sendOtpEmail({ to, patientId, otp }) {
    const subject = `Mã xác nhận xóa bệnh nhân ${patientId}`;
    const text = `Mã xác nhận: ${otp}\nMã có hiệu lực trong 5 phút. Không chia sẻ mã này.`;
    const html = `<p>Mã xác nhận xóa bệnh nhân <strong>${patientId}</strong>:</p><p style="font-size:28px;font-weight:700;letter-spacing:6px">${otp}</p><p>Mã có hiệu lực trong 5 phút. Không chia sẻ mã này.</p>`;

    if (resendConfigured) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        try {
            const response = await fetch('https://api.resend.com/emails', {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    from: process.env.RESEND_FROM || 'MedPulse <onboarding@resend.dev>',
                    to: [to],
                    subject,
                    text,
                    html,
                }),
                signal: controller.signal,
            });
            const result = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(`Resend ${response.status}: ${result.message || 'Không thể gửi email'}`);
            return { provider: 'RESEND', messageId: result.id || 'unknown' };
        } finally {
            clearTimeout(timeout);
        }
    }

    const smtpTransport = await createSmtpTransport();
    if (!smtpTransport) throw new Error('Chưa cấu hình dịch vụ gửi email');
    const result = await smtpTransport.sendMail({
        from: process.env.SMTP_FROM || process.env.SMTP_USER,
        to,
        subject,
        text,
        html,
    });
    return { provider: 'SMTP', messageId: result.messageId || 'unknown' };
}

function normalizeAccountEmail(value) {
    const email = String(value || '').trim().toLowerCase();
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function hashDeleteOtp(patientId, email, otp) {
    return crypto.createHmac('sha256', String(process.env.OTP_SECRET || ''))
        .update(`${patientId}:${email}:${otp}`)
        .digest('hex');
}

// =========================================================================
// 1. KẾT NỐI MYSQL POOL
// =========================================================================
const dbConnectionConfig = {
    host:             process.env.DB_HOST,
    user:             process.env.DB_USER,
    password:         process.env.DB_PASSWORD,
    database:         process.env.DB_DATABASE || process.env.DB_NAME,
    port:             process.env.DB_PORT,
    connectTimeout:   10000,
    ssl: parseBooleanEnv(process.env.DB_SSL_ENABLED, true)
        ? { rejectUnauthorized: parseBooleanEnv(process.env.DB_SSL_REJECT_UNAUTHORIZED, IS_PRODUCTION) }
        : undefined,
};

const db = mysql.createPool({
    ...dbConnectionConfig,
    waitForConnections: true,
    connectionLimit:  10,
    maxIdle:          2,
    idleTimeout:      60000,
    queueLimit:       0,
    enableKeepAlive:  true,
    keepAliveInitialDelay: 10000,
});

const REGISTRATION_DB_TIMEOUT_MS = Number(process.env.REGISTRATION_DB_TIMEOUT_MS || 8000);

db.getConnection((err, connection) => {
    if (err) {
        console.error(' Lỗi kết nối cơ sở dữ liệu MySQL:', err.message);
    } else {
        console.log('🔹 Cơ sở dữ liệu MySQL đã kết nối thành công!');
        connection.release();
        // Bảo đảm DB cũ có các cột truy vết nguồn đo trước khi load dữ liệu.
        ensureVitalsSourceColumns(loadPatientProfiles);
        initializeBlePersistence();
        initializeUserPersistence();
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
const directDeviceStates = new Map();
const deviceCommands = new Map();
const lastAutoBuzzerAt = new Map();
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
    patient.ble = patientConnectionText(patient);
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

function patientConnectionText(patient) {
    if (patient.signalLost) return 'Không có gói tin mới';
    if (patient.battery < 20) return 'Pin thiết bị yếu';
    if (patient.measurementSource === 'DIRECT_MQTT') return 'Đang nhận MQTT trực tiếp';
    if (patient.measurementSource === 'BLE_GATEWAY') return 'Đang nhận BLE qua trạm';
    return 'Đang nhận dữ liệu';
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

    patient.ble = patientConnectionText(patient);

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
    if (data.alertLevel === 'danger') queueAutomaticBuzzerCommand(data);
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
        lastGattEventSequence: null,
        lastGattEventUptimeSeconds: null,
        lastGattEventAt: null,
        gattReady: false,
        lastGattStatusAt: null,
        lastMeasurementAt: null,
        sensorReady: null,
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
        freeHeap: null,
        trackedBleDevices: 0,
        gattReadyConnections: 0,
        malformedGattFrames: 0,
        droppedGattFrames: 0,
        max30102Ready: null,
        mlx90614Ready: null,
        rc522Ready: null,
        lastUpdated: new Date(),
    };
}

function serializeBleDeviceState(device) {
    const patient = device.patientId ? patientsState[device.patientId] : null;
    const transportState = resolveDeviceTransportState(device.deviceId);
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
        lastGattEventSequence: Number.isInteger(device.lastGattEventSequence) ? device.lastGattEventSequence : null,
        lastGattEventUptimeSeconds: Number.isInteger(device.lastGattEventUptimeSeconds)
            ? device.lastGattEventUptimeSeconds : null,
        lastGattEventAt: toIsoOrNull(device.lastGattEventAt),
        gattReady: device.gattReady === true,
        lastGattStatusAt: toIsoOrNull(device.lastGattStatusAt),
        lastMeasurementAt: toIsoOrNull(device.lastMeasurementAt),
        sensorReady: typeof device.sensorReady === 'boolean' ? device.sensorReady : null,
        lastEvent: device.lastEvent,
        connectionAlert: device.lastEvent === 'BLE_ALERT_TIMEOUT',
        medicalFallAlert: isActiveFallDeviceEventName(device.lastEvent),
        lastUpdated: toIsoOrNull(device.lastUpdated),
        activeTransport: transportState.activeTransport,
        transportOnline: transportState.online,
        bleGatewayReady: transportState.bleGatewayOnline,
        gattStatusAgeMs: transportState.gattStatusAgeMs,
        stationStatus: transportState.stationStatus,
    };
}

function hasCompleteVitalsPayload(liveData) {
    return Number.isFinite(liveData?.heartRate)
        && Number.isFinite(liveData?.spo2)
        && Number.isFinite(liveData?.temp);
}

function serializeStationState(station) {
    return {
        stationId: station.stationId,
        status: station.status,
        lastSeenAt: toIsoOrNull(station.lastSeenAt),
        wifiRssi: Number.isFinite(station.wifiRssi) ? station.wifiRssi : null,
        bleScanner: station.bleScanner === true,
        uptimeMs: Number.isFinite(station.uptimeMs) ? station.uptimeMs : null,
        freeHeap: Number.isFinite(station.freeHeap) ? station.freeHeap : null,
        trackedBleDevices: station.trackedBleDevices,
        gattReadyConnections: station.gattReadyConnections,
        malformedGattFrames: station.malformedGattFrames,
        droppedGattFrames: station.droppedGattFrames,
        max30102Ready: station.max30102Ready,
        mlx90614Ready: station.mlx90614Ready,
        rc522Ready: station.rc522Ready,
        lastUpdated: toIsoOrNull(station.lastUpdated),
    };
}

function persistBleDeviceState(device) {
    const query = `
        INSERT INTO ble_devices
        (device_id, patient_id, station_id, ble_status, session_active, session_blocked,
         first_seen_today, last_ble_seen, disconnected_at, alert_started_at,
         rssi, battery, last_event, last_event_sequence, last_event_uptime_seconds, last_event_at,
         gatt_ready, last_gatt_status_at, sensor_ready)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_event = VALUES(last_event),
            last_event_sequence = VALUES(last_event_sequence),
            last_event_uptime_seconds = VALUES(last_event_uptime_seconds),
            last_event_at = VALUES(last_event_at),
            gatt_ready = VALUES(gatt_ready),
            last_gatt_status_at = VALUES(last_gatt_status_at),
            sensor_ready = VALUES(sensor_ready)
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
        device.lastGattEventSequence,
        device.lastGattEventUptimeSeconds,
        device.lastGattEventAt,
        device.gattReady ? 1 : 0,
        device.lastGattStatusAt,
        typeof device.sensorReady === 'boolean' ? (device.sensorReady ? 1 : 0) : null,
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
            device.lastGattEventSequence = row.last_event_sequence === null
                ? null : Number(row.last_event_sequence);
            device.lastGattEventUptimeSeconds = row.last_event_uptime_seconds === null
                ? null : Number(row.last_event_uptime_seconds);
            device.lastGattEventAt = row.last_event_at ? new Date(row.last_event_at) : null;
            device.gattReady = Boolean(row.gatt_ready);
            device.lastGattStatusAt = row.last_gatt_status_at ? new Date(row.last_gatt_status_at) : null;
            if (device.gattReady && (!device.lastGattStatusAt
                || now.getTime() - device.lastGattStatusAt.getTime() >= BLE_GATT_READY_STALE_MS)) {
                device.gattReady = false;
                device.lastEvent = device.lastEvent === 'GATT_READY' ? 'GATT_STATUS_TIMEOUT' : device.lastEvent;
            }
            device.sensorReady = row.sensor_ready === null || row.sensor_ready === undefined
                ? null : Boolean(row.sensor_ready);
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
            last_event_sequence TINYINT UNSIGNED NULL,
            last_event_uptime_seconds BIGINT UNSIGNED NULL,
            last_event_at DATETIME(3) NULL,
            gatt_ready TINYINT(1) NOT NULL DEFAULT 0,
            last_gatt_status_at DATETIME(3) NULL,
            sensor_ready TINYINT(1) NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_ble_patient (patient_id),
            INDEX idx_ble_station (station_id),
            INDEX idx_ble_status (ble_status)
        ) ENGINE=InnoDB
    `;
    const commandTable = `
        CREATE TABLE IF NOT EXISTS device_commands (
            command_id VARCHAR(64) PRIMARY KEY,
            device_id VARCHAR(24) NOT NULL,
            patient_id VARCHAR(50) NULL,
            station_id VARCHAR(50) NULL,
            action VARCHAR(30) NOT NULL,
            duration_ms INT NOT NULL DEFAULT 0,
            route VARCHAR(30) NOT NULL DEFAULT 'PENDING',
            command_status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
            error_message VARCHAR(255) NULL,
            created_at DATETIME(3) NOT NULL,
            expires_at DATETIME(3) NOT NULL,
            dispatched_at DATETIME(3) NULL,
            acknowledged_at DATETIME(3) NULL,
            INDEX idx_commands_device (device_id),
            INDEX idx_commands_patient (patient_id),
            INDEX idx_commands_status (command_status),
            INDEX idx_commands_expires (expires_at)
        ) ENGINE=InnoDB
    `;
    db.query(stationTable, err => {
        if (err) console.error(' Lỗi tạo bảng stations:', err.message);
        else loadStationStates();
    });
    db.query(deviceTable, err => {
        if (err) console.error(' Lỗi tạo bảng ble_devices:', err.message);
        else ensureBleDeviceEventColumns(loadBleDeviceStates);
    });
    db.query(commandTable, err => {
        if (err) console.error(' Lỗi tạo bảng device_commands:', err.message);
        else loadPendingDeviceCommands();
    });
}

function ensureBleDeviceEventColumns(callback = () => {}) {
    db.query(`
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'ble_devices'
    `, (error, rows = []) => {
        if (error) {
            console.error(' Lỗi kiểm tra schema sự kiện BLE:', error.message);
            callback();
            return;
        }
        const existing = new Set(rows.map(row => row.COLUMN_NAME));
        const additions = [];
        if (!existing.has('last_event_sequence')) additions.push('ADD COLUMN last_event_sequence TINYINT UNSIGNED NULL AFTER last_event');
        if (!existing.has('last_event_uptime_seconds')) additions.push('ADD COLUMN last_event_uptime_seconds BIGINT UNSIGNED NULL AFTER last_event_sequence');
        if (!existing.has('last_event_at')) additions.push('ADD COLUMN last_event_at DATETIME(3) NULL AFTER last_event_uptime_seconds');
        if (!existing.has('gatt_ready')) additions.push('ADD COLUMN gatt_ready TINYINT(1) NOT NULL DEFAULT 0 AFTER last_event_at');
        if (!existing.has('last_gatt_status_at')) additions.push('ADD COLUMN last_gatt_status_at DATETIME(3) NULL AFTER gatt_ready');
        if (!existing.has('sensor_ready')) additions.push('ADD COLUMN sensor_ready TINYINT(1) NULL AFTER last_gatt_status_at');
        if (!additions.length) {
            callback();
            return;
        }
        db.query(`ALTER TABLE ble_devices ${additions.join(', ')}`, alterError => {
            if (alterError) console.error(' Lỗi nâng cấp schema sự kiện BLE:', alterError.message);
            else console.log(' Đã bổ sung cột chống trùng sự kiện GATT cho ble_devices');
            callback();
        });
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

function autoLinkBleDeviceToMatchingPatient(device) {
    if (!device || device.patientId || !registeredPatientIds.has(device.deviceId)) return false;
    device.patientId = device.deviceId;
    device.sessionBlocked = false;
    device.lastEvent = device.lastEvent || 'AUTO_LINKED_TO_PATIENT';
    console.log(` Tu dong lien ket thiet bi ${device.deviceId} voi patient cung ID.`);
    return true;
}

function validateStationHeartbeatPayload(payload, topicStationId) {
    if (!payload || payload.protocolVersion !== BLE_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (payload.event !== 'STATION_HEARTBEAT' || payload.online !== true || payload.bleScanner !== true) return 'heartbeat không đúng giao thức';
    if (!Number.isFinite(payload.wifiRssi) || payload.wifiRssi < -127 || payload.wifiRssi > 20) return 'wifiRssi không hợp lệ';
    if (!Number.isFinite(payload.uptimeMs) || payload.uptimeMs < 0) return 'uptimeMs không hợp lệ';
    for (const field of ['freeHeap', 'trackedBleDevices', 'gattReadyConnections', 'malformedGattFrames', 'droppedGattFrames']) {
        if (payload[field] !== undefined && (!Number.isInteger(payload[field]) || payload[field] < 0)) return `${field} không hợp lệ`;
    }
    for (const field of ['max30102Ready', 'mlx90614Ready', 'rc522Ready']) {
        if (payload[field] !== undefined && typeof payload[field] !== 'boolean') return `${field} không hợp lệ`;
    }
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

function validateGatewayEventPayload(payload, topicStationId, topicDeviceId) {
    if (!payload || payload.protocolVersion !== COMMAND_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (!isValidProtocolIdentifier(topicDeviceId, 24) || payload.deviceId !== topicDeviceId) return 'deviceId không khớp topic';
    if (!Number.isInteger(payload.sequence) || payload.sequence < 0 || payload.sequence > 255) return 'sequence không hợp lệ';
    if (!GATT_EVENT_TYPES.has(payload.event)) return 'event không hợp lệ';
    if (!Number.isInteger(payload.battery) || payload.battery < -1 || payload.battery > 100) return 'battery không hợp lệ';
    if (typeof payload.fallDetected !== 'boolean' || typeof payload.charging !== 'boolean'
        || typeof payload.buzzerActive !== 'boolean') return 'flags không hợp lệ';
    if (payload.event === 'FALL_ALERT' && payload.fallDetected !== true) return 'FALL_ALERT thiếu fallDetected';
    if (payload.event === 'RECOVERED' && payload.fallDetected !== false) return 'RECOVERED còn fallDetected';
    if (!Number.isInteger(payload.deviceUptimeSeconds) || payload.deviceUptimeSeconds < 0
        || payload.deviceUptimeSeconds > 0xFFFFFFFF) return 'deviceUptimeSeconds không hợp lệ';
    if (!Number.isFinite(payload.rssi) || payload.rssi < -127 || payload.rssi > 20) return 'rssi không hợp lệ';
    if (!Number.isFinite(payload.stationUptimeMs) || payload.stationUptimeMs < 0) return 'stationUptimeMs không hợp lệ';
    return null;
}

function validateGatewayTransportPayload(payload, topicStationId, topicDeviceId) {
    if (!payload || payload.protocolVersion !== COMMAND_PROTOCOL_VERSION) return 'protocolVersion không được hỗ trợ';
    if (!isValidProtocolIdentifier(topicStationId, 50) || payload.stationId !== topicStationId) return 'stationId không khớp topic';
    if (!isValidProtocolIdentifier(topicDeviceId, 24) || payload.deviceId !== topicDeviceId) return 'deviceId không khớp topic';
    if (payload.transport !== 'BLE_GATEWAY') return 'transport không hợp lệ';
    if (typeof payload.online !== 'boolean' || typeof payload.gattReady !== 'boolean') return 'online/gattReady không hợp lệ';
    if (payload.gattReady && !payload.online) return 'gattReady không thể true khi online=false';
    if (!Number.isFinite(payload.rssi) || payload.rssi < -127 || payload.rssi > 20) return 'rssi không hợp lệ';
    if (!Number.isFinite(payload.stationUptimeMs) || payload.stationUptimeMs < 0) return 'stationUptimeMs không hợp lệ';
    return null;
}

function isFreshGatewayEvent(device, payload) {
    if (!Number.isInteger(device.lastGattEventSequence)) return true;
    if (Number.isInteger(device.lastGattEventUptimeSeconds)
        && payload.deviceUptimeSeconds < device.lastGattEventUptimeSeconds) return true;
    const delta = (payload.sequence - device.lastGattEventSequence + 256) % 256;
    return delta > 0 && delta < 128;
}

function firmwareStatusFromDeviceEvent(event) {
    if (event === 'PREALERT') return 'MOTIONLESS';
    if (event === 'FALL_ALERT') return 'ALERT';
    if (event === 'RECOVERED') return 'IDLE';
    return null;
}

function statusFromDeviceEvent(event, fallbackStatus = 'ONLINE') {
    if (event === 'FALL_ALERT') return 'ALERT';
    if (event === 'RECOVERED') return 'ONLINE';
    return fallbackStatus;
}

function isActiveFallDeviceEventName(eventName) {
    return ['GATT_FALL_ALERT', 'DIRECT_FALL_ALERT'].includes(eventName);
}

function logUnmappedFallEvent(source, device, event) {
    if (!['PREALERT', 'FALL_ALERT', 'RECOVERED'].includes(event)) return;
    console.warn(` Fall event ${source} chưa cập nhật patient: device=${device.deviceId}, event=${event}, patientId=${device.patientId || 'none'}, sessionActive=${device.sessionActive}, sessionBlocked=${device.sessionBlocked}`);
}

function markStationOnlineFromPayload(stationId, payload, now = new Date()) {
    const station = getOrCreateStationState(stationId);
    const wasOffline = station.status !== 'ONLINE';
    station.status = 'ONLINE';
    station.lastSeenAt = now;
    station.wifiRssi = Number.isFinite(payload.wifiRssi) ? Math.round(payload.wifiRssi) : station.wifiRssi;
    station.bleScanner = payload.bleScanner !== false;
    station.uptimeMs = Number.isFinite(payload.uptimeMs) ? Math.round(payload.uptimeMs) : station.uptimeMs;
    for (const field of ['freeHeap', 'trackedBleDevices', 'gattReadyConnections', 'malformedGattFrames', 'droppedGattFrames']) {
        if (Number.isInteger(payload[field]) && payload[field] >= 0) station[field] = payload[field];
    }
    for (const field of ['max30102Ready', 'mlx90614Ready', 'rc522Ready']) {
        if (typeof payload[field] === 'boolean') station[field] = payload[field];
    }
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
            broadcastDeviceTransportState(device.deviceId);
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
    autoLinkBleDeviceToMatchingPatient(device);

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
    if (payload.event === 'SEEN') dispatchPendingCommandsForDevice(payload.deviceId);
}

function applyIncomingVitals(patientId, liveData, source = {}) {
    ensurePatientState(patientId);
    lastSeenByPatient.set(patientId, Date.now());

    const patient = patientsState[patientId];
    const isCompleteMeasurement = hasCompleteVitalsPayload(liveData);
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
        // RFID lạ chỉ được đưa vào hàng chờ sau khi trạm gửi đủ một lần đo.
        // Không tạo hồ sơ từ heartbeat/trạng thái hoặc gói sinh hiệu dở dang.
        if (!isCompleteMeasurement) {
            console.warn(` Bỏ qua gói đo chưa đầy đủ của RFID chưa đăng ký ${patientId}`);
            return false;
        }
        patient.hasRealData = true;
        patient.lastUpdate = new Date();
        patient.lastMeasurementAt = patient.lastUpdate.toISOString();
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
            saveToDatabase: isCompleteMeasurement,
            markAsMeasurement: isCompleteMeasurement,
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
    autoLinkBleDeviceToMatchingPatient(device);

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

function handleGatewayEventPayload(payload, stationId, deviceId) {
    const now = new Date();
    markStationOnlineFromPayload(stationId, {
        uptimeMs: payload.stationUptimeMs,
        bleScanner: true,
    }, now);

    const device = getOrCreateBleDeviceState(deviceId);
    if (!isFreshGatewayEvent(device, payload)) return;
    autoLinkBleDeviceToMatchingPatient(device);

    device.stationId = stationId;
    device.rssi = Math.round(payload.rssi);
    if (payload.battery >= 0) device.battery = payload.battery;
    if (!isBleDeviceSeenToday(device, now)) device.firstSeenToday = now;
    device.lastBleSeen = now;
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastGattEventSequence = payload.sequence;
    device.lastGattEventUptimeSeconds = payload.deviceUptimeSeconds;
    device.lastGattEventAt = now;
    device.gattReady = true;
    device.lastGattStatusAt = now;
    if (payload.event === 'HARDWARE_FAULT') device.sensorReady = false;
    if (payload.event === 'HARDWARE_RECOVERED') device.sensorReady = true;
    const eventImpliesActiveFall = payload.event === 'FALL_ALERT'
        || (payload.event === 'HEARTBEAT' && payload.fallDetected === true);
    const shouldKeepFallEvent = isActiveFallDeviceEventName(device.lastEvent)
        && payload.event === 'HEARTBEAT'
        && !eventImpliesActiveFall;
    if (!shouldKeepFallEvent) {
        device.lastEvent = eventImpliesActiveFall ? 'GATT_FALL_ALERT' : `GATT_${payload.event}`;
    }
    device.lastUpdated = now;
    if (device.sessionBlocked) {
        device.sessionActive = false;
        device.status = 'OFFLINE';
    } else {
        device.sessionActive = true;
        device.status = eventImpliesActiveFall ? 'ALERT' : statusFromDeviceEvent(payload.event, device.status === 'ALERT' && shouldKeepFallEvent ? 'ALERT' : 'ONLINE');
    }

    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_GATT_EVENT');
    broadcastDeviceTransportState(deviceId);
    if (!device.sessionBlocked) dispatchPendingCommandsForDevice(deviceId);
    if (!device.patientId || !device.sessionActive || device.sessionBlocked) {
        logUnmappedFallEvent('BLE_GATEWAY', device, payload.event);
        return;
    }

    const patient = patientsState[device.patientId];
    if (!patient || !registeredPatientIds.has(device.patientId)) {
        logUnmappedFallEvent('BLE_GATEWAY', device, payload.event);
        return;
    }
    if (payload.battery >= 0) patient.battery = payload.battery;
    patient.rssi = Math.round(payload.rssi);
    patient.signalLost = false;
    patient.sourceDeviceId = deviceId;
    patient.sourceStationId = stationId;

    const firmwareStatus = eventImpliesActiveFall ? 'ALERT' : firmwareStatusFromDeviceEvent(payload.event);
    if (!firmwareStatus) return;
    syncFallSafeState(patient, 'status', firmwareStatus);
    console.log(` Cap nhat patient ${device.patientId} tu BLE event ${payload.event}: ${firmwareStatus}`);
    publishPatientUpdate(device.patientId, {
        saveToDatabase: false,
        markAsMeasurement: false,
    });
}

function handleGatewayTransportPayload(payload, stationId, deviceId) {
    const now = new Date();
    markStationOnlineFromPayload(stationId, {
        uptimeMs: payload.stationUptimeMs,
        bleScanner: true,
    }, now);

    const device = getOrCreateBleDeviceState(deviceId);
    autoLinkBleDeviceToMatchingPatient(device);
    device.stationId = stationId;
    device.rssi = Math.round(payload.rssi);
    device.gattReady = payload.gattReady;
    device.lastGattStatusAt = now;
    device.lastUpdated = now;

    if (payload.gattReady) {
        if (!isBleDeviceSeenToday(device, now)) device.firstSeenToday = now;
        device.lastBleSeen = now;
        device.disconnectedAt = null;
        device.alertStartedAt = null;
        const keepFallAlert = device.status === 'ALERT' && isActiveFallDeviceEventName(device.lastEvent);
        if (!keepFallAlert) device.lastEvent = 'GATT_READY';
        if (!device.sessionBlocked) {
            device.sessionActive = true;
            if (!keepFallAlert) device.status = 'ONLINE';
        }
    } else if (device.lastEvent === 'GATT_READY') {
        device.lastEvent = 'GATT_DISCONNECTED';
    }

    persistBleDeviceState(device);
    broadcastBleDeviceState(device);
    broadcastDeviceTransportState(deviceId);
    if (resolveDeviceTransportState(deviceId).online) dispatchPendingCommandsForDevice(deviceId);
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
            for (const device of bleDeviceStates.values()) {
                if (device.stationId !== station.stationId) continue;
                broadcastDeviceTransportState(device.deviceId);
                if (isDirectDeviceOnline(device.deviceId)) dispatchPendingCommandsForDevice(device.deviceId);
            }
        }
    }

    for (const device of bleDeviceStates.values()) {
        if (device.gattReady && device.lastGattStatusAt
            && nowMs - device.lastGattStatusAt.getTime() >= BLE_GATT_READY_STALE_MS) {
            device.gattReady = false;
            device.lastEvent = 'GATT_STATUS_TIMEOUT';
            device.lastUpdated = now;
            persistBleDeviceState(device);
            broadcastBleDeviceState(device);
            broadcastDeviceTransportState(device.deviceId);
            if (isDirectDeviceOnline(device.deviceId)) dispatchPendingCommandsForDevice(device.deviceId);
        }
        if (device.firstSeenToday && !isBleDeviceSeenToday(device, now)) {
            resetBleDeviceForNewDay(device, now);
            continue;
        }
        if (!device.patientId || !device.sessionActive || !isBleDeviceSeenToday(device, now)) continue;

        const station = device.stationId ? stationStates.get(device.stationId) : null;
        const stationOnline = station?.status === 'ONLINE';
        if (!stationOnline) {
            if (isDirectDeviceOnline(device.deviceId)) {
                const keepFallAlert = device.status === 'ALERT' && isActiveFallDeviceEventName(device.lastEvent);
                const directStatus = keepFallAlert ? 'ALERT' : 'ONLINE';
                if (device.status !== directStatus) {
                    device.status = directStatus;
                    device.disconnectedAt = null;
                    device.alertStartedAt = null;
                    device.lastUpdated = now;
                    persistBleDeviceState(device);
                    broadcastBleDeviceState(device);
                }
                continue;
            }
            if (!device.stationId) {
                const keepFallAlert = device.status === 'ALERT' && isActiveFallDeviceEventName(device.lastEvent);
                const directOnlyStatus = keepFallAlert ? 'ALERT' : 'OFFLINE';
                if (device.status !== directOnlyStatus) {
                    device.status = directOnlyStatus;
                    device.disconnectedAt = null;
                    device.alertStartedAt = null;
                    if (!keepFallAlert) device.lastEvent = 'DIRECT_MQTT_TIMEOUT';
                    device.lastUpdated = now;
                    persistBleDeviceState(device);
                    broadcastBleDeviceState(device);
                    broadcastDeviceTransportState(device.deviceId);
                }
                continue;
            }
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
let wss = null;

function broadcastToDashboards(data) {
    if (!wss) return;
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

function registerDashboardWebSocketHandlers() {
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
            type: 'DEVICE_TRANSPORTS_SYNC',
            data: Array.from(bleDeviceStates.keys()).map(resolveDeviceTransportState),
        }));
        ws.send(JSON.stringify({
            type: 'STATIONS_SYNC',
            data: Array.from(stationStates.values()).map(serializeStationState),
        }));
        ws.send(JSON.stringify({
            type: 'DEVICE_COMMANDS_SYNC',
            data: Array.from(deviceCommands.values()).map(serializeDeviceCommand),
        }));
        ws.on('close', () => console.log(' Một Dashboard Client đã đóng kết nối.'));
    });
}

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

if (brokerUrl.startsWith('ws://') || brokerUrl.startsWith('wss://')) {
    mqttOptions.path = '/mqtt';
}

const mqttClient = mqtt.connect(brokerUrl, mqttOptions);

mqttClient.on('connect', () => {
    console.log(` Đã kết nối MQTT Broker: ${brokerUrl}`);
    mqttClient.subscribe([
        `${MQTT_ROOT_TOPIC}/+/vitals`,
        `${MQTT_ROOT_TOPIC}/stations/+/ble`,
        `${MQTT_ROOT_TOPIC}/stations/+/heartbeat`,
        `${MQTT_ROOT_TOPIC}/stations/+/devices/+/vitals`,
        `${MQTT_ROOT_TOPIC}/stations/+/devices/+/events`,
        `${MQTT_ROOT_TOPIC}/stations/+/devices/+/transport`,
        `${MQTT_ROOT_TOPIC}/devices/+/status`,
        `${MQTT_ROOT_TOPIC}/devices/+/events`,
        `${MQTT_ROOT_TOPIC}/devices/+/acks`,
    ]);
    console.log(' Đang lắng nghe vitals + BLE presence + GATT events/transport + station heartbeat');
    dispatchAllPendingDeviceCommands();
});

mqttClient.on('error', (err) =>
    console.error(' Lỗi kết nối MQTT:', err.message)
);

function handleDirectDeviceMessage(deviceId, channel, liveData) {
    if (!isValidProtocolIdentifier(deviceId, 24)
        || liveData?.protocolVersion !== COMMAND_PROTOCOL_VERSION
        || liveData.deviceId !== deviceId) return false;

    if (channel === 'status') {
        if (liveData.transport !== 'DIRECT_MQTT' || typeof liveData.online !== 'boolean') return false;
        const now = new Date();
        directDeviceStates.set(deviceId, {
            deviceId,
            online: liveData.online,
            wifiRssi: Number.isFinite(liveData.wifiRssi) ? Math.round(liveData.wifiRssi) : null,
            lastSeenAt: now,
        });
        const device = getOrCreateBleDeviceState(deviceId);
        autoLinkBleDeviceToMatchingPatient(device);
        if (typeof liveData.sensorReady === 'boolean') device.sensorReady = liveData.sensorReady;
        const bleReady = isBleGatewayReady(deviceId, now.getTime());
        const keepFallAlert = device.status === 'ALERT' && isActiveFallDeviceEventName(device.lastEvent);
        if (liveData.online && !device.sessionBlocked && !keepFallAlert) {
            device.sessionActive = true;
            device.status = 'ONLINE';
        } else if (!liveData.online && !bleReady) {
            device.status = 'OFFLINE';
        }
        if ((liveData.online || !bleReady) && !keepFallAlert) {
            device.lastEvent = liveData.online ? 'DIRECT_MQTT_ONLINE' : 'DIRECT_MQTT_OFFLINE';
        }
        device.lastUpdated = now;
        persistBleDeviceState(device);
        broadcastBleDeviceState(device, 'DIRECT_DEVICE_STATUS');
        broadcastDeviceTransportState(deviceId);
        if (resolveDeviceTransportState(deviceId).online) dispatchPendingCommandsForDevice(deviceId);
        return true;
    }
    if (channel === 'events') {
        if (!Number.isInteger(liveData.sequence) || liveData.sequence < 0 || liveData.sequence > 255) return false;
        if (!GATT_EVENT_TYPES.has(liveData.event)) return false;
        if (!Number.isInteger(liveData.battery) || liveData.battery < -1 || liveData.battery > 100) return false;
        if (typeof liveData.fallDetected !== 'boolean' || typeof liveData.charging !== 'boolean'
            || typeof liveData.buzzerActive !== 'boolean') return false;
        if (liveData.event === 'FALL_ALERT' && liveData.fallDetected !== true) return false;
        if (liveData.event === 'RECOVERED' && liveData.fallDetected !== false) return false;
        if (!Number.isInteger(liveData.deviceUptimeSeconds) || liveData.deviceUptimeSeconds < 0
            || liveData.deviceUptimeSeconds > 0xFFFFFFFF) return false;

        const now = new Date();
        directDeviceStates.set(deviceId, {
            deviceId,
            online: true,
            wifiRssi: directDeviceStates.get(deviceId)?.wifiRssi ?? null,
            lastSeenAt: now,
        });
        const device = getOrCreateBleDeviceState(deviceId);
        autoLinkBleDeviceToMatchingPatient(device);
        if (!isFreshGatewayEvent(device, liveData)) return true;
        if (liveData.battery >= 0) device.battery = liveData.battery;
        device.lastGattEventSequence = liveData.sequence;
        device.lastGattEventUptimeSeconds = liveData.deviceUptimeSeconds;
        device.lastGattEventAt = now;
        if (liveData.event === 'HARDWARE_FAULT') device.sensorReady = false;
        if (liveData.event === 'HARDWARE_RECOVERED') device.sensorReady = true;
        const eventImpliesActiveFall = liveData.event === 'FALL_ALERT'
            || (liveData.event === 'HEARTBEAT' && liveData.fallDetected === true);
        const shouldKeepFallEvent = isActiveFallDeviceEventName(device.lastEvent)
            && liveData.event === 'HEARTBEAT'
            && !eventImpliesActiveFall;
        if (!shouldKeepFallEvent) {
            device.lastEvent = eventImpliesActiveFall ? 'DIRECT_FALL_ALERT' : `DIRECT_${liveData.event}`;
        }
        device.lastUpdated = now;
        if (!device.sessionBlocked) {
            device.sessionActive = true;
            device.status = eventImpliesActiveFall ? 'ALERT'
                : statusFromDeviceEvent(liveData.event, device.status === 'ALERT' && shouldKeepFallEvent ? 'ALERT' : device.sessionActive ? 'ONLINE' : device.status);
        }
        persistBleDeviceState(device);
        broadcastBleDeviceState(device, 'DIRECT_DEVICE_EVENT');
        broadcastDeviceTransportState(deviceId);
        if (!device.sessionBlocked) dispatchPendingCommandsForDevice(deviceId);
        if (!device.patientId || device.sessionBlocked) {
            logUnmappedFallEvent('DIRECT_MQTT', device, liveData.event);
            return true;
        }

        const patient = patientsState[device.patientId];
        if (!patient || !registeredPatientIds.has(device.patientId)) {
            logUnmappedFallEvent('DIRECT_MQTT', device, liveData.event);
            return true;
        }
        if (liveData.battery >= 0) patient.battery = liveData.battery;
        patient.signalLost = false;
        patient.sourceDeviceId = deviceId;
        patient.sourceStationId = null;
        const firmwareStatus = eventImpliesActiveFall ? 'ALERT' : firmwareStatusFromDeviceEvent(liveData.event);
        if (firmwareStatus) {
            syncFallSafeState(patient, 'status', firmwareStatus);
            console.log(` Cap nhat patient ${device.patientId} tu DIRECT event ${liveData.event}: ${firmwareStatus}`);
            publishPatientUpdate(device.patientId, { saveToDatabase: false, markAsMeasurement: false });
        }
        return true;
    }
    if (channel !== 'acks') return false;

    const command = deviceCommands.get(String(liveData.commandId || ''));
    if (!command || command.deviceId !== deviceId) {
        console.warn(` [MQTT command ack reject] ${deviceId}: command khong khop (${liveData.commandId || 'none'})`);
        return false;
    }
    const validStatuses = new Set(['DELIVERED', 'EXECUTED', 'FAILED']);
    if (!validStatuses.has(liveData.status)) {
        console.warn(` [MQTT command ack reject] ${deviceId}/${command.commandId}: status khong hop le ${liveData.status || 'none'}`);
        return false;
    }
    if (!['BLE_GATEWAY', 'DIRECT_MQTT'].includes(liveData.transport)) {
        console.warn(` [MQTT command ack reject] ${deviceId}/${command.commandId}: transport khong hop le ${liveData.transport || 'none'}`);
        return false;
    }
    if (liveData.transport === 'BLE_GATEWAY') {
        if (!isValidProtocolIdentifier(liveData.stationId, 50)) {
            console.warn(` [MQTT command ack reject] ${deviceId}/${command.commandId}: stationId khong hop le`);
            return false;
        }
        if (command.stationId && command.stationId !== liveData.stationId) {
            console.warn(` [MQTT command ack reject] ${deviceId}/${command.commandId}: station mismatch command=${command.stationId}, ack=${liveData.stationId}`);
            return false;
        }
        if (command.route === 'DIRECT_MQTT' && liveData.status !== 'EXECUTED') {
            console.warn(` [MQTT command ack reject] ${deviceId}/${command.commandId}: bo qua BLE ack ${liveData.status} vi lenh da di DIRECT_MQTT`);
            return false;
        }
    }
    console.log(` Nhan ACK lenh: command=${command.commandId}, device=${deviceId}, status=${liveData.status}, transport=${liveData.transport}${liveData.error ? `, error=${liveData.error}` : ''}`);
    command.status = liveData.status;
    command.route = liveData.transport === 'BLE_GATEWAY' ? 'BLE_GATEWAY' : 'DIRECT_MQTT';
    command.stationId = liveData.stationId || command.stationId;
    command.error = liveData.error || null;
    command.acknowledgedAt = new Date();
    if (liveData.status === 'FAILED' && liveData.transport === 'BLE_GATEWAY') {
        markBleGatewayCommandRouteUnready(deviceId, liveData.error);
        command.status = 'PENDING';
        command.route = 'PENDING';
        command.stationId = null;
        command.error = 'BLE gateway failed; waiting for DIRECT_MQTT';
        persistDeviceCommand(command);
        broadcastDeviceCommand(command);
        if (isDirectDeviceOnline(deviceId)) dispatchDeviceCommand(command);
        return true;
    }
    persistDeviceCommand(command);
    broadcastDeviceCommand(command);
    return true;
}

mqttClient.on('message', (topic, message, packet) => {
    try {
        const parts = topic.split('/');
        if (packet?.retain) {
            console.log(` Bỏ qua retained MQTT trên topic: ${topic}`);
            return;
        }

        const liveData = JSON.parse(message.toString().trim());

        if (parts.length === 4 && parts[0] === MQTT_ROOT_TOPIC && parts[1] === 'devices'
            && (parts[3] === 'status' || parts[3] === 'events' || parts[3] === 'acks')) {
            const deviceId = parts[2];
            if (!handleDirectDeviceMessage(deviceId, parts[3], liveData)) {
                console.warn(` [MQTT command protocol reject] ${topic}`);
            }
            return;
        }

        if (parts.length === 6 && parts[0] === MQTT_ROOT_TOPIC && parts[1] === 'stations'
            && parts[3] === 'devices'
            && (parts[5] === 'vitals' || parts[5] === 'events' || parts[5] === 'transport')) {
            const stationId = parts[2];
            const deviceId = parts[4];
            const channel = parts[5];
            const validationError = channel === 'vitals'
                ? validateGatewayVitalsPayload(liveData, stationId, deviceId)
                : channel === 'events'
                    ? validateGatewayEventPayload(liveData, stationId, deviceId)
                    : validateGatewayTransportPayload(liveData, stationId, deviceId);
            if (validationError) {
                console.warn(` [MQTT gateway ${channel} reject] ${topic}: ${validationError}`);
                return;
            }
            if (channel === 'vitals') handleGatewayVitalsPayload(liveData, stationId, deviceId);
            else if (channel === 'events') handleGatewayEventPayload(liveData, stationId, deviceId);
            else handleGatewayTransportPayload(liveData, stationId, deviceId);
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

    const legacyQuery = `
        INSERT INTO patients
        (id, name, age, gender, room, bed, condition_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    `;
    const legacyValues = [
        profile.id, profile.name, profile.age,
        profile.gender, profile.room, profile.bed, profile.conditionSummary,
    ];

    // Dùng pool đang hoạt động thay vì mở TCP/TLS connection mới cho từng lần lưu.
    // Điều này tránh hai lần connect timeout liên tiếp giữa Render và Railway.
    db.query(
        { sql: withDateOfBirth, timeout: REGISTRATION_DB_TIMEOUT_MS },
        values,
        (error, result) => {
            if (!error || error.code !== 'ER_BAD_FIELD_ERROR') {
                return callback(error, result, true);
            }
            // Tương thích database cũ chưa có date_of_birth.
            db.query(
                { sql: legacyQuery, timeout: REGISTRATION_DB_TIMEOUT_MS },
                legacyValues,
                (legacyError, legacyResult) => callback(legacyError, legacyResult, false)
            );
        }
    );
}

app.get('/api/pending-registrations', (req, res) => {
    res.json(Array.from(pendingRegistrations.values()).map(serializePendingRegistration));
});

app.post('/api/auth/login', (req, res) => {
    const login = String(req.body?.email || req.body?.username || '').trim().toLowerCase();
    const password = String(req.body?.password || req.body?.pin || '');
    if (!login || !password) return res.status(400).json({ error: 'Tài khoản và mật khẩu không hợp lệ' });

    if (DEMO_LOGIN_ENABLED && login === DEMO_LOGIN_USER && password === DEMO_LOGIN_PASSWORD) {
        const token = crypto.randomBytes(32).toString('hex');
        const authUser = { id: 0, email: DEMO_LOGIN_USER, role: 'HEAD_NURSE', demo: true };
        authSessions.set(token, {
            user: authUser,
            expiresAt: Date.now() + AUTH_SESSION_TTL_MS,
        });
        return res.json({ token, user: authUser, expiresInMs: AUTH_SESSION_TTL_MS, demo: true });
    }

    const email = normalizeAccountEmail(login);
    if (!email) return res.status(400).json({ error: 'Email tài khoản manager không hợp lệ' });

    db.query(
        'SELECT id, email, password_salt, password_hash, role, active FROM users WHERE email = ? LIMIT 1',
        [email],
        async (error, rows = []) => {
            if (error) {
                console.error(' Lỗi kiểm tra tài khoản đăng nhập:', error.code || 'DB_ERROR', error.message);
                return res.status(500).json({
                    error: 'Không thể kiểm tra tài khoản',
                    details: error.message,
                });
            }
            const user = rows[0];
            if (!user || !user.active) return res.status(401).json({ error: 'Sai email hoặc mật khẩu' });
            try {
                const valid = await verifyPassword(password, user.password_salt, user.password_hash);
                if (!valid) return res.status(401).json({ error: 'Sai email hoặc mật khẩu' });
                const token = crypto.randomBytes(32).toString('hex');
                const authUser = serializeAuthUser(user);
                authSessions.set(token, {
                    user: authUser,
                    expiresAt: Date.now() + AUTH_SESSION_TTL_MS,
                });
                res.json({ token, user: authUser, expiresInMs: AUTH_SESSION_TTL_MS });
            } catch (verifyError) {
                console.error(' Lỗi xác thực tài khoản:', verifyError.message);
                res.status(500).json({ error: 'Không thể xác thực tài khoản' });
            }
        }
    );
});

app.post('/api/auth/logout', requireApiAuth, (req, res) => {
    const token = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    authSessions.delete(token);
    res.json({ message: 'Đã đăng xuất' });
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

app.post('/api/patients/:id/device', requireApiAuth, (req, res) => {
    const patientId = String(req.params.id || '').trim();
    const deviceId = String(req.body?.deviceId || '').trim();
    if (!registeredPatientIds.has(patientId)) return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    if (!isValidProtocolIdentifier(deviceId, 24)) return res.status(400).json({ error: 'deviceId không hợp lệ' });

    const target = bleDeviceStates.get(deviceId);
    if (!target) return res.status(404).json({ error: 'Thiết bị chưa từng được hệ thống phát hiện' });

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
        broadcastDeviceTransportState(existing.deviceId);
    }

    target.patientId = patientId;
    target.sessionBlocked = false;
    target.lastEvent = 'LINKED_TO_PATIENT';
    target.lastUpdated = new Date();
    persistBleDeviceState(target);
    broadcastBleDeviceState(target);
    broadcastDeviceTransportState(target.deviceId);
    res.json({ message: 'Đã liên kết thiết bị với bệnh nhân', device: serializeBleDeviceState(target) });
});

app.post('/api/ble/devices/:deviceId/end-session', requireApiAuth, (req, res) => {
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
    broadcastDeviceTransportState(device.deviceId);
    res.json({ message: 'Đã kết thúc phiên theo dõi', device: serializeBleDeviceState(device) });
});

app.post('/api/ble/devices/:deviceId/start-session', requireApiAuth, (req, res) => {
    const deviceId = String(req.params.deviceId || '').trim();
    const device = bleDeviceStates.get(deviceId);
    if (!device) return res.status(404).json({ error: 'Không tìm thấy thiết bị BLE' });
    if (!device.patientId) return res.status(409).json({ error: 'Thiết bị chưa liên kết với bệnh nhân' });

    const now = new Date();
    device.sessionBlocked = false;
    const direct = directDeviceStates.get(deviceId);
    const directRecentlyOnline = Boolean(direct?.online && direct.lastSeenAt
        && now.getTime() - direct.lastSeenAt.getTime() < DIRECT_DEVICE_TIMEOUT_MS);
    device.sessionActive = isBleDeviceSeenToday(device, now) || directRecentlyOnline;
    const station = device.stationId ? stationStates.get(device.stationId) : null;
    const recentlySeen = device.lastBleSeen
        && now.getTime() - device.lastBleSeen.getTime() < BLE_DEVICE_SEEN_STALE_MS;
    const bleRecentlyOnline = station?.status === 'ONLINE' && recentlySeen;
    device.status = device.sessionActive && (bleRecentlyOnline || directRecentlyOnline) ? 'ONLINE' : 'OFFLINE';
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'START_SESSION';
    device.lastUpdated = now;
    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_SESSION_STARTED');
    broadcastDeviceTransportState(device.deviceId);
    res.json({ message: 'Đã bắt đầu phiên theo dõi', device: serializeBleDeviceState(device) });
});

app.delete('/api/patients/:id/device', requireApiAuth, (req, res) => {
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
    broadcastDeviceTransportState(device.deviceId);
    res.json({ message: 'Đã gỡ liên kết thiết bị', device: serializeBleDeviceState(device) });
});

app.get('/api/device-commands', (req, res) => {
    const patientId = String(req.query.patientId || '').trim();
    const deviceId = String(req.query.deviceId || '').trim();
    const commands = Array.from(deviceCommands.values())
        .filter(command => (!patientId || command.patientId === patientId) && (!deviceId || command.deviceId === deviceId))
        .sort((a, b) => b.createdAt - a.createdAt)
        .slice(0, 100)
        .map(serializeDeviceCommand);
    res.json(commands);
});

app.post('/api/devices/:deviceId/commands', requireApiAuth, (req, res) => {
    try {
        const deviceId = String(req.params.deviceId || '').trim();
        const action = String(req.body?.action || '').trim().toUpperCase();
        const durationMs = Number(req.body?.durationMs ?? 10000);
        const wearable = bleDeviceStates.get(deviceId);
        if (!wearable) return res.status(404).json({ error: 'Thiết bị chưa từng được phát hiện' });
        if (!wearable.patientId) return res.status(409).json({ error: 'Thiết bị chưa gán cho bệnh nhân' });
        if (!wearable.sessionActive || wearable.sessionBlocked) {
            return res.status(409).json({ error: 'Phiên theo dõi thiết bị chưa hoạt động' });
        }
        if (!['BUZZER_ON', 'BUZZER_OFF', 'BUZZER_TEST'].includes(action)) return res.status(400).json({ error: 'Action không hợp lệ' });
        if (!Number.isInteger(durationMs) || durationMs < 0 || durationMs > 300000) return res.status(400).json({ error: 'durationMs không hợp lệ' });

        const now = new Date();
        const command = {
            commandId: `CMD-${Date.now()}-${crypto.randomBytes(3).toString('hex').toUpperCase()}`,
            deviceId,
            patientId: wearable.patientId,
            stationId: wearable.stationId,
            action,
            durationMs,
            route: 'PENDING',
            status: 'PENDING',
            error: null,
            createdAt: now,
            expiresAt: new Date(now.getTime() + 5 * 60 * 1000),
            dispatchedAt: null,
            acknowledgedAt: null,
        };
        deviceCommands.set(command.commandId, command);
        persistDeviceCommand(command);
        console.log(` Tao lenh thiet bi: command=${command.commandId}, device=${deviceId}, patient=${wearable.patientId}, transport=${JSON.stringify(resolveDeviceTransportState(deviceId))}`);
        dispatchDeviceCommand(command);
        res.status(202).json({ message: 'Lệnh đã được tạo', command: serializeDeviceCommand(command) });
    } catch (error) {
        console.error(' Lỗi tạo/gửi lệnh thiết bị:', error.message);
        res.status(500).json({ error: 'Không thể tạo hoặc gửi lệnh thiết bị', details: error.message });
    }
});

app.post('/api/patients/:id/delete-otp', requireApiAuth, requireMinimumRole('HEAD_NURSE'), (req, res) => {
    const patientId = String(req.params.id || '').trim();
    if (!registeredPatientIds.has(patientId)) return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    if (!checkDeleteOtpRateLimit(req, patientId)) {
        return res.status(429).json({ error: 'Yêu cầu mã xác nhận quá nhiều lần; vui lòng thử lại sau' });
    }
    if ((!resendConfigured && !smtpConfigured) || !process.env.OTP_SECRET) {
        return res.status(503).json({ error: 'Backend chưa cấu hình dịch vụ email/OTP_SECRET' });
    }

    const configuredManagerEmail = normalizeAccountEmail(process.env.MANAGER_EMAIL);
    const query = configuredManagerEmail
        ? 'SELECT email, role, active FROM users WHERE email = ? AND active = 1 LIMIT 1'
        : "SELECT email, role, active FROM users WHERE active = 1 AND role IN ('HEAD_NURSE','MANAGER','ADMIN') ORDER BY FIELD(role,'MANAGER','ADMIN','HEAD_NURSE') LIMIT 1";
    const params = configuredManagerEmail ? [configuredManagerEmail] : [];
    db.query(query, params, async (error, rows) => {
        if (error) return res.status(500).json({ error: 'Không thể kiểm tra tài khoản' });
        const user = rows[0];
        if (!user || !user.active || (ROLE_LEVELS[user.role] || 0) < ROLE_LEVELS.HEAD_NURSE) {
            return res.status(503).json({ error: 'Chưa có tài khoản Y tá trưởng/Quản lý hoạt động' });
        }
        const email = normalizeAccountEmail(user.email);

        const existing = Array.from(deleteOtpChallenges.values()).find(item => item.patientId === patientId && item.email === email);
        if (existing && Date.now() - existing.requestedAt < DELETE_OTP_RESEND_MS) {
            return res.status(429).json({ error: 'Vui lòng chờ 60 giây trước khi gửi lại mã' });
        }

        const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
        let otp = '';
        for (let index = 0; index < 5; index += 1) otp += alphabet[crypto.randomInt(0, alphabet.length)];
        const challengeId = crypto.randomBytes(18).toString('hex');
        deleteOtpChallenges.set(challengeId, {
            patientId,
            email,
            hash: hashDeleteOtp(patientId, email, otp),
            expiresAt: Date.now() + DELETE_OTP_TTL_MS,
            requestedAt: Date.now(),
            attempts: 0,
        });

        try {
            console.log(` Đang gửi OTP xóa bệnh nhân ${patientId} qua ${resendConfigured ? 'Resend HTTPS' : 'SMTP'}...`);
            const mailResult = await sendOtpEmail({ to: email, patientId, otp });
            console.log(` Đã gửi OTP qua ${mailResult.provider}, messageId=${mailResult.messageId}`);
            const [localPart, domain] = email.split('@');
            const maskedEmail = `${localPart.slice(0, 2)}${'*'.repeat(Math.max(3, localPart.length - 2))}@${domain}`;
            res.json({
                message: 'Mã xác nhận 5 ký tự đã được gửi tới email quản lý',
                challengeId,
                maskedEmail,
                expiresInSeconds: 300,
            });
        } catch (mailError) {
            deleteOtpChallenges.delete(challengeId);
            console.error(' Lỗi gửi email OTP:', mailError.code || 'EMAIL_PROVIDER_ERROR', mailError.message);
            res.status(502).json({ error: 'Không thể gửi email xác nhận' });
        }
    });
});

function requireDeleteOtp(req, res, next) {
    const patientId = String(req.params.id || '').trim();
    const challengeId = String(req.body?.challengeId || '').trim();
    const otp = String(req.body?.otp || '').trim().toUpperCase();
    if (!/^[a-f0-9]{36}$/.test(challengeId) || !/^[A-Z2-9]{5}$/.test(otp)) return res.status(400).json({ error: 'Phiên xác minh hoặc mã OTP không hợp lệ' });

    const challenge = deleteOtpChallenges.get(challengeId);
    if (!challenge || challenge.patientId !== patientId || challenge.expiresAt < Date.now()) {
        deleteOtpChallenges.delete(challengeId);
        return res.status(410).json({ error: 'Mã xác nhận đã hết hạn hoặc không tồn tại' });
    }
    if (challenge.attempts >= 5) {
        deleteOtpChallenges.delete(challengeId);
        return res.status(429).json({ error: 'Đã nhập sai quá 5 lần; vui lòng yêu cầu mã mới' });
    }
    challenge.attempts += 1;
    const expected = Buffer.from(challenge.hash, 'hex');
    const supplied = Buffer.from(hashDeleteOtp(patientId, challenge.email, otp), 'hex');
    if (!crypto.timingSafeEqual(expected, supplied)) return res.status(403).json({ error: 'Mã xác nhận không đúng' });

    deleteOtpChallenges.delete(challengeId);
    req.deleteAuthorizedEmail = challenge.email;
    next();
}

app.delete('/api/patients/:id', requireApiAuth, requireMinimumRole('HEAD_NURSE'), requireDeleteOtp, (req, res) => {
    const patientId = String(req.params.id || '').trim();
    if (!registeredPatientIds.has(patientId)) {
        return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    }

    db.getConnection((connectionError, connection) => {
        if (connectionError) {
            console.error(` Lỗi lấy kết nối để xóa bệnh nhân ${patientId}:`, connectionError.message);
            return res.status(500).json({ error: 'Không thể kết nối database' });
        }

        connection.beginTransaction(beginError => {
            if (beginError) {
                connection.release();
                return res.status(500).json({ error: 'Không thể bắt đầu giao dịch xóa' });
            }

            connection.query(`
                UPDATE ble_devices
                SET patient_id = NULL,
                    session_active = 0,
                    session_blocked = 0,
                    ble_status = 'OFFLINE',
                    disconnected_at = NULL,
                    alert_started_at = NULL,
                    last_event = 'PATIENT_DELETED'
                WHERE patient_id = ?
            `, [patientId], unlinkError => {
                if (unlinkError) return rollbackDelete(connection, res, patientId, unlinkError);

                connection.query('DELETE FROM vitals_log WHERE patient_id = ?', [patientId], (vitalsError, vitalsResult) => {
                    if (vitalsError) return rollbackDelete(connection, res, patientId, vitalsError);

                    connection.query('DELETE FROM patients WHERE id = ?', [patientId], (patientError, patientResult) => {
                        if (patientError) return rollbackDelete(connection, res, patientId, patientError);
                        if (!patientResult.affectedRows) {
                            return rollbackDelete(connection, res, patientId, new Error('Hồ sơ không còn tồn tại'));
                        }

                        connection.commit(commitError => {
                            if (commitError) return rollbackDelete(connection, res, patientId, commitError);
                            connection.release();

                            for (const device of bleDeviceStates.values()) {
                                if (device.patientId !== patientId) continue;
                                device.patientId = null;
                                device.sessionActive = false;
                                device.sessionBlocked = false;
                                device.status = 'OFFLINE';
                                device.disconnectedAt = null;
                                device.alertStartedAt = null;
                                device.lastEvent = 'PATIENT_DELETED';
                                device.lastUpdated = new Date();
                                persistBleDeviceState(device);
                                broadcastBleDeviceState(device);
                                broadcastDeviceTransportState(device.deviceId);
                            }

                            registeredPatientIds.delete(patientId);
                            pendingRegistrations.delete(patientId);
                            lastSeenByPatient.delete(patientId);
                            delete patientsState[patientId];
                            broadcastToDashboards({ type: 'PATIENT_DELETED', data: { id: patientId } });

                            res.json({
                                message: 'Đã xóa bệnh nhân và toàn bộ lịch sử đo',
                                patientId,
                                deletedVitals: vitalsResult.affectedRows,
                            });
                        });
                    });
                });
            });
        });
    });
});

function rollbackDelete(connection, res, patientId, error) {
    connection.rollback(() => {
        connection.release();
        console.error(` Lỗi xóa bệnh nhân ${patientId}:`, error.message);
        if (!res.headersSent) res.status(500).json({ error: 'Không thể xóa bệnh nhân', details: error.message });
    });
}

function serializeDeviceCommand(command) {
    return {
        commandId: command.commandId,
        deviceId: command.deviceId,
        patientId: command.patientId || null,
        stationId: command.stationId || null,
        action: command.action,
        durationMs: command.durationMs,
        route: command.route,
        status: command.status,
        error: command.error || null,
        createdAt: toIsoOrNull(command.createdAt),
        expiresAt: toIsoOrNull(command.expiresAt),
        dispatchedAt: toIsoOrNull(command.dispatchedAt),
        acknowledgedAt: toIsoOrNull(command.acknowledgedAt),
    };
}

function persistDeviceCommand(command) {
    db.query(`
        INSERT INTO device_commands
        (command_id, device_id, patient_id, station_id, action, duration_ms, route,
         command_status, error_message, created_at, expires_at, dispatched_at, acknowledged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE station_id=VALUES(station_id), route=VALUES(route),
          command_status=VALUES(command_status), error_message=VALUES(error_message),
          dispatched_at=VALUES(dispatched_at), acknowledged_at=VALUES(acknowledged_at)
    `, [command.commandId, command.deviceId, command.patientId, command.stationId,
        command.action, command.durationMs, command.route, command.status, command.error,
        command.createdAt, command.expiresAt, command.dispatchedAt, command.acknowledgedAt], error => {
        if (error) console.error(' Lỗi lưu lệnh thiết bị:', error.message);
    });
}

function broadcastDeviceCommand(command) {
    broadcastToDashboards({ type: 'DEVICE_COMMAND_UPDATE', data: serializeDeviceCommand(command) });
}

function loadPendingDeviceCommands() {
    db.query(`SELECT * FROM device_commands ORDER BY created_at DESC LIMIT 100`, (error, rows = []) => {
        if (error) return console.error(' Lỗi load lệnh thiết bị:', error.message);
        rows.forEach(row => deviceCommands.set(row.command_id, {
            commandId: row.command_id, deviceId: row.device_id, patientId: row.patient_id,
            stationId: row.station_id, action: row.action, durationMs: Number(row.duration_ms),
            route: row.route, status: row.command_status, error: row.error_message,
            createdAt: new Date(row.created_at), expiresAt: new Date(row.expires_at),
            dispatchedAt: row.dispatched_at ? new Date(row.dispatched_at) : null,
            acknowledgedAt: row.acknowledged_at ? new Date(row.acknowledged_at) : null,
        }));
        broadcastToDashboards({
            type: 'DEVICE_COMMANDS_SYNC',
            data: Array.from(deviceCommands.values()).map(serializeDeviceCommand),
        });
        dispatchAllPendingDeviceCommands();
    });
}

function isDirectDeviceOnline(deviceId) {
    const direct = directDeviceStates.get(deviceId);
    const device = bleDeviceStates.get(deviceId);
    return Boolean(device?.sessionActive && !device.sessionBlocked && direct?.online
        && Date.now() - direct.lastSeenAt.getTime() < DIRECT_DEVICE_TIMEOUT_MS);
}

function isBleGatewayReady(deviceId, nowMs = Date.now()) {
    const device = bleDeviceStates.get(deviceId);
    if (!device?.sessionActive || !device.gattReady || !device.lastGattStatusAt || device.sessionBlocked) return false;
    const station = device.stationId ? stationStates.get(device.stationId) : null;
    return station?.status === 'ONLINE'
        && nowMs - device.lastGattStatusAt.getTime() < BLE_GATT_READY_STALE_MS;
}

function resolveDeviceTransportState(deviceId, nowMs = Date.now()) {
    const device = bleDeviceStates.get(deviceId);
    const direct = directDeviceStates.get(deviceId);
    const station = device?.stationId ? stationStates.get(device.stationId) : null;
    const gattStatusAgeMs = device?.lastGattStatusAt ? Math.max(0, nowMs - device.lastGattStatusAt.getTime()) : null;
    const directStatusAgeMs = direct?.lastSeenAt ? Math.max(0, nowMs - direct.lastSeenAt.getTime()) : null;
    const bleOnline = isBleGatewayReady(deviceId, nowMs);
    const directOnline = Boolean(device?.sessionActive && !device.sessionBlocked && direct?.online
        && nowMs - direct.lastSeenAt.getTime() < DIRECT_DEVICE_TIMEOUT_MS);
    const activeTransport = bleOnline ? 'BLE_GATEWAY' : directOnline ? 'DIRECT_MQTT' : 'OFFLINE';
    return {
        deviceId,
        activeTransport,
        transport: activeTransport,
        online: activeTransport !== 'OFFLINE',
        bleGatewayOnline: bleOnline,
        stationId: bleOnline ? device?.stationId || null : null,
        candidateStationId: device?.stationId || null,
        stationStatus: station?.status || null,
        sessionActive: device?.sessionActive === true,
        sessionBlocked: device?.sessionBlocked === true,
        gattReady: device?.gattReady === true,
        gattStatusFresh: Number.isFinite(gattStatusAgeMs) && gattStatusAgeMs < BLE_GATT_READY_STALE_MS,
        gattStatusAgeMs,
        directMqttOnline: directOnline,
        directStatusAgeMs,
        wifiRssi: directOnline && Number.isFinite(direct?.wifiRssi) ? direct.wifiRssi : null,
        lastDirectSeenAt: toIsoOrNull(direct?.lastSeenAt),
        lastGattStatusAt: toIsoOrNull(device?.lastGattStatusAt),
    };
}

function describeTransportBlocker(transport) {
    if (!transport) return 'NO_TRANSPORT: no snapshot';
    const gattAge = Number.isFinite(transport.gattStatusAgeMs) ? Math.round(transport.gattStatusAgeMs) : 'none';
    const directAge = Number.isFinite(transport.directStatusAgeMs) ? Math.round(transport.directStatusAgeMs) : 'none';
    return `NO_TRANSPORT: session=${transport.sessionActive ? 1 : 0}, blocked=${transport.sessionBlocked ? 1 : 0}, `
        + `station=${transport.candidateStationId || 'none'}, stationStatus=${transport.stationStatus || 'none'}, `
        + `gattReady=${transport.gattReady ? 1 : 0}, gattFresh=${transport.gattStatusFresh ? 1 : 0}, gattAgeMs=${gattAge}, `
        + `ble=${transport.bleGatewayOnline ? 1 : 0}, direct=${transport.directMqttOnline ? 1 : 0}, directAgeMs=${directAge}`;
}

function broadcastDeviceTransportState(deviceId) {
    broadcastToDashboards({
        type: 'DEVICE_TRANSPORT_UPDATE',
        data: resolveDeviceTransportState(deviceId),
    });
}

function markBleGatewayCommandRouteUnready(deviceId, errorMessage) {
    const transientBleErrors = new Set([
        'GATT_NOT_READY',
        'GATT_WRITE_FAILED',
        'DEVICE_ACK_TIMEOUT',
        'STATION_COMMAND_QUEUE_TIMEOUT',
        'STATION_COMMAND_QUEUE_FULL',
    ]);
    if (!transientBleErrors.has(String(errorMessage || ''))) return;
    const device = bleDeviceStates.get(deviceId);
    if (!device?.gattReady) return;
    device.gattReady = false;
    device.lastGattStatusAt = new Date();
    device.lastUpdated = new Date();
    persistBleDeviceState(device);
    broadcastBleDeviceState(device, 'BLE_GATEWAY_COMMAND_ROUTE_UNREADY');
    broadcastDeviceTransportState(deviceId);
}

function dispatchDeviceCommand(command) {
    if (command.expiresAt.getTime() <= Date.now()) return false;
    if (!mqttClient.connected) {
        command.route = 'PENDING';
        command.status = 'PENDING';
        command.error = 'BACKEND_MQTT_OFFLINE';
        persistDeviceCommand(command);
        broadcastDeviceCommand(command);
        console.warn(` Chua gui lenh ${command.commandId}: backend MQTT chua ket noi.`);
        return false;
    }
    const wearable = bleDeviceStates.get(command.deviceId);
    const transport = resolveDeviceTransportState(command.deviceId);
    let topic;
    if (transport.activeTransport === 'BLE_GATEWAY') {
        command.route = 'BLE_GATEWAY';
        command.stationId = wearable.stationId;
        topic = `${MQTT_ROOT_TOPIC}/stations/${wearable.stationId}/devices/${command.deviceId}/commands`;
    } else if (transport.activeTransport === 'DIRECT_MQTT') {
        command.route = 'DIRECT_MQTT';
        command.stationId = null;
        topic = `${MQTT_ROOT_TOPIC}/devices/${command.deviceId}/commands`;
    } else {
        command.route = 'PENDING';
        command.status = 'PENDING';
        command.stationId = null;
        command.error = describeTransportBlocker(transport);
        persistDeviceCommand(command);
        broadcastDeviceCommand(command);
        console.warn(` Chua gui lenh ${command.commandId}: ${command.error}`);
        return false;
    }
    const payload = JSON.stringify({
        protocolVersion: COMMAND_PROTOCOL_VERSION,
        commandId: command.commandId,
        deviceId: command.deviceId,
        action: command.action,
        durationMs: command.durationMs,
        expiresAt: command.expiresAt.toISOString(),
    });
    console.log(` Gui lenh thiet bi: command=${command.commandId}, device=${command.deviceId}, action=${command.action}, route=${command.route}, topic=${topic}`);
    command.status = 'DISPATCHED';
    command.error = null;
    command.dispatchedAt = new Date();
    persistDeviceCommand(command);
    broadcastDeviceCommand(command);
    try {
        mqttClient.publish(topic, payload, { qos: 1, retain: false }, error => {
            if (!error) {
                console.log(` Da publish lenh ${command.commandId} qua ${command.route}`);
                return;
            }
            command.status = 'PENDING';
            command.route = 'PENDING';
            command.error = `MQTT_PUBLISH_ERROR: ${error.message}`;
            command.dispatchedAt = null;
            console.error(` Loi publish lenh ${command.commandId}:`, error.message);
            persistDeviceCommand(command);
            broadcastDeviceCommand(command);
        });
    } catch (error) {
        command.status = 'PENDING';
        command.route = 'PENDING';
        command.error = `MQTT_PUBLISH_THROW: ${error.message}`;
        command.dispatchedAt = null;
        persistDeviceCommand(command);
        broadcastDeviceCommand(command);
        console.error(` Loi publish lenh ${command.commandId}:`, error.message);
    }
    return true;
}

function dispatchPendingCommandsForDevice(deviceId) {
    const transport = resolveDeviceTransportState(deviceId);
    for (const command of deviceCommands.values()) {
        if (command.deviceId !== deviceId || command.expiresAt.getTime() <= Date.now()) continue;
        if (transport.activeTransport === 'DIRECT_MQTT' && command.route === 'BLE_GATEWAY'
            && ['DISPATCHED', 'DELIVERED'].includes(command.status)) {
            command.status = 'PENDING';
            command.route = 'PENDING';
            command.stationId = null;
            command.error = 'BLE unavailable; rerouting same commandId to DIRECT_MQTT';
            persistDeviceCommand(command);
            broadcastDeviceCommand(command);
        }
        if (command.status === 'PENDING') dispatchDeviceCommand(command);
    }
}

function dispatchAllPendingDeviceCommands() {
    for (const command of deviceCommands.values()) {
        if (command.status === 'PENDING' && command.expiresAt.getTime() > Date.now()) {
            dispatchDeviceCommand(command);
        }
    }
}

function inFlightCommandAgeMs(command, nowMs = Date.now()) {
    const reference = command.acknowledgedAt || command.dispatchedAt;
    return reference ? nowMs - reference.getTime() : 0;
}

function resetCommandForRetry(command, errorMessage) {
    command.status = 'PENDING';
    command.route = 'PENDING';
    command.stationId = null;
    command.error = errorMessage;
    command.dispatchedAt = null;
    command.acknowledgedAt = null;
    persistDeviceCommand(command);
    broadcastDeviceCommand(command);
}

function retryTimedOutDeviceCommands() {
    const now = Date.now();
    for (const command of deviceCommands.values()) {
        if (!['DISPATCHED', 'DELIVERED'].includes(command.status)) continue;
        if (command.expiresAt.getTime() <= now) continue;
        if (inFlightCommandAgeMs(command, now) < DEVICE_COMMAND_ACK_TIMEOUT_MS) continue;

        const previousRoute = command.route;
        const transport = resolveDeviceTransportState(command.deviceId, now);
        if (!transport.online) {
            resetCommandForRetry(command,
                `${previousRoute}_ACK_TIMEOUT; ${describeTransportBlocker(transport)}`);
            continue;
        }

        const nextRoute = transport.activeTransport;
        resetCommandForRetry(command, previousRoute === nextRoute
            ? `${previousRoute}_ACK_TIMEOUT; retrying ${nextRoute}`
            : `${previousRoute}_ACK_TIMEOUT; rerouting to ${nextRoute}`);
        dispatchDeviceCommand(command);
    }
}

function queueAutomaticBuzzerCommand(patient) {
    const nowMs = Date.now();
    if (nowMs - (lastAutoBuzzerAt.get(patient.id) || 0) < 60000) return;
    const wearable = Array.from(bleDeviceStates.values()).find(device => device.patientId === patient.id);
    if (!wearable || !wearable.sessionActive || wearable.sessionBlocked) return;
    lastAutoBuzzerAt.set(patient.id, nowMs);
    const now = new Date(nowMs);
    const command = {
        commandId: `ALERT-${nowMs}-${crypto.randomBytes(3).toString('hex').toUpperCase()}`,
        deviceId: wearable.deviceId, patientId: patient.id, stationId: wearable.stationId,
        action: 'BUZZER_ON', durationMs: 30000, route: 'PENDING', status: 'PENDING', error: null,
        createdAt: now, expiresAt: new Date(nowMs + 5 * 60 * 1000), dispatchedAt: null, acknowledgedAt: null,
    };
    deviceCommands.set(command.commandId, command);
    persistDeviceCommand(command);
    dispatchDeviceCommand(command);
}

function expireDeviceCommands() {
    const now = Date.now();
    for (const command of deviceCommands.values()) {
        if (!['PENDING', 'DISPATCHED', 'DELIVERED'].includes(command.status) || command.expiresAt.getTime() > now) continue;
        const previousError = command.error;
        command.status = 'EXPIRED';
        command.error = previousError ? `Hết thời gian giao lệnh; last=${previousError}` : 'Hết thời gian giao lệnh';
        persistDeviceCommand(command);
        broadcastDeviceCommand(command);
    }
    for (const [deviceId, direct] of directDeviceStates) {
        if (direct.online && now - direct.lastSeenAt.getTime() >= DIRECT_DEVICE_TIMEOUT_MS) {
            direct.online = false;
            broadcastDeviceTransportState(deviceId);
        }
    }
}

setInterval(() => {
    retryTimedOutDeviceCommands();
    dispatchAllPendingDeviceCommands();
    expireDeviceCommands();
}, DEVICE_COMMAND_RETRY_INTERVAL_MS);

function hashPassword(password, salt = crypto.randomBytes(16).toString('hex')) {
    return new Promise((resolve, reject) => {
        crypto.scrypt(password, salt, 64, (error, derivedKey) => {
            if (error) reject(error);
            else resolve({ salt, hash: derivedKey.toString('hex') });
        });
    });
}

function verifyPassword(password, salt, expectedHash) {
    return new Promise((resolve, reject) => {
        crypto.scrypt(password, salt, 64, (error, derivedKey) => {
            if (error) {
                reject(error);
                return;
            }
            const expected = Buffer.from(String(expectedHash || ''), 'hex');
            resolve(expected.length === derivedKey.length && crypto.timingSafeEqual(expected, derivedKey));
        });
    });
}

function serializeAuthUser(user) {
    return {
        id: user.id,
        email: user.email,
        role: user.role,
    };
}

function requireApiAuth(req, res, next) {
    const header = String(req.headers.authorization || '');
    const match = header.match(/^Bearer\s+([A-Fa-f0-9]{64})$/);
    if (!match) return res.status(401).json({ error: 'Cần đăng nhập để thực hiện thao tác này' });
    const session = authSessions.get(match[1]);
    if (!session || session.expiresAt <= Date.now()) {
        authSessions.delete(match[1]);
        return res.status(401).json({ error: 'Phiên đăng nhập đã hết hạn' });
    }
    session.expiresAt = Date.now() + AUTH_SESSION_TTL_MS;
    req.user = session.user;
    next();
}

function requireMinimumRole(minimumRole) {
    return (req, res, next) => {
        const currentLevel = ROLE_LEVELS[req.user?.role] || 0;
        const requiredLevel = ROLE_LEVELS[minimumRole] || 0;
        if (currentLevel < requiredLevel) {
            return res.status(403).json({ error: 'Tài khoản không đủ quyền thực hiện thao tác này' });
        }
        next();
    };
}

function checkDeleteOtpRateLimit(req, patientId) {
    const key = `${req.ip || req.socket?.remoteAddress || 'unknown'}:${patientId}`;
    const now = Date.now();
    const bucket = deleteOtpRequestWindows.get(key) || { startedAt: now, count: 0 };
    if (now - bucket.startedAt >= DELETE_OTP_WINDOW_MS) {
        bucket.startedAt = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    deleteOtpRequestWindows.set(key, bucket);
    return bucket.count <= DELETE_OTP_MAX_REQUESTS_PER_WINDOW;
}

setInterval(() => {
    const now = Date.now();
    for (const [id, challenge] of deleteOtpChallenges.entries()) {
        if (challenge.expiresAt <= now) deleteOtpChallenges.delete(id);
    }
    for (const [key, bucket] of deleteOtpRequestWindows.entries()) {
        if (now - bucket.startedAt >= DELETE_OTP_WINDOW_MS) deleteOtpRequestWindows.delete(key);
    }
    for (const [token, session] of authSessions.entries()) {
        if (session.expiresAt <= now) authSessions.delete(token);
    }
}, 60000);

function initializeUserPersistence() {
    db.query(`
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(190) NOT NULL UNIQUE,
            password_salt VARCHAR(64) NOT NULL,
            password_hash VARCHAR(128) NOT NULL,
            role VARCHAR(30) NOT NULL DEFAULT 'STAFF_NURSE',
            active TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_users_role_active (role, active)
        ) ENGINE=InnoDB
    `, async createError => {
        if (createError) {
            console.error(' Lỗi tạo bảng users:', createError.message);
            return;
        }

        const managerEmail = normalizeAccountEmail(process.env.MANAGER_EMAIL);
        const managerPassword = String(process.env.MANAGER_PASSWORD || '');
        if (!managerEmail || !managerPassword) {
            console.warn(' Chưa cấu hình MANAGER_EMAIL/MANAGER_PASSWORD; không bootstrap tài khoản quản lý.');
            return;
        }
        if (managerPassword.length < 10) {
            console.error(' MANAGER_PASSWORD phải có ít nhất 10 ký tự.');
            return;
        }

        db.query('SELECT id FROM users WHERE email = ? LIMIT 1', [managerEmail], async (findError, rows) => {
            if (findError) {
                console.error(' Lỗi kiểm tra tài khoản quản lý:', findError.message);
                return;
            }
            if (rows.length) {
                db.query("UPDATE users SET role = 'MANAGER', active = 1 WHERE email = ?", [managerEmail]);
                return;
            }
            try {
                const credential = await hashPassword(managerPassword);
                db.query(`
                    INSERT INTO users (email, password_salt, password_hash, role, active)
                    VALUES (?, ?, ?, 'MANAGER', 1)
                `, [managerEmail, credential.salt, credential.hash], insertError => {
                    if (insertError) console.error(' Lỗi tạo tài khoản quản lý:', insertError.message);
                    else console.log(` Đã tạo tài khoản MANAGER: ${managerEmail}`);
                });
            } catch (hashError) {
                console.error(' Lỗi băm mật khẩu quản lý:', hashError.message);
            }
        });
    });
}

app.post('/api/patients/:id/register', requireApiAuth, (req, res) => {
    const id = String(req.params.id || '').trim();
    const registrationStartedAt = Date.now();
    res.once('finish', () => {
        console.log(` Đăng ký bệnh nhân ${id}: HTTP ${res.statusCode} sau ${Date.now() - registrationStartedAt}ms`);
    });
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
                latestRows.forEach(row => {
                    const patient = ensurePatientState(row.id);
                    const latestRecordedAt = row.time ? new Date(row.time).getTime() : 0;
                    const currentUpdatedAt = patient.lastUpdate ? new Date(patient.lastUpdate).getTime() : 0;
                    if (!currentUpdatedAt || latestRecordedAt >= currentUpdatedAt || patient.hasRealData !== true) {
                        applyStoredVitalsAndRecalculate(patient, row);
                    }
                });
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
                const latestRecordedAt = latestRows[0].time ? new Date(latestRows[0].time).getTime() : 0;
                const currentUpdatedAt = patient.lastUpdate ? new Date(patient.lastUpdate).getTime() : 0;
                // Khong de ban ghi vitals cu de len trang thai realtime moi hon
                // nhu RECOVERED/IDLE vua nhan qua BLE event hoac MQTT direct.
                if (!currentUpdatedAt || latestRecordedAt >= currentUpdatedAt) {
                    applyStoredVitalsAndRecalculate(patient, latestRows[0]);
                }
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

app.use((error, req, res, next) => {
    console.error(` Lỗi HTTP ${req.method} ${req.originalUrl}:`, error.stack || error.message);
    if (res.headersSent) return next(error);
    res.status(error.status || 500).json({
        error: 'Lỗi máy chủ nội bộ',
        details: error.message || 'Không rõ nguyên nhân',
    });
});

const server = app.listen(PORT, () =>
    console.log(` MedPulse Backend Service đang chạy trên Port: ${PORT}`)
);
wss = new WebSocket.Server({ server });
registerDashboardWebSocketHandlers();
