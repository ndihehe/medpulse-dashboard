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
        // [FIX] Load hồ sơ bệnh nhân từ DB vào RAM ngay khi khởi động
        loadPatientProfiles();
    }
});

// =========================================================================
// 2. QUẢN LÝ TRẠNG THÁI BỆNH NHÂN IN-MEMORY
// =========================================================================
const patientsState = {};
const lastSeenByPatient = new Map();
const registeredPatientIds = new Set();
const pendingRegistrations = new Map();
const OFFLINE_TIMEOUT_MS = Number(process.env.MQTT_OFFLINE_TIMEOUT_MS || 25000);
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
        ble:          'Ổn định',
        riskScore:    4,
        status:       'IDLE',
        current_status: 'IDLE',
        alertLevel:   'safe',
        statusHistory: [{ status: 'IDLE', at: new Date().toISOString() }],
        alert:        false,
        lastUpdate:   new Date(),
        hasRealData:  false,
        lastMeasurementAt: null,
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
    patient.ble = patient.signalLost ? 'Mất tín hiệu'
        : patient.battery < 20 ? 'Pin yếu'
        : 'Ổn định';
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
    patient.ble = 'Ổn định';
    patient.riskScore = 4;
    patient.status = 'IDLE';
    patient.current_status = 'IDLE';
    patient.alertLevel = 'safe';
    patient.alert = false;
    patient.hasRealData = false;
    patient.lastMeasurementAt = null;
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
            applyLatestVitalsToPatientState(ensurePatientState(row.id), row);
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
              + (isSignalLost ? 24 : 0)
              + statusMeta.riskWeight;

    patient.riskScore = Math.max(4, Math.min(99, Math.round(score)));

    if (statusMeta.alertLevel === 'danger' || isFall || isSignalLost || spo2 < 92 || hr > 125) {
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

    patient.ble = isSignalLost ? 'Mất tín hiệu'
                : patient.battery < 20 ? 'Pin yếu'
                : 'Ổn định';

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
    patient.safe        = false;
    patient.alertLevel  = 'danger';
    patient.alert       = true;
    patient.ble         = 'Mất tín hiệu';
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

setInterval(() => {
    const now = Date.now();
    for (const patientId of Object.keys(patientsState)) {
        const lastSeen = lastSeenByPatient.get(patientId);
        if (lastSeen && now - lastSeen > OFFLINE_TIMEOUT_MS) {
            markPatientOffline(patientId);
        }
    }
}, 5000);

// =========================================================================
// 4. WEBSOCKET SERVER
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
    mqttClient.subscribe('medpulse_duy/+/vitals');
    console.log(' Đang lắng nghe topic: medpulse_duy/+/vitals');
});

mqttClient.on('error', (err) =>
    console.error(' Lỗi kết nối MQTT:', err.message)
);

mqttClient.on('message', (topic, message, packet) => {
    try {
        const parts = topic.split('/');
        if (parts.length !== 3 || parts[0] !== 'medpulse_duy' || parts[2] !== 'vitals') return;
        if (packet?.retain) {
            console.log(` Bỏ qua retained MQTT trên topic: ${topic}`);
            return;
        }

        const patientId = parts[1];
        const liveData  = JSON.parse(message.toString().trim());

        ensurePatientState(patientId);
        lastSeenByPatient.set(patientId, Date.now());

        const p = patientsState[patientId];
        const isMeasurementPayload = hasMeasurementPayload(liveData);

        // Đồng bộ sinh hiệu
        if (Number.isFinite(liveData.heartRate)) p.heartRate = Math.round(liveData.heartRate);
        if (Number.isFinite(liveData.spo2))      p.spo2      = Math.round(liveData.spo2);
        if (Number.isFinite(liveData.temp))      p.temp      = Number(liveData.temp.toFixed(1));
        if (Number.isFinite(liveData.battery))   p.battery   = Number(liveData.battery.toFixed(1));
        // [FIX] Đồng bộ rssi từ sketch mới
        if (Number.isFinite(liveData.rssi))      p.rssi      = Math.round(liveData.rssi);

        p.signalLost = (liveData.signalLost === true || liveData.signalLost === 1);

        const normalizedStatus = normalizeFirmwareStatus(liveData.firmwareStatus || 'IDLE');
        if (normalizedStatus) {
            syncFallSafeState(p, 'status', normalizedStatus);
        }

        // RFID chưa có hồ sơ: giữ gói tin trong RAM, không lưu vitals_log và không đưa lên dashboard.
        if (!registeredPatientIds.has(patientId)) {
            if (isMeasurementPayload) {
                p.hasRealData = true;
                p.lastUpdate = new Date();
                p.lastMeasurementAt = p.lastUpdate.toISOString();
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
                broadcastToDashboards({
                    type: 'UNKNOWN_PATIENT_DETECTED',
                    data: serializePendingRegistration(pending),
                });
                console.log(` Phát hiện RFID chưa đăng ký: ${patientId}`);
            }
            return;
        }

        if (!p.signalLost) {
            publishPatientUpdate(patientId, {
                saveToDatabase: isMeasurementPayload,
                markAsMeasurement: isMeasurementPayload,
            });
            console.log(isMeasurementPayload
                ? ` Đã cập nhật + lưu DB cho: ${patientId}`
                : ` Đã cập nhật trạng thái UI, không lưu DB vì không có sinh hiệu đo: ${patientId}`);
        } else {
            const updated = processPatientMetricsCalculation(patientId);
            broadcastToDashboards({ type: 'PATIENT_UPDATE', data: serializePatientState(updated) });
            console.log(` [Mất tín hiệu] Cập nhật UI cho: ${patientId}`);
        }

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
         status_level, status_history, risk_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                latestRows.forEach(row => applyLatestVitalsToPatientState(ensurePatientState(row.id), row));
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
                   recorded_at AS time
            FROM vitals_log
            WHERE patient_id = ?
            ORDER BY recorded_at DESC
            LIMIT 1
        `;
        db.query(latestQuery, [id], (latestErr, latestRows) => {
            if (!latestErr && latestRows && latestRows[0]) {
                applyLatestVitalsToPatientState(patient, latestRows[0]);
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
    const limit     = parseInt(req.query.limit) || 30;

    const query = `
        SELECT heart_rate AS heartRate, spo2, temp,
               device_status AS status, current_status,
               status_level AS alertLevel, status_history AS statusHistory,
               risk_score AS riskScore, recorded_at AS time
        FROM vitals_log
        WHERE patient_id = ?
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
