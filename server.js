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

// =========================================================================
// 1. KẾT NỐI CƠ SỞ DỮ LIỆU MYSQL POOL (Tối ưu hóa đa kết nối)
// =========================================================================
const db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT || 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Kiểm tra kết nối DB khi khởi động hệ thống
db.getConnection((err, connection) => {
    if (err) {
        console.error('❌ Lỗi kết nối cơ sở dữ liệu MySQL:', err.message);
    } else {
        console.log('🔹 Cơ sở dữ liệu MySQL đã kết nối thành công!');
        connection.release();
    }
});


// =========================================================================
// 2. QUẢN LÝ TRẠNG THÁI BỆNH NHÂN IN-MEMORY CACHE (Khớp cấu trúc web.html)
// =========================================================================
// Khởi tạo trước danh sách 16 bệnh nhân định danh bằng RFID để tránh lỗi Undefined trên UI
const patientsState = {};
const totalPatientIds = Array.from({ length: 16 }, (_, i) => `RFID-${1001 + i}`);

totalPatientIds.forEach(id => {
    patientsState[id] = {
        id: id,
        heartRate: 75,
        spo2: 98,
        temp: 36.5,
        battery: 100,
        rssi: -55,
        safe: true,
        fall: false,
        signalLost: false,
        ble: 'Ổn định',
        riskScore: 4,
        status: 'IDLE',
        alertLevel: 'safe',
        statusHistory: [{ status: 'IDLE', at: new Date().toISOString() }],
        alert: false,
        lastUpdate: new Date()
    };
});

function normalizeFirmwareStatus(value) {
    const status = String(value || '').trim().toUpperCase();
    if (!status || !FIRMWARE_STATUSES.has(status)) {
        return null;
    }
    return status;
}


// =========================================================================
// 3. THUẬT TOÁN TÍNH TOÁN ĐIỂM RỦI RO & PHÂN CẤP TRẠNG THÁI (Business Logic)
// =========================================================================
function processPatientMetricsCalculation(patientId) {
    const patient = patientsState[patientId];
    
    const hr = patient.heartRate;
    const spo2 = patient.spo2;
    const temp = patient.temp;
    const isSafe = patient.safe;
    const isFall = patient.fall;
    const isSignalLost = patient.signalLost;

    // Áp dụng chính xác công thức tính toán trọng số rủi ro lâm sàng
    let score = (100 - spo2) * 1.2 
                + Math.abs(hr - 78) * 1.1 
                + Math.max(temp - 37.3, 0) * 18 
                + (isSafe ? 0 : 12) 
                + (isSignalLost ? 24 : 0) 
                + (isFall ? 40 : 0);
    
    // Giới hạn điểm số từ mức tối thiểu 4 đến tối đa 99 điểm
    patient.riskScore = Math.max(4, Math.min(99, Math.round(score)));

    const firmwareStatus = normalizeFirmwareStatus(patient.status) || 'IDLE';
    patient.status = firmwareStatus;

    if (firmwareStatus === 'ALERT') {
        patient.fall = true;
        patient.safe = false;
    } else if (firmwareStatus === 'IDLE') {
        patient.fall = false;
        patient.safe = true;
    } else {
        patient.safe = false;
    }

    // Gán mức nguy cơ phục vụ màu giao diện và cảnh báo phụ trợ
    if (firmwareStatus === 'ALERT' || isFall || isSignalLost || spo2 < 92 || hr > 125) {
        patient.alertLevel = 'danger';
    } else if (firmwareStatus !== 'IDLE' || !isSafe || spo2 < 94 || hr > 110 || hr < 55 || temp > 37.8) {
        patient.alertLevel = 'warning';
    } else {
        patient.alertLevel = 'safe';
    }

    // Gán cờ cảnh báo kích hoạt
    patient.alert = (firmwareStatus !== 'IDLE' || patient.alertLevel !== 'safe');
    patient.lastUpdate = new Date();

    patient.statusHistory = Array.isArray(patient.statusHistory) ? patient.statusHistory : [];
    const lastStatus = patient.statusHistory[patient.statusHistory.length - 1];
    if (!lastStatus || lastStatus.status !== firmwareStatus) {
        patient.statusHistory = [
            ...patient.statusHistory,
            { status: firmwareStatus, at: patient.lastUpdate.toISOString() }
        ].slice(-25);
    }

    // Đồng bộ hóa chuỗi trạng thái văn bản hiển thị cho kết nối BLE
    if (isSignalLost) {
        patient.ble = 'Mất tín hiệu';
    } else if (patient.battery < 20) {
        patient.ble = 'Pin yếu';
    } else {
        patient.ble = 'Ổn định';
    }

    return patient;
}


// =========================================================================
// 4. KHỞI TẠO WEBSOCKET SERVER (Đẩy dữ liệu chân rết thời gian thực về Web UI)
// =========================================================================
const server = app.listen(PORT, () => console.log(`🚀 MedPulse Backend Service đang chạy trên Port: ${PORT}`));
const wss = new WebSocket.Server({ server });

// Hàm Broadcast gửi gói tin tới tất cả các trình duyệt đang kết nối dashboard
function broadcastToDashboards(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

wss.on('connection', (ws) => {
    console.log('🔌 Thiết bị Giám sát (Dashboard Client) đã thiết lập kết nối WebSocket.');
    
    // Tính năng tối ưu: Khi Web load lại trang, Backend lập tức gửi toàn bộ trạng thái hiện tại của 16 bệnh nhân để đồng bộ ngay lập tức
    ws.send(JSON.stringify({
        type: 'INITIAL_SYNC',
        data: Object.values(patientsState)
    }));

    ws.on('close', () => console.log('❌ Một Dashboard Client đã đóng kết nối WebSocket.'));
});


// =========================================================================
// 5. KẾT NỐI HỆ THỐNG BROKER MQTT (Hứng tín hiệu từ các Chip ESP32 nhúng)
// =========================================================================
const mqttOptions = {
    clientId: `medpulse_backend_${Math.random().toString(16).substr(2, 8)}`,
    clean: true,
    connectTimeout: 4000,
    reconnectPeriod: 1000,
};

if (process.env.MQTT_USER) mqttOptions.username = process.env.MQTT_USER;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;

// Tự động thêm cấu hình đuôi đường dẫn nếu hệ thống chạy giao thức WebSocket (ws://)
const brokerUrl = process.env.MQTT_BROKER_URL || 'ws://broker.hivemq.com:8000/mqtt';
if (brokerUrl.startsWith('ws://') || brokerUrl.startsWith('wss://')) {
    mqttOptions.path = '/mqtt';
}

const mqttClient = mqtt.connect(brokerUrl, mqttOptions);

mqttClient.on('connect', () => {
    console.log('📡 Đã thiết lập liên kết thành công với HiveMQ Broker!');
    
    // Lắng nghe tất cả dữ liệu sinh hiệu phát ra từ mạng lưới phần cứng thông qua wildcard (+)
    // Cấu trúc Topic chuẩn: medpulse/health/{patient_id}/{metric}
    mqttClient.subscribe('medpulse/health/+/+');
});

mqttClient.on('error', (err) => {
    console.error('❌ Kết nối MQTT Broker thất bại:', err.message);
});

mqttClient.on('message', (topic, message) => {
    try {
        const parts = topic.split('/');
        const patientId = parts[2];  // Ví dụ: RFID-1001
        const metric = parts[3];     // Ví dụ: heart_rate, spo2, temp, fall, safe, battery, rssi
        const rawText = message.toString().trim();
        const rawValue = Number(rawText);

        // Kiểm tra xem ID bệnh nhân nhận được có nằm trong danh mục quản lý hay không
        if (!patientsState[patientId]) {
            console.warn(`⚠️ Nhận dữ liệu từ thiết bị lạ chưa định danh: ${patientId}`);
            return;
        }

        // Cập nhật giá trị thô tương ứng vào bộ nhớ đệm dựa vào metric định tuyến
        switch(metric) {
            case 'heart_rate':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].heartRate = Math.round(rawValue);
                break;
            case 'spo2':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].spo2 = Math.round(rawValue);
                break;
            case 'temp':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].temp = Number(rawValue.toFixed(1));
                break;
            case 'battery':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].battery = Math.round(rawValue);
                break;
            case 'rssi':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].rssi = Math.round(rawValue);
                break;
            case 'fall':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].fall = (rawValue === 1);
                break;
            case 'safe':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].safe = (rawValue === 1);
                break;
            case 'status':
                const normalizedStatus = normalizeFirmwareStatus(rawText);
                if (!normalizedStatus) {
                    console.log(`🔍 Trạng thái lạ: ${rawText}`);
                    return;
                }
                patientsState[patientId].status = normalizedStatus;
                patientsState[patientId].statusHistory = Array.isArray(patientsState[patientId].statusHistory) ? patientsState[patientId].statusHistory : [];
                patientsState[patientId].statusHistory = [
                    ...patientsState[patientId].statusHistory,
                    { status: normalizedStatus, at: new Date().toISOString() }
                ].slice(-25);
                if (normalizedStatus === 'ALERT') {
                    patientsState[patientId].fall = true;
                    patientsState[patientId].safe = false;
                } else if (normalizedStatus === 'IDLE') {
                    patientsState[patientId].fall = false;
                    patientsState[patientId].safe = true;
                } else {
                    patientsState[patientId].safe = false;
                }
                break;
            case 'signal_lost':
                if (!Number.isFinite(rawValue)) return;
                patientsState[patientId].signalLost = (rawValue === 1);
                break;
            default:
                console.log(`🔍 Metric lạ: ${metric}`);
                return;
        }

        // Thực hiện tính toán tập trung Điểm rủi ro & Xác định trạng thái màu sắc hệ thống
        const updatedPatientData = processPatientMetricsCalculation(patientId);

        // Đẩy ngay lập tức gói dữ liệu "sạch và trọn gói" này tới Dashboard qua WebSocket
        broadcastToDashboards({
            type: 'PATIENT_UPDATE',
            data: updatedPatientData
        });

        // Tối ưu hóa ghi log: Lưu trữ bản ghi lịch sử vào database phục vụ truy vấn báo cáo đồ thị
        saveVitalsToDatabase(updatedPatientData);

    } catch (error) {
        console.error('❌ Lỗi xử lý gói tin MQTT:', error.message);
    }
});


// =========================================================================
// 6. HÀM LƯU DỮ LIỆU ĐỒNG BỘ ASYNC VÀO MYSQL DB
// =========================================================================
function saveVitalsToDatabase(p) {
    const query = `
        INSERT INTO vitals_log 
        (patient_id, heart_rate, spo2, temp, battery, rssi, fall_status, is_safe, device_status, status_level, status_history, risk_score) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    const values = [
        p.id, p.heartRate, p.spo2, p.temp, p.battery, p.rssi, 
        p.fall ? 1 : 0, p.safe ? 1 : 0, p.status, p.alertLevel, JSON.stringify(p.statusHistory || []), p.riskScore
    ];

    db.query(query, values, (err, result) => {
        if (err) {
            console.error(`❌ Không thể lưu log cho bệnh nhân ${p.id}:`, err.message);
        }
    });
}

app.get('/api/patients', (req, res) => {
    res.json(Object.values(patientsState));
});

app.get('/api/patients/:id', (req, res) => {
    const patient = patientsState[req.params.id];
    if (!patient) {
        return res.status(404).json({ error: 'Không tìm thấy bệnh nhân' });
    }
    res.json(patient);
});


// =========================================================================
// 7. HTTP API BỔ SUNG (Dành cho các tác vụ lấy dữ liệu lịch sử)
// =========================================================================
// API lấy dữ liệu lịch sử của 1 bệnh nhân để vẽ đồ thị dài hạn (thay thế cho giả lập)
app.get('/api/patients/:id/history', (req, res) => {
    const patientId = req.params.id;
    const limit = parseInt(req.query.limit) || 30; // Mặc định lấy 30 điểm dữ liệu gần nhất để vẽ hình

    const query = `
        SELECT heart_rate as heartRate, spo2, temp, device_status as status, status_level as alertLevel, status_history as statusHistory, risk_score as riskScore, recorded_at as time 
        FROM vitals_log 
        WHERE patient_id = ? 
        ORDER BY recorded_at DESC 
        LIMIT ?
    `;

    db.query(query, [patientId, limit], (err, results) => {
        if (err) {
            return res.status(500).json({ error: 'Lỗi truy vấn database', details: err.message });
        }
        // Đảo ngược mảng để đồ thị chạy đúng thứ tự thời gian từ cũ đến mới
        res.json(results.reverse());
    });
});
// Serve web.html tại route /
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'web.html'));
});