CREATE DATABASE IF NOT EXISTS medpulse_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE medpulse_db;

-- Hồ sơ bệnh nhân. RFID chỉ xuất hiện trên dashboard sau khi có dòng trong bảng này.
CREATE TABLE IF NOT EXISTS patients (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT NULL,
    date_of_birth DATE NULL,
    gender VARCHAR(20) NULL,
    room VARCHAR(20) NULL,
    bed VARCHAR(20) NULL,
    condition_summary TEXT NULL,
    risk_score INT DEFAULT 0,
    current_status VARCHAR(50) DEFAULT 'IDLE',
    status_level VARCHAR(20) DEFAULT 'safe',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Railway database hiện có cần chạy một lần:
-- ALTER TABLE patients ADD COLUMN date_of_birth DATE NULL AFTER age;

-- Bảng lưu trữ lịch sử sinh hiệu và trạng thái cảnh báo của bệnh nhân
CREATE TABLE IF NOT EXISTS vitals_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id VARCHAR(50) NOT NULL,
    heart_rate INT NOT NULL,
    spo2 INT NOT NULL,
    temp DECIMAL(4,1) NOT NULL,
    battery INT DEFAULT 100,
    rssi INT DEFAULT -50,
    fall_status TINYINT(1) DEFAULT 0,  -- 0: Bình thường, 1: Bị té ngã
    is_safe INT DEFAULT 1,             -- Trạng thái an toàn y tế; không dùng để suy luận kết nối BLE
    device_status VARCHAR(20) NOT NULL DEFAULT 'IDLE',  -- Firmware status: IDLE/FALLING/IMPACT/MOTIONLESS/ALERT
    current_status VARCHAR(20) NOT NULL DEFAULT 'IDLE',  -- Current firmware status used by API/UI
    status_level VARCHAR(20) NOT NULL DEFAULT 'safe',    -- Legacy alert level: safe/warning/danger
    status_history JSON DEFAULT NULL,                   -- Chuỗi trạng thái gần nhất từ firmware
    risk_score INT NOT NULL,           -- Điểm số rủi ro tính toán từ Backend
    measurement_source VARCHAR(20) NOT NULL DEFAULT 'DIRECT_MQTT', -- DIRECT_MQTT hoặc BLE_GATEWAY
    source_device_id VARCHAR(24) NULL,
    source_station_id VARCHAR(50) NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX(patient_id),
    INDEX(recorded_at)
) ENGINE=InnoDB;

-- Backend tự kiểm tra và bổ sung ba cột source trên khi khởi động với DB cũ.

-- Trạng thái gateway/trạm ESP32. Heartbeat MQTT cập nhật mỗi 30 giây.
CREATE TABLE IF NOT EXISTS stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_status VARCHAR(20) NOT NULL DEFAULT 'OFFLINE',
    last_seen_at DATETIME(3) NULL,
    wifi_rssi INT NULL,
    ble_scanner TINYINT(1) NOT NULL DEFAULT 0,
    uptime_ms BIGINT UNSIGNED NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Thiết bị đeo BLE được trạm phát hiện và liên kết với bệnh nhân.
-- State machine: OFFLINE -> ONLINE -> GRACE -> ALERT; STATION_OFFLINE là trạng thái riêng.
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
) ENGINE=InnoDB;
