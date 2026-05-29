CREATE DATABASE IF NOT EXISTS medpulse_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE medpulse_db;

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
    is_safe INT DEFAULT 1,             -- 1: Trong vùng an toàn, 0: Ra ngoài vùng (BLE mất)
    device_status VARCHAR(20) NOT NULL DEFAULT 'IDLE',  -- Firmware status: IDLE/FALLING/IMPACT/MOTIONLESS/ALERT
    status_level VARCHAR(20) NOT NULL DEFAULT 'safe',    -- Legacy alert level: safe/warning/danger
    status_history JSON DEFAULT NULL,                   -- Chuỗi trạng thái gần nhất từ firmware
    risk_score INT NOT NULL,           -- Điểm số rủi ro tính toán từ Backend
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX(patient_id),
    INDEX(recorded_at)
) ENGINE=InnoDB;