#pragma once

#include <Arduino.h>

// Nguồn chuẩn duy nhất cho giao thức trạm MedPulse.
// Telemetry v1 giữ tương thích với dashboard hiện tại.
// Command/event hai chiều dùng protocol v2.

static constexpr uint8_t MEDPULSE_TELEMETRY_PROTOCOL_VERSION = 1;
static constexpr uint8_t MEDPULSE_COMMAND_PROTOCOL_VERSION = 2;

static constexpr char MEDPULSE_BLE_NAME_PREFIX[] = "MEDPULSE-";
static constexpr uint8_t MEDPULSE_DEVICE_ID_MAX_LENGTH = 24;
static constexpr uint8_t MEDPULSE_MAX_TRACKED_BLE_DEVICES = 8;
static constexpr uint8_t MEDPULSE_MAX_GATT_CONNECTIONS = 3;
static constexpr uint8_t MEDPULSE_GATT_EVENT_FRAME_LENGTH = 12;
static constexpr uint8_t MEDPULSE_GATT_EVENT_QUEUE_LENGTH = 16;
static constexpr uint8_t MEDPULSE_GATT_EVENTS_PER_LOOP = 8;
static constexpr uint8_t MEDPULSE_GATT_COMMAND_FRAME_LENGTH = 16;
static constexpr uint8_t MEDPULSE_GATT_ACK_FRAME_LENGTH = 12;
static constexpr uint8_t MEDPULSE_GATT_ACK_QUEUE_LENGTH = 8;
static constexpr uint8_t MEDPULSE_DEVICE_COMMAND_QUEUE_LENGTH = 4;

static constexpr uint8_t MEDPULSE_FRAME_MAGIC_M = 0x4D;
static constexpr uint8_t MEDPULSE_FRAME_MAGIC_P = 0x50;
static constexpr uint8_t MEDPULSE_FRAME_VERSION = 0x01;
static constexpr uint8_t MEDPULSE_FRAME_TYPE_PRESENCE = 0x00;
static constexpr uint8_t MEDPULSE_FRAME_TYPE_VITALS = 0x10;
static constexpr uint8_t MEDPULSE_FRAME_TYPE_EVENT = 0x20;
static constexpr uint8_t MEDPULSE_FRAME_TYPE_COMMAND = 0x30;
static constexpr uint8_t MEDPULSE_FRAME_TYPE_COMMAND_ACK = 0x31;
static constexpr uint8_t MEDPULSE_GATT_EVENT_FLAG_FALL = 0x01;
static constexpr uint8_t MEDPULSE_GATT_EVENT_FLAG_CHARGING = 0x02;
static constexpr uint8_t MEDPULSE_GATT_EVENT_FLAG_BUZZER = 0x04;
static constexpr uint8_t MEDPULSE_GATT_EVENT_ALLOWED_FLAGS = 0x07;
static constexpr uint8_t MEDPULSE_BATTERY_UNKNOWN = 0xFF;

static constexpr char MEDPULSE_GATT_SERVICE_UUID[] = "7e400001-b5a3-f393-e0a9-e50e24dcca9e";
static constexpr char MEDPULSE_GATT_EVENT_UUID[] = "7e400002-b5a3-f393-e0a9-e50e24dcca9e";
static constexpr char MEDPULSE_GATT_COMMAND_UUID[] = "7e400003-b5a3-f393-e0a9-e50e24dcca9e";

static constexpr unsigned long MEDPULSE_BLE_LOCAL_LOST_MS = 15000UL;
static constexpr unsigned long MEDPULSE_BLE_SEEN_PUBLISH_INTERVAL_MS = 30000UL;
static constexpr unsigned long MEDPULSE_STATION_HEARTBEAT_INTERVAL_MS = 30000UL;
static constexpr uint32_t MEDPULSE_BLE_SCAN_CYCLE_SECONDS = 10;
static constexpr unsigned long MEDPULSE_GATT_RECONNECT_BACKOFF_MS = 5000UL;
static constexpr unsigned long MEDPULSE_GATT_COMMAND_ACK_TIMEOUT_MS = 15000UL;
static constexpr unsigned long MEDPULSE_STATION_COMMAND_MAX_QUEUE_MS = 60000UL;
static constexpr unsigned long MEDPULSE_GATT_STATUS_INTERVAL_MS = 30000UL;
static constexpr unsigned long MEDPULSE_WIFI_RECONNECT_INTERVAL_MS = 10000UL;
static constexpr unsigned long MEDPULSE_MQTT_RECONNECT_INTERVAL_MS = 5000UL;
static constexpr unsigned long MEDPULSE_SENSOR_RETRY_INTERVAL_MS = 30000UL;
static constexpr unsigned long MEDPULSE_VITALS_PUBLISH_RETRY_MS = 2000UL;
static constexpr unsigned long MEDPULSE_MEASUREMENT_SESSION_TIMEOUT_MS = 180000UL;

enum class MedPulseTransport : uint8_t {
  OFFLINE = 0,
  BLE_GATEWAY = 1,
  DIRECT_MQTT = 2,
};

enum class MedPulseEventType : uint8_t {
  HEARTBEAT = 0,
  PREALERT = 1,
  FALL_ALERT = 2,
  RECOVERED = 3,
  HARDWARE_FAULT = 4,
  HARDWARE_RECOVERED = 5,
};

enum class MedPulseCommandAction : uint8_t {
  BUZZER_ON = 1,
  BUZZER_OFF = 2,
  BUZZER_TEST = 3,
};

enum class MedPulseCommandAckStatus : uint8_t {
  EXECUTED = 1,
  FAILED = 2,
};

enum class MedPulseCommandAckError : uint8_t {
  NONE = 0,
  INVALID_FRAME = 1,
  UNSUPPORTED_ACTION = 2,
  EXECUTION_FAILED = 3,
  DUPLICATE_COMMAND = 4,
};

inline String medpulseStationTopic(const char* rootTopic, const char* stationId, const char* channel) {
  return String(rootTopic) + "/stations/" + stationId + "/" + channel;
}

inline String medpulseGatewayDeviceTopic(
    const char* rootTopic,
    const char* stationId,
    const char* deviceId,
    const char* channel) {
  return String(rootTopic) + "/stations/" + stationId + "/devices/" + deviceId + "/" + channel;
}

inline String medpulseDirectDeviceTopic(const char* rootTopic, const char* deviceId, const char* channel) {
  return String(rootTopic) + "/devices/" + deviceId + "/" + channel;
}
