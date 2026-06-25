#pragma once

#include <Arduino.h>

static constexpr uint8_t MEDPULSE_TELEMETRY_PROTOCOL_VERSION = 1;
static constexpr uint8_t MEDPULSE_COMMAND_PROTOCOL_VERSION = 2;

static constexpr char MEDPULSE_BLE_NAME_PREFIX[] = "MEDPULSE-";
static constexpr uint8_t MEDPULSE_DEVICE_ID_MAX_LENGTH = 24;
static constexpr uint8_t MEDPULSE_GATT_EVENT_FRAME_LENGTH = 12;
static constexpr uint8_t MEDPULSE_GATT_COMMAND_FRAME_LENGTH = 16;
static constexpr uint8_t MEDPULSE_GATT_ACK_FRAME_LENGTH = 12;

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

static constexpr unsigned long MEDPULSE_GATT_EVENT_HEARTBEAT_MS = 30000UL;
static constexpr unsigned long MEDPULSE_WIFI_RECONNECT_INTERVAL_MS = 10000UL;
static constexpr unsigned long MEDPULSE_WIFI_CONNECT_TIMEOUT_MS = 30000UL;
static constexpr unsigned long MEDPULSE_WIFI_SCAN_TIMEOUT_MS = 15000UL;
static constexpr unsigned long MEDPULSE_WIFI_DISCONNECT_SETTLE_MS = 5000UL;
static constexpr unsigned long MEDPULSE_MQTT_RECONNECT_INTERVAL_MS = 5000UL;
static constexpr unsigned long MEDPULSE_DIRECT_STATUS_INTERVAL_MS = 30000UL;

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

inline String medpulseDirectDeviceTopic(const char* rootTopic, const char* deviceId, const char* channel) {
  return String(rootTopic) + "/devices/" + deviceId + "/" + channel;
}

inline uint16_t medpulseCrc16Ccitt(const uint8_t* data, size_t length) {
  uint16_t crc = 0xFFFF;
  for (size_t index = 0; index < length; index++) {
    crc ^= static_cast<uint16_t>(data[index]) << 8;
    for (uint8_t bit = 0; bit < 8; bit++) {
      crc = (crc & 0x8000) ? static_cast<uint16_t>((crc << 1) ^ 0x1021)
                           : static_cast<uint16_t>(crc << 1);
    }
  }
  return crc;
}

inline uint32_t medpulseFnv1a(const char* value) {
  uint32_t hash = 2166136261UL;
  while (value && *value) {
    hash ^= static_cast<uint8_t>(*value++);
    hash *= 16777619UL;
  }
  return hash;
}

inline uint32_t medpulseReadUint32Le(const uint8_t* source) {
  return static_cast<uint32_t>(source[0])
      | (static_cast<uint32_t>(source[1]) << 8)
      | (static_cast<uint32_t>(source[2]) << 16)
      | (static_cast<uint32_t>(source[3]) << 24);
}

inline void medpulseWriteUint32Le(uint8_t* destination, uint32_t value) {
  destination[0] = value & 0xFF;
  destination[1] = (value >> 8) & 0xFF;
  destination[2] = (value >> 16) & 0xFF;
  destination[3] = (value >> 24) & 0xFF;
}
