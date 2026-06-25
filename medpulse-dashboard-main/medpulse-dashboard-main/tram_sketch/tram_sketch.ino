#include <WiFi.h>
#include <PubSubClient.h>
#include <BLEDevice.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>
#include <BLEClient.h>
#include <ArduinoJson.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/task.h>
#include "station_config.h"
#include "medpulse_protocol.h"
#include <esp_arduino_version.h>

#include <Wire.h>
#include <Adafruit_MLX90614.h>

#include <SPI.h>
#include <MFRC522.h>

// ==================================================
// WIFI CONFIG
// ==================================================
// Lưu ý: ESP32 chỉ dùng WiFi 2.4GHz, không dùng được 5GHz.
const char* WIFI_SSID = MEDPULSE_WIFI_SSID;
const char* WIFI_PASS = MEDPULSE_WIFI_PASSWORD;

// Nếu chạy Wokwi thì dùng:
// const char* WIFI_SSID = "Wokwi-GUEST";
// const char* WIFI_PASS = "";

// ==================================================
// MQTT CONFIG
// ==================================================
const char* MQTT_HOST = MEDPULSE_MQTT_HOST;
const int   MQTT_PORT = MEDPULSE_MQTT_PORT;

// Backend đang nghe: medpulse_duy/+/vitals
const char* MQTT_ROOT_TOPIC = MEDPULSE_MQTT_ROOT_TOPIC;

// ==================================================
// BLE GATEWAY PROTOCOL
// ==================================================
// Thiết bị đeo tương lai phải phát BLE Advertising với tên:
//   MEDPULSE-<DEVICE_ID>  (ví dụ MEDPULSE-MINI01)
// Manufacturer data hiện diện (5 byte): 4D 50 01 00 <battery 0..100>
// Manufacturer data sinh hiệu (12 byte):
//   4D 50 01 10 <seq> <battery> <heartRate> <spo2> <tempLE16 x10> <flags> 00
// flags: bit0=fall, bit1=charging, bit2=measurementValid.
// Trạm chỉ báo SEEN/LOST. Backend mới là nơi chờ 180 giây trước khi cảnh báo.
const char* STATION_ID = MEDPULSE_STATION_ID;
const char* BLE_DEVICE_NAME_PREFIX = MEDPULSE_BLE_NAME_PREFIX;
const unsigned long BLE_LOCAL_LOST_MS = MEDPULSE_BLE_LOCAL_LOST_MS;
const unsigned long BLE_SEEN_PUBLISH_INTERVAL_MS = MEDPULSE_BLE_SEEN_PUBLISH_INTERVAL_MS;
const unsigned long STATION_HEARTBEAT_INTERVAL_MS = MEDPULSE_STATION_HEARTBEAT_INTERVAL_MS;
const uint32_t BLE_SCAN_CYCLE_SECONDS = MEDPULSE_BLE_SCAN_CYCLE_SECONDS;
const uint8_t MAX_TRACKED_BLE_DEVICES = MEDPULSE_MAX_TRACKED_BLE_DEVICES;
const uint8_t BLE_DEVICE_ID_MAX_LENGTH = MEDPULSE_DEVICE_ID_MAX_LENGTH;

WiFiClient espClient;
PubSubClient mqtt(espClient);

class MedPulseGattClientCallbacks;

struct TrackedBleDevice {
  bool used;
  bool online;
  bool pendingSeen;
  bool pendingLost;
  char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1];
  char address[18];
  int rssi;
  int battery;
  unsigned long lastSeenMs;
  unsigned long lastPublishedMs;
  bool pendingVitals;
  bool hasVitalsSequence;
  uint8_t vitalsSequence;
  uint8_t heartRate;
  uint8_t spo2;
  int16_t tempX10;
  bool fallDetected;
  bool charging;
  uint8_t gattState;
  bool gattConnectQueued;
  unsigned long lastGattAttemptMs;
  unsigned long gattConnectedAtMs;
  BLEClient* gattClient;
  MedPulseGattClientCallbacks* gattClientCallbacks;
  BLERemoteCharacteristic* gattEventCharacteristic;
  BLERemoteCharacteristic* gattCommandCharacteristic;
  bool hasGattEventSequence;
  uint8_t lastGattEventSequence;
  bool pendingGattTransportUpdate;
  bool hasPublishedGattTransport;
  bool lastPublishedGattReady;
  unsigned long lastGattTransportPublishedMs;
};

enum GattConnectionState : uint8_t {
  GATT_DISCONNECTED = 0,
  GATT_QUEUED = 1,
  GATT_CONNECTING = 2,
  GATT_READY = 3,
  GATT_BACKOFF = 4,
};

struct PendingDeviceCommand {
  char commandId[65];
  char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1];
  char action[20];
  uint32_t durationMs;
  unsigned long receivedAtMs;
};

struct InflightDeviceCommand {
  bool active;
  bool ackReceived;
  bool ackTimedOut;
  uint32_t token;
  unsigned long sentAtMs;
  uint8_t ackStatus;
  uint8_t ackErrorCode;
  PendingDeviceCommand command;
};

struct GattCommandAckMessage {
  char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1];
  uint32_t token;
  uint8_t status;
  uint8_t errorCode;
};

InflightDeviceCommand inflightDeviceCommand = {};

struct GattEventMessage {
  char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1];
  uint8_t sequence;
  uint8_t eventType;
  int16_t battery;
  uint8_t flags;
  uint32_t deviceUptimeSeconds;
};

struct BleAdvertisementFrame {
  int battery;
  bool hasVitals;
  uint8_t sequence;
  uint8_t heartRate;
  uint8_t spo2;
  int16_t tempX10;
  bool fallDetected;
  bool charging;
};

TrackedBleDevice trackedBleDevices[MAX_TRACKED_BLE_DEVICES] = {};
portMUX_TYPE bleStateMux = portMUX_INITIALIZER_UNLOCKED;
BLEScan* bleScan = nullptr;
volatile bool bleScanRestartRequested = false;
unsigned long lastStationHeartbeatMs = 0;
QueueHandle_t gattConnectQueue = nullptr;
QueueHandle_t gattEventQueue = nullptr;
QueueHandle_t gattAckQueue = nullptr;
QueueHandle_t deviceCommandQueue = nullptr;
QueueHandle_t rejectedCommandQueue = nullptr;
TaskHandle_t gattWorkerTaskHandle = nullptr;
volatile bool gattOperationInProgress = false;
volatile uint32_t malformedGattEventCount = 0;
volatile uint32_t droppedGattEventCount = 0;
unsigned long lastWiFiReconnectAttemptMs = 0;
unsigned long lastMqttReconnectAttemptMs = 0;
unsigned long lastSensorRecoveryAttemptMs = 0;
unsigned long lastVitalsPublishAttemptMs = 0;
bool wifiWasConnected = false;
bool mqttWasConnected = false;
bool max30102Ready = false;
bool mlx90614Ready = false;
bool rc522Ready = false;
uint8_t mlxInvalidSampleCount = 0;

// ==================================================
// I2C: MAX30102 + MLX90614
// ==================================================
#define SDA_PIN 21
#define SCL_PIN 22

Adafruit_MLX90614 mlx;

// ==================================================
// RC522 SPI PIN - theo dây bạn đang nối
// ==================================================
#define RC522_SS_PIN   26
#define RC522_RST_PIN  27
#define RC522_SCK_PIN  32
#define RC522_MISO_PIN 25
#define RC522_MOSI_PIN 33

MFRC522 rfid(RC522_SS_PIN, RC522_RST_PIN);

// ==================================================
// MAX30102 REGISTERS
// ==================================================
#define MAX30102_ADDR   0x57
#define REG_FIFO_WR_PTR 0x04
#define REG_OVR_COUNTER 0x05
#define REG_FIFO_RD_PTR 0x06
#define REG_FIFO_DATA   0x07
#define REG_FIFO_CONFIG 0x08
#define REG_MODE_CONFIG 0x09
#define REG_SPO2_CONFIG 0x0A
#define REG_LED1_PA     0x0C   // RED
#define REG_LED2_PA     0x0D   // IR
#define REG_PART_ID     0xFF

// ==================================================
// MEASURE STATE
// ==================================================
enum MeasureState {
  WAIT_RFID,
  MEASURING,
  PUBLISH_DONE
};

MeasureState measureState = WAIT_RFID;

String currentUID = "";
String currentPatientId = "";

unsigned long measureStartMs = 0;
unsigned long measurementSessionStartedAtMs = 0;
unsigned long lastPublishMs = 0;
unsigned long lastMLXMs = 0;
unsigned long lastPrintMs = 0;

float lastTempC = -999.0;
long lastRed = 0;
long lastIr = 0;

// ==================================================
// MLX90614 5-SAMPLE STABLE FILTER
// ==================================================
// MLX90614 thường đo bề mặt da thấp hơn thực tế, nên cộng bù nhẹ 0.6°C.
const float TEMP_OFFSET_C = 0.6f;

// Lấy đúng 5 lần đo cho mỗi lần đưa tay.
const int TEMP_SAMPLE_SIZE = 5;

// Mỗi mẫu cách nhau 700ms.
const unsigned long TEMP_SAMPLE_INTERVAL_MS = 700;

// Nếu 5 mẫu dao động quá 0.7°C thì đo lại.
const float TEMP_MAX_RANGE_ACCEPT = 0.7f;

// Ngưỡng nhiệt độ hợp lệ sau khi đã cộng bù.
const float TEMP_MIN_VALID = 35.5f;
const float TEMP_MAX_VALID = 42.5f;

float tempSamples[TEMP_SAMPLE_SIZE];
int tempSampleCount = 0;

bool tempStable = false;
float stableTempC = -999.0;

// ==================================================
// HEART RATE VARIABLES
// ==================================================
float dcFilter = 0;
float alpha = 0.99f;
long acValue = 0;

long peakThresh = 80;
bool isPeak = false;
long lastPeakMs = 0;
long peakCooldown = 300;

bool filterReady = false;
bool fingerPresent = false;
unsigned long fingerSince = 0;

const byte RATE_SIZE = 6;
float rates[RATE_SIZE];
byte rateSpot = 0;
float beatAvg = 0;
bool bufferFull = false;

// Uoc luong SpO2 tu hai kenh RED/IR theo cua so mau.
// Thuat toan nay phuc vu tich hop he thong, khong thay the thiet bi y te hieu chuan.
const uint16_t SPO2_WINDOW_SIZE = 100;
const uint8_t SPO2_REQUIRED_WINDOWS = 3;
uint16_t spo2SampleCount = 0;
uint8_t spo2ValidWindowCount = 0;
double redSum = 0;
double irSum = 0;
double redSquareSum = 0;
double irSquareSum = 0;
float stableSpo2 = 0;
bool spo2Stable = false;

// ==================================================
// MAX30102 LOW LEVEL I/O
// ==================================================
void writeReg(uint8_t reg, uint8_t val) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission();
}

uint8_t readReg(uint8_t reg) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.endTransmission(false);

  Wire.requestFrom(MAX30102_ADDR, 1);

  if (Wire.available()) {
    return Wire.read();
  }

  return 0;
}

void readBytes(uint8_t reg, uint8_t *buf, uint8_t len) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.endTransmission(false);

  Wire.requestFrom(MAX30102_ADDR, (int)len);

  uint8_t i = 0;

  while (Wire.available() && i < len) {
    buf[i++] = Wire.read();
  }

  while (i < len) {
    buf[i++] = 0;
  }
}

// ==================================================
// MAX30102 INIT
// ==================================================
bool max30102_init() {
  uint8_t id = readReg(REG_PART_ID);

  Serial.printf("[MAX30102] Part ID: 0x%02X\n", id);

  if (id != 0x15) {
    return false;
  }

  writeReg(REG_MODE_CONFIG, 0x40);
  delay(100);

  unsigned long startWait = millis();

  while (readReg(REG_MODE_CONFIG) & 0x40) {
    delay(10);

    if (millis() - startWait > 1000) {
      Serial.println("[MAX30102] Reset timeout!");
      return false;
    }
  }

  writeReg(REG_FIFO_WR_PTR, 0x00);
  writeReg(REG_OVR_COUNTER, 0x00);
  writeReg(REG_FIFO_RD_PTR, 0x00);

  writeReg(REG_FIFO_CONFIG, 0x5F);
  writeReg(REG_SPO2_CONFIG, 0x27);

  writeReg(REG_LED1_PA, 0x24);
  writeReg(REG_LED2_PA, 0x24);

  writeReg(REG_MODE_CONFIG, 0x03);

  delay(500);

  Serial.printf("[MAX30102] MODE=0x%02X FIFO=0x%02X SPO2=0x%02X LED1=0x%02X LED2=0x%02X\n",
                readReg(REG_MODE_CONFIG),
                readReg(REG_FIFO_CONFIG),
                readReg(REG_SPO2_CONFIG),
                readReg(REG_LED1_PA),
                readReg(REG_LED2_PA));

  return true;
}

// ==================================================
// MAX30102 READ RED + IR
// ==================================================
bool max30102_readSample(long &red, long &ir) {
  uint8_t wr = readReg(REG_FIFO_WR_PTR);
  uint8_t rd = readReg(REG_FIFO_RD_PTR);

  if (wr == rd) {
    return false;
  }

  uint8_t buf[6];
  readBytes(REG_FIFO_DATA, buf, 6);

  red = ((long)(buf[0] & 0x03) << 16) | ((long)buf[1] << 8) | buf[2];
  ir  = ((long)(buf[3] & 0x03) << 16) | ((long)buf[4] << 8) | buf[5];

  return true;
}

// ==================================================
// RESET FILTERS
// ==================================================
void resetTempFilter() {
  tempSampleCount = 0;
  tempStable = false;
  stableTempC = -999.0;
  lastTempC = -999.0;

  for (int i = 0; i < TEMP_SAMPLE_SIZE; i++) {
    tempSamples[i] = -999.0;
  }
}

void resetHR() {
  dcFilter = 0;
  acValue = 0;

  filterReady = false;
  fingerPresent = false;
  fingerSince = 0;

  beatAvg = 0;
  rateSpot = 0;
  bufferFull = false;

  spo2SampleCount = 0;
  spo2ValidWindowCount = 0;
  redSum = 0;
  irSum = 0;
  redSquareSum = 0;
  irSquareSum = 0;
  stableSpo2 = 0;
  spo2Stable = false;

  isPeak = false;
  lastPeakMs = 0;

  lastRed = 0;
  lastIr = 0;

  for (byte i = 0; i < RATE_SIZE; i++) {
    rates[i] = 0;
  }
}

void processSpo2(long red, long ir) {
  if (!filterReady || red <= 0 || ir <= 0) return;

  redSum += red;
  irSum += ir;
  redSquareSum += (double)red * red;
  irSquareSum += (double)ir * ir;
  spo2SampleCount++;

  if (spo2SampleCount < SPO2_WINDOW_SIZE) return;

  const double redDc = redSum / spo2SampleCount;
  const double irDc = irSum / spo2SampleCount;
  double redVariance = redSquareSum / spo2SampleCount - redDc * redDc;
  double irVariance = irSquareSum / spo2SampleCount - irDc * irDc;
  if (redVariance < 0) redVariance = 0;
  if (irVariance < 0) irVariance = 0;
  const double redAc = sqrt(redVariance);
  const double irAc = sqrt(irVariance);

  bool validWindow = redDc > 1000.0 && irDc > 1000.0 && redAc > 1.0 && irAc > 1.0;
  float estimatedSpo2 = 0;
  if (validWindow) {
    const double ratio = (redAc / redDc) / (irAc / irDc);
    validWindow = ratio >= 0.2 && ratio <= 1.8;
    if (validWindow) {
      estimatedSpo2 = (float)(-45.060 * ratio * ratio + 30.354 * ratio + 94.845);
      estimatedSpo2 = constrain(estimatedSpo2, 70.0f, 100.0f);
    }
  }

  if (validWindow) {
    stableSpo2 = spo2ValidWindowCount == 0
      ? estimatedSpo2
      : stableSpo2 * 0.65f + estimatedSpo2 * 0.35f;
    if (spo2ValidWindowCount < SPO2_REQUIRED_WINDOWS) spo2ValidWindowCount++;
    spo2Stable = spo2ValidWindowCount >= SPO2_REQUIRED_WINDOWS;
  } else {
    spo2ValidWindowCount = 0;
    stableSpo2 = 0;
    spo2Stable = false;
  }

  spo2SampleCount = 0;
  redSum = 0;
  irSum = 0;
  redSquareSum = 0;
  irSquareSum = 0;
}

// ==================================================
// HEART RATE PROCESS
// ==================================================
bool processBeat(long rawIR) {
  if (rawIR < 0) return false;

  dcFilter = alpha * dcFilter + (1.0f - alpha) * rawIR;
  acValue = rawIR - (long)dcFilter;

  if (!filterReady) return false;

  long now = millis();
  bool detected = false;

  if (acValue > peakThresh && !isPeak && (now - lastPeakMs) > peakCooldown) {
    isPeak = true;

    long delta = now - lastPeakMs;
    lastPeakMs = now;

    float bpm = 60000.0f / delta;

    if (bpm > 40 && bpm < 180) {
      rates[rateSpot++] = bpm;
      rateSpot %= RATE_SIZE;

      if (rateSpot == 0) {
        bufferFull = true;
      }

      byte count = bufferFull ? RATE_SIZE : rateSpot;

      beatAvg = 0;

      for (byte i = 0; i < count; i++) {
        beatAvg += rates[i];
      }

      beatAvg /= count;
      detected = true;
    }
  }

  if (acValue < -50) {
    isPeak = false;
  }

  return detected;
}

// ==================================================
// WIFI + MQTT
// ==================================================
void connectWiFi() {
  Serial.print("[WiFi] Bat dau ket noi nen den ");
  Serial.println(WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  lastWiFiReconnectAttemptMs = millis();
}

void ensureWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    if (!wifiWasConnected) {
      wifiWasConnected = true;
      Serial.print("[WiFi] Connected. IP = ");
      Serial.println(WiFi.localIP());
    }
    return;
  }
  if (wifiWasConnected) {
    wifiWasConnected = false;
    Serial.println("[WiFi] Mat ket noi; gateway BLE van tiep tuc chay.");
  }
  const unsigned long now = millis();
  if (now - lastWiFiReconnectAttemptMs < MEDPULSE_WIFI_RECONNECT_INTERVAL_MS) return;
  lastWiFiReconnectAttemptMs = now;
  Serial.println("[WiFi] Thu ket noi lai...");
  // SSID/password da duoc nap boi connectWiFi(). Goi begin() lap lai khi
  // driver van dang CONNECTING se gay "sta is connecting, cannot set config".
  WiFi.reconnect();
}

void ensureMQTT() {
  if (mqtt.connected()) {
    mqttWasConnected = true;
    return;
  }
  if (mqttWasConnected) {
    mqttWasConnected = false;
    Serial.println("[MQTT] Mat ket noi; se retry theo backoff.");
  }
  if (WiFi.status() != WL_CONNECTED) return;
  const unsigned long now = millis();
  if (now - lastMqttReconnectAttemptMs < MEDPULSE_MQTT_RECONNECT_INTERVAL_MS) return;
  lastMqttReconnectAttemptMs = now;
  Serial.print("[MQTT] Connecting to ");
  Serial.println(MQTT_HOST);
  String clientId = "ESP32_KIOSK_" + String((uint32_t)ESP.getEfuseMac(), HEX);
  if (!mqtt.connect(clientId.c_str())) {
    Serial.print("[MQTT] Failed, state=");
    Serial.println(mqtt.state());
    return;
  }
  mqttWasConnected = true;
  Serial.println("[MQTT] Connected!");
  String commandTopic = medpulseGatewayDeviceTopic(MQTT_ROOT_TOPIC, STATION_ID, "+", "commands");
  const bool commandSubscribed = mqtt.subscribe(commandTopic.c_str(), 1);
  Serial.print("[MQTT] Subscribed commands ");
  Serial.print(commandSubscribed ? "OK: " : "FAIL: ");
  Serial.println(commandTopic);

  // Sau reconnect, phat lai snapshot de backend khong phai cho den heartbeat ke tiep.
  lastStationHeartbeatMs = millis() - STATION_HEARTBEAT_INTERVAL_MS;
  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    TrackedBleDevice& device = trackedBleDevices[index];
    if (!device.used) continue;
    if (device.online) device.pendingSeen = true;
    device.pendingGattTransportUpdate = true;
    device.hasPublishedGattTransport = false;
  }
  portEXIT_CRITICAL(&bleStateMux);
}

void handleMqttMessage(char* topicChars, byte* payloadBytes, unsigned int length) {
  String topic(topicChars);
  String expectedPrefix = String(MQTT_ROOT_TOPIC) + "/stations/" + STATION_ID + "/devices/";
  if (!topic.startsWith(expectedPrefix) || !topic.endsWith("/commands") || length == 0 || length >= 512) return;

  Serial.print("[COMMAND MQTT] Topic=");
  Serial.print(topic);
  Serial.print(" bytes=");
  Serial.println(length);

  String deviceId = topic.substring(expectedPrefix.length(), topic.length() - strlen("/commands"));
  if (!isValidBleDeviceId(deviceId)) {
    Serial.println("[COMMAND MQTT] DeviceId khong hop le.");
    return;
  }

  JsonDocument document;
  DeserializationError error = deserializeJson(document, payloadBytes, length);
  if (error) {
    Serial.print("[COMMAND MQTT] JSON loi: ");
    Serial.println(error.c_str());
    return;
  }
  const char* payloadDeviceId = document["deviceId"] | "";
  if (document["protocolVersion"] != MEDPULSE_COMMAND_PROTOCOL_VERSION
      || strcmp(payloadDeviceId, deviceId.c_str()) != 0) {
    Serial.println("[COMMAND MQTT] protocolVersion/deviceId khong khop.");
    return;
  }
  const char* commandId = document["commandId"] | "";
  const char* action = document["action"] | "";
  uint32_t durationMs = document["durationMs"] | 0;
  if (strlen(commandId) == 0 || strlen(commandId) > 64
      || (strcmp(action, "BUZZER_ON") != 0 && strcmp(action, "BUZZER_OFF") != 0 && strcmp(action, "BUZZER_TEST") != 0)
      || durationMs > 300000) {
    Serial.println("[COMMAND MQTT] Noi dung lenh khong hop le.");
    return;
  }

  if (!deviceCommandQueue) {
    Serial.println("[COMMAND] Queue chua san sang.");
    return;
  }
  PendingDeviceCommand command = {};
  strncpy(command.commandId, commandId, sizeof(command.commandId) - 1);
  strncpy(command.deviceId, deviceId.c_str(), sizeof(command.deviceId) - 1);
  strncpy(command.action, action, sizeof(command.action) - 1);
  command.durationMs = durationMs;
  command.receivedAtMs = millis();
  if (xQueueSend(deviceCommandQueue, &command, 0) != pdTRUE) {
    Serial.println("[COMMAND] Queue day, dua lenh vao hang doi tu choi.");
    if (!rejectedCommandQueue || xQueueSend(rejectedCommandQueue, &command, 0) != pdTRUE) {
      Serial.println("[COMMAND] Hang doi tu choi cung da day.");
    }
  } else {
    Serial.print("[COMMAND MQTT] Da nhan lenh ");
    Serial.print(command.commandId);
    Serial.print(" action=");
    Serial.print(command.action);
    Serial.print(" device=");
    Serial.println(command.deviceId);
  }
}

// ==================================================
// BLE SCANNER + MQTT DEVICE PRESENCE
// ==================================================
bool isValidBleDeviceId(const String& deviceId) {
  if (deviceId.length() == 0 || deviceId.length() > BLE_DEVICE_ID_MAX_LENGTH) {
    return false;
  }

  for (size_t i = 0; i < deviceId.length(); i++) {
    char c = deviceId.charAt(i);
    bool allowed = (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '-'
                || c == '_';
    if (!allowed) return false;
  }
  return true;
}

bool decodeMedPulseFrame(BLEAdvertisedDevice& advertisedDevice, BleAdvertisementFrame& frame) {
  frame = {};
  frame.battery = -1;
  if (!advertisedDevice.haveManufacturerData()) return false;

  uint8_t raw[12] = {};
  size_t dataLength = 0;

#if ESP_ARDUINO_VERSION_MAJOR >= 3
  String data = advertisedDevice.getManufacturerData();
  dataLength = data.length();
  for (size_t i = 0; i < dataLength && i < sizeof(raw); i++) raw[i] = static_cast<uint8_t>(data.charAt(i));
#else
  std::string data = advertisedDevice.getManufacturerData();
  dataLength = data.size();
  for (size_t i = 0; i < dataLength && i < sizeof(raw); i++) raw[i] = static_cast<uint8_t>(data[i]);
#endif

  if (dataLength < 4 || raw[0] != MEDPULSE_FRAME_MAGIC_M || raw[1] != MEDPULSE_FRAME_MAGIC_P
      || raw[2] != MEDPULSE_FRAME_VERSION) return false;

  // Tương thích frame pin 4 byte cũ: 4D 50 01 <battery>.
  if (dataLength == 4) {
    if (raw[3] > 100) return false;
    frame.battery = raw[3];
    return true;
  }

  const uint8_t frameType = raw[3];
  if (frameType == MEDPULSE_FRAME_TYPE_PRESENCE) {
    if (dataLength < 5 || (raw[4] != MEDPULSE_BATTERY_UNKNOWN && raw[4] > 100)) return false;
    frame.battery = raw[4] == MEDPULSE_BATTERY_UNKNOWN ? -1 : raw[4];
    return true;
  }

  if (frameType != MEDPULSE_FRAME_TYPE_VITALS || dataLength < 12) return false;
  const int16_t tempX10 = static_cast<int16_t>(raw[8] | (static_cast<uint16_t>(raw[9]) << 8));
  const bool measurementValid = (raw[10] & 0x04) != 0;
  if (raw[5] > 100 || raw[6] < 25 || raw[6] > 240 || raw[7] < 50 || raw[7] > 100
      || tempX10 < 250 || tempX10 > 500 || !measurementValid) return false;

  frame.battery = raw[5];
  frame.hasVitals = true;
  frame.sequence = raw[4];
  frame.heartRate = raw[6];
  frame.spo2 = raw[7];
  frame.tempX10 = tempX10;
  frame.fallDetected = (raw[10] & 0x01) != 0;
  frame.charging = (raw[10] & 0x02) != 0;
  return true;
}

void recordBleAdvertisement(const String& deviceId, const String& address, int rssi, const BleAdvertisementFrame& frame) {
  const unsigned long now = millis();
  int targetIndex = -1;
  int freeIndex = -1;

  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t i = 0; i < MAX_TRACKED_BLE_DEVICES; i++) {
    if (trackedBleDevices[i].used) {
      if (strncmp(trackedBleDevices[i].deviceId, deviceId.c_str(), BLE_DEVICE_ID_MAX_LENGTH) == 0) {
        targetIndex = i;
        break;
      }
    } else if (freeIndex < 0) {
      freeIndex = i;
    }
  }

  if (targetIndex < 0) targetIndex = freeIndex;
  if (targetIndex >= 0) {
    TrackedBleDevice& device = trackedBleDevices[targetIndex];
    const bool firstSeenOrRecovered = !device.used || !device.online;

    if (!device.used) {
      memset(&device, 0, sizeof(device));
      device.used = true;
      device.battery = -1;
      strncpy(device.deviceId, deviceId.c_str(), BLE_DEVICE_ID_MAX_LENGTH);
      device.deviceId[BLE_DEVICE_ID_MAX_LENGTH] = '\0';
    }

    device.online = true;
    strncpy(device.address, address.c_str(), sizeof(device.address) - 1);
    device.address[sizeof(device.address) - 1] = '\0';
    device.rssi = rssi;
    if (frame.battery >= 0) device.battery = frame.battery;
    device.lastSeenMs = now;
    device.pendingLost = false;

    if (frame.hasVitals && (!device.hasVitalsSequence || device.vitalsSequence != frame.sequence)) {
      device.hasVitalsSequence = true;
      device.vitalsSequence = frame.sequence;
      device.heartRate = frame.heartRate;
      device.spo2 = frame.spo2;
      device.tempX10 = frame.tempX10;
      device.fallDetected = frame.fallDetected;
      device.charging = frame.charging;
      device.pendingVitals = true;
    }

    if (firstSeenOrRecovered || now - device.lastPublishedMs >= BLE_SEEN_PUBLISH_INTERVAL_MS) {
      device.pendingSeen = true;
    }
  }
  portEXIT_CRITICAL(&bleStateMux);

  if (targetIndex < 0) {
    Serial.println("[BLE] Bang theo doi da day, bo qua thiet bi moi.");
  }
}

class MedPulseAdvertisedDeviceCallbacks : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice advertisedDevice) override {
    if (!advertisedDevice.haveName()) return;

#if ESP_ARDUINO_VERSION_MAJOR >= 3
    String advertisedName = advertisedDevice.getName();
#else
    String advertisedName = advertisedDevice.getName().c_str();
#endif
    if (!advertisedName.startsWith(BLE_DEVICE_NAME_PREFIX)) return;

    String deviceId = advertisedName.substring(strlen(BLE_DEVICE_NAME_PREFIX));
    deviceId.trim();
    if (!isValidBleDeviceId(deviceId)) return;

    BleAdvertisementFrame frame;
    if (!decodeMedPulseFrame(advertisedDevice, frame)) return;
    String address = String(advertisedDevice.getAddress().toString().c_str());
    recordBleAdvertisement(deviceId, address, advertisedDevice.getRSSI(), frame);
  }
};

MedPulseAdvertisedDeviceCallbacks bleAdvertisedDeviceCallbacks;

uint16_t medpulseCrc16Ccitt(const uint8_t* data, size_t length) {
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

uint32_t commandTokenForId(const char* commandId) {
  uint32_t hash = 2166136261UL;
  for (const uint8_t* cursor = reinterpret_cast<const uint8_t*>(commandId); *cursor; cursor++) {
    hash ^= *cursor;
    hash *= 16777619UL;
  }
  return hash;
}

void writeUint32Le(uint8_t* destination, uint32_t value) {
  destination[0] = value & 0xFF;
  destination[1] = (value >> 8) & 0xFF;
  destination[2] = (value >> 16) & 0xFF;
  destination[3] = (value >> 24) & 0xFF;
}

uint32_t readUint32Le(const uint8_t* source) {
  return static_cast<uint32_t>(source[0])
      | (static_cast<uint32_t>(source[1]) << 8)
      | (static_cast<uint32_t>(source[2]) << 16)
      | (static_cast<uint32_t>(source[3]) << 24);
}

bool decodeGattEventFrame(const uint8_t* data, size_t length, GattEventMessage& event) {
  if (!data || length != MEDPULSE_GATT_EVENT_FRAME_LENGTH) return false;
  if (data[0] != MEDPULSE_FRAME_MAGIC_M || data[1] != MEDPULSE_FRAME_MAGIC_P
      || data[2] != MEDPULSE_COMMAND_PROTOCOL_VERSION
      || data[3] != MEDPULSE_FRAME_TYPE_EVENT) return false;
  if (data[5] > static_cast<uint8_t>(MedPulseEventType::HARDWARE_RECOVERED)) return false;
  if (data[6] != MEDPULSE_BATTERY_UNKNOWN && data[6] > 100) return false;
  if ((data[7] & ~MEDPULSE_GATT_EVENT_ALLOWED_FLAGS) != 0) return false;

  event.sequence = data[4];
  event.eventType = data[5];
  event.battery = data[6] == MEDPULSE_BATTERY_UNKNOWN ? -1 : data[6];
  event.flags = data[7];
  event.deviceUptimeSeconds = static_cast<uint32_t>(data[8])
      | (static_cast<uint32_t>(data[9]) << 8)
      | (static_cast<uint32_t>(data[10]) << 16)
      | (static_cast<uint32_t>(data[11]) << 24);
  return true;
}

bool decodeGattCommandAckFrame(
    const uint8_t* data,
    size_t length,
    GattCommandAckMessage& ack) {
  if (!data || length != MEDPULSE_GATT_ACK_FRAME_LENGTH) return false;
  if (data[0] != MEDPULSE_FRAME_MAGIC_M || data[1] != MEDPULSE_FRAME_MAGIC_P
      || data[2] != MEDPULSE_COMMAND_PROTOCOL_VERSION
      || data[3] != MEDPULSE_FRAME_TYPE_COMMAND_ACK) return false;
  if (data[8] < static_cast<uint8_t>(MedPulseCommandAckStatus::EXECUTED)
      || data[8] > static_cast<uint8_t>(MedPulseCommandAckStatus::FAILED)) return false;
  if (data[8] == static_cast<uint8_t>(MedPulseCommandAckStatus::EXECUTED)
      && data[9] != static_cast<uint8_t>(MedPulseCommandAckError::NONE)) return false;
  const uint16_t expectedCrc = static_cast<uint16_t>(data[10])
      | (static_cast<uint16_t>(data[11]) << 8);
  if (medpulseCrc16Ccitt(data, 10) != expectedCrc) return false;
  ack.token = readUint32Le(data + 4);
  ack.status = data[8];
  ack.errorCode = data[9];
  return true;
}

const char* gattEventTypeName(uint8_t eventType) {
  switch (static_cast<MedPulseEventType>(eventType)) {
    case MedPulseEventType::HEARTBEAT: return "HEARTBEAT";
    case MedPulseEventType::PREALERT: return "PREALERT";
    case MedPulseEventType::FALL_ALERT: return "FALL_ALERT";
    case MedPulseEventType::RECOVERED: return "RECOVERED";
    case MedPulseEventType::HARDWARE_FAULT: return "HARDWARE_FAULT";
    case MedPulseEventType::HARDWARE_RECOVERED: return "HARDWARE_RECOVERED";
    default: return "UNKNOWN";
  }
}

void onMedPulseGattNotify(
    BLERemoteCharacteristic* characteristic,
    uint8_t* data,
    size_t length,
    bool isNotify) {
  if (!isNotify || !gattEventQueue) return;

  bool matched = false;
  char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1] = {};
  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    const TrackedBleDevice& device = trackedBleDevices[index];
    if (device.used && device.gattState == GATT_READY
        && device.gattEventCharacteristic == characteristic) {
      strncpy(deviceId, device.deviceId, BLE_DEVICE_ID_MAX_LENGTH);
      deviceId[BLE_DEVICE_ID_MAX_LENGTH] = '\0';
      matched = true;
      break;
    }
  }
  portEXIT_CRITICAL(&bleStateMux);

  if (!matched) {
    malformedGattEventCount++;
    return;
  }

  if (length >= 4 && data[3] == MEDPULSE_FRAME_TYPE_EVENT) {
    GattEventMessage event = {};
    if (!decodeGattEventFrame(data, length, event)) {
      malformedGattEventCount++;
      return;
    }
    strncpy(event.deviceId, deviceId, BLE_DEVICE_ID_MAX_LENGTH);
    if (xQueueSend(gattEventQueue, &event, 0) != pdTRUE) droppedGattEventCount++;
    return;
  }

  if (length >= 4 && data[3] == MEDPULSE_FRAME_TYPE_COMMAND_ACK && gattAckQueue) {
    GattCommandAckMessage ack = {};
    if (!decodeGattCommandAckFrame(data, length, ack)) {
      malformedGattEventCount++;
      return;
    }
    strncpy(ack.deviceId, deviceId, BLE_DEVICE_ID_MAX_LENGTH);
    if (xQueueSend(gattAckQueue, &ack, 0) != pdTRUE) droppedGattEventCount++;
    return;
  }
  malformedGattEventCount++;
}

class MedPulseGattClientCallbacks : public BLEClientCallbacks {
 public:
  explicit MedPulseGattClientCallbacks(uint8_t slotIndex) : slot(slotIndex) {}

  void onConnect(BLEClient* client) override {
    (void)client;
  }

  void onDisconnect(BLEClient* client) override {
    (void)client;
    const unsigned long now = millis();
    portENTER_CRITICAL(&bleStateMux);
    if (slot < MAX_TRACKED_BLE_DEVICES && trackedBleDevices[slot].used
        && trackedBleDevices[slot].gattClient == client) {
      trackedBleDevices[slot].gattState = GATT_BACKOFF;
      trackedBleDevices[slot].gattConnectQueued = false;
      trackedBleDevices[slot].lastGattAttemptMs = now;
      trackedBleDevices[slot].gattEventCharacteristic = nullptr;
      trackedBleDevices[slot].gattCommandCharacteristic = nullptr;
      trackedBleDevices[slot].pendingGattTransportUpdate = true;
    }
    portEXIT_CRITICAL(&bleStateMux);
  }

 private:
  uint8_t slot;
};

void setGattBackoff(
    uint8_t slot,
    BLEClient* client,
    MedPulseGattClientCallbacks* callbacks = nullptr) {
  const unsigned long now = millis();
  portENTER_CRITICAL(&bleStateMux);
  if (slot < MAX_TRACKED_BLE_DEVICES && trackedBleDevices[slot].used) {
    trackedBleDevices[slot].gattState = GATT_BACKOFF;
    trackedBleDevices[slot].gattConnectQueued = false;
    trackedBleDevices[slot].lastGattAttemptMs = now;
    trackedBleDevices[slot].gattClient = client;
    trackedBleDevices[slot].gattClientCallbacks = callbacks;
    trackedBleDevices[slot].gattEventCharacteristic = nullptr;
    trackedBleDevices[slot].gattCommandCharacteristic = nullptr;
    trackedBleDevices[slot].pendingGattTransportUpdate = true;
  }
  portEXIT_CRITICAL(&bleStateMux);
}

void gattConnectionWorker(void* parameter) {
  (void)parameter;
  uint8_t slot = 0;

  for (;;) {
    if (xQueueReceive(gattConnectQueue, &slot, portMAX_DELAY) != pdTRUE) continue;

    char address[18] = {};
    char deviceId[BLE_DEVICE_ID_MAX_LENGTH + 1] = {};
    BLEClient* oldClient = nullptr;
    MedPulseGattClientCallbacks* oldCallbacks = nullptr;
    bool canConnect = false;

    portENTER_CRITICAL(&bleStateMux);
    if (slot < MAX_TRACKED_BLE_DEVICES && trackedBleDevices[slot].used
        && trackedBleDevices[slot].online && strlen(trackedBleDevices[slot].address) > 0) {
      TrackedBleDevice& device = trackedBleDevices[slot];
      device.gattState = GATT_CONNECTING;
      device.gattConnectQueued = false;
      device.lastGattAttemptMs = millis();
      strncpy(address, device.address, sizeof(address) - 1);
      strncpy(deviceId, device.deviceId, sizeof(deviceId) - 1);
      oldClient = device.gattClient;
      oldCallbacks = device.gattClientCallbacks;
      device.gattClient = nullptr;
      device.gattClientCallbacks = nullptr;
      device.gattEventCharacteristic = nullptr;
      device.gattCommandCharacteristic = nullptr;
      canConnect = true;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!canConnect) continue;
    gattOperationInProgress = true;
    if (bleScan) bleScan->stop();

    // disconnect() hoan tat bat dong bo. Khong huy BLEClient khi stack van con
    // giu ket noi, neu khong callback co the truy cap con tro da bi giai phong.
    if (oldClient && oldClient->isConnected()) {
      oldClient->disconnect();
      setGattBackoff(slot, oldClient, oldCallbacks);
      bleScanRestartRequested = true;
      gattOperationInProgress = false;
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    if (oldClient) {
      delete oldClient;
      oldClient = nullptr;
    }
    if (oldCallbacks) {
      delete oldCallbacks;
      oldCallbacks = nullptr;
    }

    BLEClient* client = BLEDevice::createClient();
    if (!client) {
      setGattBackoff(slot, nullptr);
      bleScanRestartRequested = true;
      gattOperationInProgress = false;
      continue;
    }
    MedPulseGattClientCallbacks* callbacks = new MedPulseGattClientCallbacks(slot);
    client->setClientCallbacks(callbacks);

    Serial.print("[GATT] Connecting ");
    Serial.print(deviceId);
    Serial.print(" @ ");
    Serial.println(address);

    bool ready = false;
    BLERemoteCharacteristic* eventCharacteristic = nullptr;
    BLERemoteCharacteristic* commandCharacteristic = nullptr;
    if (client->connect(BLEAddress(address))) {
      BLERemoteService* service = client->getService(BLEUUID(MEDPULSE_GATT_SERVICE_UUID));
      if (service) {
        eventCharacteristic = service->getCharacteristic(BLEUUID(MEDPULSE_GATT_EVENT_UUID));
        commandCharacteristic = service->getCharacteristic(BLEUUID(MEDPULSE_GATT_COMMAND_UUID));
        ready = eventCharacteristic && eventCharacteristic->canNotify()
             && commandCharacteristic && commandCharacteristic->canWrite();
      }
    }

    if (ready) {
      portENTER_CRITICAL(&bleStateMux);
      if (trackedBleDevices[slot].used && strcmp(trackedBleDevices[slot].deviceId, deviceId) == 0) {
        trackedBleDevices[slot].gattState = GATT_READY;
        trackedBleDevices[slot].gattClient = client;
        trackedBleDevices[slot].gattClientCallbacks = callbacks;
        trackedBleDevices[slot].gattEventCharacteristic = eventCharacteristic;
        trackedBleDevices[slot].gattCommandCharacteristic = commandCharacteristic;
        trackedBleDevices[slot].gattConnectedAtMs = millis();
        trackedBleDevices[slot].online = true;
        trackedBleDevices[slot].pendingGattTransportUpdate = true;
      }
      portEXIT_CRITICAL(&bleStateMux);
      eventCharacteristic->registerForNotify(onMedPulseGattNotify, true);
      Serial.print("[GATT] Ready: ");
      Serial.println(deviceId);
    } else {
      Serial.print("[GATT] Service/characteristic unavailable: ");
      Serial.println(deviceId);
      if (client->isConnected()) client->disconnect();
      setGattBackoff(slot, client, callbacks);
    }

    bleScanRestartRequested = true;
    gattOperationInProgress = false;
    vTaskDelay(pdMS_TO_TICKS(20));
  }
}

void initializeGattConnectionManager() {
  if (gattConnectQueue) return;
  gattConnectQueue = xQueueCreate(MAX_TRACKED_BLE_DEVICES, sizeof(uint8_t));
  if (!gattConnectQueue) {
    Serial.println("[GATT] Cannot create connection queue.");
    return;
  }
  gattEventQueue = xQueueCreate(MEDPULSE_GATT_EVENT_QUEUE_LENGTH, sizeof(GattEventMessage));
  if (!gattEventQueue) {
    Serial.println("[GATT] Cannot create event queue.");
    vQueueDelete(gattConnectQueue);
    gattConnectQueue = nullptr;
    return;
  }
  gattAckQueue = xQueueCreate(MEDPULSE_GATT_ACK_QUEUE_LENGTH, sizeof(GattCommandAckMessage));
  if (!gattAckQueue) {
    Serial.println("[GATT] Cannot create ACK queue.");
    vQueueDelete(gattConnectQueue);
    vQueueDelete(gattEventQueue);
    gattConnectQueue = nullptr;
    gattEventQueue = nullptr;
    return;
  }
  BaseType_t result = xTaskCreate(
      gattConnectionWorker,
      "medpulse-gatt",
      8192,
      nullptr,
      1,
      &gattWorkerTaskHandle);
  if (result != pdPASS) {
    Serial.println("[GATT] Cannot create worker task.");
    vQueueDelete(gattConnectQueue);
    vQueueDelete(gattEventQueue);
    vQueueDelete(gattAckQueue);
    gattConnectQueue = nullptr;
    gattEventQueue = nullptr;
    gattAckQueue = nullptr;
  }
}

void initializeDeviceCommandQueue() {
  if (deviceCommandQueue) return;
  deviceCommandQueue = xQueueCreate(
      MEDPULSE_DEVICE_COMMAND_QUEUE_LENGTH, sizeof(PendingDeviceCommand));
  rejectedCommandQueue = xQueueCreate(
      MEDPULSE_DEVICE_COMMAND_QUEUE_LENGTH, sizeof(PendingDeviceCommand));
  if (!deviceCommandQueue || !rejectedCommandQueue) {
    Serial.println("[COMMAND] Cannot create command queues.");
    if (deviceCommandQueue) vQueueDelete(deviceCommandQueue);
    if (rejectedCommandQueue) vQueueDelete(rejectedCommandQueue);
    deviceCommandQueue = nullptr;
    rejectedCommandQueue = nullptr;
  }
}

void maintainGattConnections() {
  if (!gattConnectQueue) return;
  const unsigned long now = millis();
  uint8_t activeConnections = 0;

  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    const uint8_t state = trackedBleDevices[index].gattState;
    if (trackedBleDevices[index].used
        && (state == GATT_QUEUED || state == GATT_CONNECTING || state == GATT_READY)) {
      activeConnections++;
    }
  }
  portEXIT_CRITICAL(&bleStateMux);
  if (activeConnections >= MEDPULSE_MAX_GATT_CONNECTIONS) return;

  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES && activeConnections < MEDPULSE_MAX_GATT_CONNECTIONS; index++) {
    bool shouldQueue = false;
    portENTER_CRITICAL(&bleStateMux);
    TrackedBleDevice& device = trackedBleDevices[index];
    const bool retryReady = device.gattState == GATT_DISCONNECTED
        || (device.gattState == GATT_BACKOFF && now - device.lastGattAttemptMs >= MEDPULSE_GATT_RECONNECT_BACKOFF_MS);
    if (device.used && device.online && !device.gattConnectQueued && retryReady && strlen(device.address) > 0) {
      device.gattConnectQueued = true;
      device.gattState = GATT_QUEUED;
      shouldQueue = true;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!shouldQueue) continue;
    if (xQueueSend(gattConnectQueue, &index, 0) == pdTRUE) {
      activeConnections++;
    } else {
      portENTER_CRITICAL(&bleStateMux);
      trackedBleDevices[index].gattConnectQueued = false;
      trackedBleDevices[index].gattState = GATT_BACKOFF;
      trackedBleDevices[index].lastGattAttemptMs = now;
      portEXIT_CRITICAL(&bleStateMux);
    }
  }
}

void onBleScanComplete(BLEScanResults) {
  // Khởi động lại ở loop chính để giải phóng kết quả scan cũ và tránh tăng RAM lâu dài.
  bleScanRestartRequested = true;
}

void initializeBleScanner() {
  Serial.println("[BLE] Khoi tao scanner...");
  BLEDevice::init(STATION_ID);
  bleScan = BLEDevice::getScan();
  bleScan->setAdvertisedDeviceCallbacks(&bleAdvertisedDeviceCallbacks, true);
  bleScan->setActiveScan(true);
  bleScan->setInterval(100);
  bleScan->setWindow(80);
  initializeGattConnectionManager();

  if (bleScan->start(BLE_SCAN_CYCLE_SECONDS, onBleScanComplete, false)) {
    Serial.print("[BLE] Dang quet thiet bi co prefix: ");
    Serial.println(BLE_DEVICE_NAME_PREFIX);
  } else {
    Serial.println("[BLE] Khong the bat dau scan.");
  }
}

void maintainBleScanner() {
  if (!bleScan || !bleScanRestartRequested || gattOperationInProgress) return;
  bleScanRestartRequested = false;
  bleScan->clearResults();
  if (!bleScan->start(BLE_SCAN_CYCLE_SECONDS, onBleScanComplete, false)) {
    Serial.println("[BLE] Restart scan failed, se thu lai.");
    bleScanRestartRequested = true;
  }
}

bool publishBlePresenceEvent(const TrackedBleDevice& device, const char* eventName) {
  ensureWiFi();
  ensureMQTT();
  if (!mqtt.connected()) return false;

  String topic = medpulseStationTopic(MQTT_ROOT_TOPIC, STATION_ID, "ble");
  char payload[384];
  snprintf(payload, sizeof(payload),
           "{"
           "\"protocolVersion\":%u,"
           "\"stationId\":\"%s\","
           "\"deviceId\":\"%s\","
           "\"event\":\"%s\","
           "\"bleStatus\":\"%s\","
           "\"rssi\":%d,"
           "\"battery\":%d,"
           "\"stationUptimeMs\":%lu,"
           "\"lastSeenAgoMs\":%lu"
           "}",
           MEDPULSE_TELEMETRY_PROTOCOL_VERSION,
           STATION_ID,
           device.deviceId,
           eventName,
           strcmp(eventName, "LOST") == 0 ? "DISCONNECTED" : "CONNECTED",
           device.rssi,
           device.battery,
           millis(),
           millis() - device.lastSeenMs);

  bool ok = mqtt.publish(topic.c_str(), payload, false);
  Serial.print("[BLE MQTT] ");
  Serial.print(eventName);
  Serial.print(" ");
  Serial.print(device.deviceId);
  Serial.println(ok ? " -> OK" : " -> FAILED");
  return ok;
}

void handleBlePresenceEvents() {
  const unsigned long now = millis();

  for (uint8_t i = 0; i < MAX_TRACKED_BLE_DEVICES; i++) {
    TrackedBleDevice snapshot = {};
    bool shouldPublishSeen = false;
    bool shouldPublishLost = false;

    portENTER_CRITICAL(&bleStateMux);
    TrackedBleDevice& device = trackedBleDevices[i];
    if (device.used) {
      if (device.online && device.gattState != GATT_READY && now - device.lastSeenMs >= BLE_LOCAL_LOST_MS) {
        device.online = false;
        device.pendingSeen = false;
        device.pendingLost = true;
      }

      shouldPublishLost = device.pendingLost;
      shouldPublishSeen = !shouldPublishLost && device.pendingSeen;
      snapshot = device;
      if (shouldPublishLost) device.pendingLost = false;
      if (shouldPublishSeen) device.pendingSeen = false;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!snapshot.used || (!shouldPublishSeen && !shouldPublishLost)) continue;

    const char* eventName = shouldPublishLost ? "LOST" : "SEEN";
    if (publishBlePresenceEvent(snapshot, eventName)) {
      portENTER_CRITICAL(&bleStateMux);
      if (trackedBleDevices[i].used
          && strncmp(trackedBleDevices[i].deviceId, snapshot.deviceId, BLE_DEVICE_ID_MAX_LENGTH) == 0) {
        trackedBleDevices[i].lastPublishedMs = now;
      }
      portEXIT_CRITICAL(&bleStateMux);
    } else {
      portENTER_CRITICAL(&bleStateMux);
      if (trackedBleDevices[i].used
          && strncmp(trackedBleDevices[i].deviceId, snapshot.deviceId, BLE_DEVICE_ID_MAX_LENGTH) == 0) {
        if (shouldPublishLost) trackedBleDevices[i].pendingLost = true;
        if (shouldPublishSeen) trackedBleDevices[i].pendingSeen = true;
      }
      portEXIT_CRITICAL(&bleStateMux);
    }
  }
}

bool publishGattTransportState(const TrackedBleDevice& device, bool gattReady) {
  if (!mqtt.connected()) return false;
  const String topic = medpulseGatewayDeviceTopic(
      MQTT_ROOT_TOPIC, STATION_ID, device.deviceId, "transport");
  char payload[384];
  snprintf(payload, sizeof(payload),
           "{"
           "\"protocolVersion\":%u,"
           "\"stationId\":\"%s\","
           "\"deviceId\":\"%s\","
           "\"transport\":\"BLE_GATEWAY\","
           "\"online\":%s,"
           "\"gattReady\":%s,"
           "\"rssi\":%d,"
           "\"stationUptimeMs\":%lu"
           "}",
           MEDPULSE_COMMAND_PROTOCOL_VERSION,
           STATION_ID,
           device.deviceId,
           gattReady ? "true" : "false",
           gattReady ? "true" : "false",
           device.rssi,
           millis());
  const bool published = mqtt.publish(topic.c_str(), payload, false);
  Serial.print("[GATT TRANSPORT MQTT] ");
  Serial.print(device.deviceId);
  Serial.println(published ? (gattReady ? " -> ONLINE" : " -> OFFLINE") : " -> RETRY");
  return published;
}

void handleGattTransportUpdates() {
  const unsigned long now = millis();
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    TrackedBleDevice snapshot = {};
    bool shouldPublish = false;
    bool gattReady = false;

    portENTER_CRITICAL(&bleStateMux);
    TrackedBleDevice& device = trackedBleDevices[index];
    if (device.used) {
      gattReady = device.gattState == GATT_READY;
      const bool stateChanged = !device.hasPublishedGattTransport
          || device.lastPublishedGattReady != gattReady;
      shouldPublish = (device.pendingGattTransportUpdate && stateChanged)
          || (gattReady && now - device.lastGattTransportPublishedMs >= MEDPULSE_GATT_STATUS_INTERVAL_MS);
      if (shouldPublish) snapshot = device;
      else if (device.pendingGattTransportUpdate) device.pendingGattTransportUpdate = false;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!shouldPublish || !publishGattTransportState(snapshot, gattReady)) continue;
    portENTER_CRITICAL(&bleStateMux);
    TrackedBleDevice& current = trackedBleDevices[index];
    if (current.used && strcmp(current.deviceId, snapshot.deviceId) == 0
        && (current.gattState == GATT_READY) == gattReady) {
      current.pendingGattTransportUpdate = false;
      current.hasPublishedGattTransport = true;
      current.lastPublishedGattReady = gattReady;
      current.lastGattTransportPublishedMs = now;
    }
    portEXIT_CRITICAL(&bleStateMux);
  }
}

bool publishBleVitals(const TrackedBleDevice& device) {
  ensureWiFi();
  ensureMQTT();
  if (!mqtt.connected()) return false;

  String topic = medpulseGatewayDeviceTopic(MQTT_ROOT_TOPIC, STATION_ID, device.deviceId, "vitals");
  char payload[512];
  snprintf(payload, sizeof(payload),
           "{"
           "\"protocolVersion\":%u,"
           "\"stationId\":\"%s\","
           "\"deviceId\":\"%s\","
           "\"sequence\":%u,"
           "\"heartRate\":%u,"
           "\"spo2\":%u,"
           "\"temp\":%.1f,"
           "\"battery\":%d,"
           "\"charging\":%s,"
           "\"fallDetected\":%s,"
           "\"firmwareStatus\":\"%s\","
           "\"rssi\":%d,"
           "\"stationUptimeMs\":%lu"
           "}",
           MEDPULSE_TELEMETRY_PROTOCOL_VERSION,
           STATION_ID,
           device.deviceId,
           device.vitalsSequence,
           device.heartRate,
           device.spo2,
           device.tempX10 / 10.0f,
           device.battery,
           device.charging ? "true" : "false",
           device.fallDetected ? "true" : "false",
           device.fallDetected ? "ALERT" : "IDLE",
           device.rssi,
           millis());

  const bool ok = mqtt.publish(topic.c_str(), payload, false);
  Serial.print("[BLE VITALS MQTT] ");
  Serial.print(device.deviceId);
  Serial.print(" seq=");
  Serial.print(device.vitalsSequence);
  Serial.println(ok ? " -> OK" : " -> FAILED");
  return ok;
}

void handleBleVitalsEvents() {
  for (uint8_t i = 0; i < MAX_TRACKED_BLE_DEVICES; i++) {
    TrackedBleDevice snapshot = {};
    bool shouldPublish = false;

    portENTER_CRITICAL(&bleStateMux);
    if (trackedBleDevices[i].used && trackedBleDevices[i].pendingVitals) {
      snapshot = trackedBleDevices[i];
      trackedBleDevices[i].pendingVitals = false;
      shouldPublish = true;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!shouldPublish) continue;
    if (!publishBleVitals(snapshot)) {
      portENTER_CRITICAL(&bleStateMux);
      if (trackedBleDevices[i].used
          && strncmp(trackedBleDevices[i].deviceId, snapshot.deviceId, BLE_DEVICE_ID_MAX_LENGTH) == 0
          && trackedBleDevices[i].vitalsSequence == snapshot.vitalsSequence) {
        trackedBleDevices[i].pendingVitals = true;
      }
      portEXIT_CRITICAL(&bleStateMux);
    }
  }
}

bool publishGattEvent(const GattEventMessage& event, const TrackedBleDevice& device) {
  if (!mqtt.connected()) return false;

  const String topic = medpulseGatewayDeviceTopic(
      MQTT_ROOT_TOPIC, STATION_ID, event.deviceId, "events");
  char payload[512];
  snprintf(payload, sizeof(payload),
           "{"
           "\"protocolVersion\":%u,"
           "\"stationId\":\"%s\","
           "\"deviceId\":\"%s\","
           "\"sequence\":%u,"
           "\"event\":\"%s\","
           "\"battery\":%d,"
           "\"fallDetected\":%s,"
           "\"charging\":%s,"
           "\"buzzerActive\":%s,"
           "\"deviceUptimeSeconds\":%lu,"
           "\"rssi\":%d,"
           "\"stationUptimeMs\":%lu"
           "}",
           MEDPULSE_COMMAND_PROTOCOL_VERSION,
           STATION_ID,
           event.deviceId,
           event.sequence,
           gattEventTypeName(event.eventType),
           event.battery,
           (event.flags & MEDPULSE_GATT_EVENT_FLAG_FALL) ? "true" : "false",
           (event.flags & MEDPULSE_GATT_EVENT_FLAG_CHARGING) ? "true" : "false",
           (event.flags & MEDPULSE_GATT_EVENT_FLAG_BUZZER) ? "true" : "false",
           static_cast<unsigned long>(event.deviceUptimeSeconds),
           device.rssi,
           millis());

  const bool published = mqtt.publish(topic.c_str(), payload, false);
  Serial.print("[GATT EVENT MQTT] ");
  Serial.print(event.deviceId);
  Serial.print(" seq=");
  Serial.print(event.sequence);
  Serial.print(" type=");
  Serial.print(gattEventTypeName(event.eventType));
  Serial.println(published ? " -> OK" : " -> RETRY");
  return published;
}

void handleGattEvents() {
  if (!gattEventQueue) return;

  for (uint8_t processed = 0; processed < MEDPULSE_GATT_EVENTS_PER_LOOP; processed++) {
    GattEventMessage event = {};
    if (xQueueReceive(gattEventQueue, &event, 0) != pdTRUE) break;

    bool isFresh = false;
    TrackedBleDevice snapshot = {};
    int matchedIndex = -1;
    portENTER_CRITICAL(&bleStateMux);
    for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
      TrackedBleDevice& device = trackedBleDevices[index];
      if (!device.used || strcmp(device.deviceId, event.deviceId) != 0) continue;

      // Sai phan tu 1..127 la ban tin moi; 0 la trung, 128..255 la goi cu den tre.
      const uint8_t sequenceDelta = static_cast<uint8_t>(event.sequence - device.lastGattEventSequence);
      if (!device.hasGattEventSequence || (sequenceDelta > 0 && sequenceDelta < 128)) {
        snapshot = device;
        matchedIndex = index;
        isFresh = true;
      }
      break;
    }
    portEXIT_CRITICAL(&bleStateMux);

    if (!isFresh) continue;
    if (!publishGattEvent(event, snapshot)) {
      if (xQueueSendToFront(gattEventQueue, &event, 0) != pdTRUE) droppedGattEventCount++;
      break;
    }

    portENTER_CRITICAL(&bleStateMux);
    TrackedBleDevice& device = trackedBleDevices[matchedIndex];
    if (device.used && strcmp(device.deviceId, event.deviceId) == 0) {
      device.hasGattEventSequence = true;
      device.lastGattEventSequence = event.sequence;
      device.lastSeenMs = millis();
      device.online = true;
      if (event.battery >= 0) device.battery = event.battery;
    }
    portEXIT_CRITICAL(&bleStateMux);
  }
}

bool publishDeviceCommandAck(const PendingDeviceCommand& command, const char* status, const char* errorMessage = nullptr) {
  ensureMQTT();
  if (!mqtt.connected()) return false;
  String topic = medpulseDirectDeviceTopic(MQTT_ROOT_TOPIC, command.deviceId, "acks");
  JsonDocument document;
  document["protocolVersion"] = MEDPULSE_COMMAND_PROTOCOL_VERSION;
  document["commandId"] = command.commandId;
  document["deviceId"] = command.deviceId;
  document["status"] = status;
  document["transport"] = "BLE_GATEWAY";
  document["stationId"] = STATION_ID;
  if (errorMessage) document["error"] = errorMessage;
  char payload[384];
  size_t length = serializeJson(document, payload, sizeof(payload));
  const bool published = mqtt.publish(topic.c_str(), reinterpret_cast<const uint8_t*>(payload), length, false);
  Serial.print("[COMMAND ACK MQTT] ");
  Serial.print(command.commandId);
  Serial.print(" status=");
  Serial.print(status);
  if (errorMessage) {
    Serial.print(" error=");
    Serial.print(errorMessage);
  }
  Serial.println(published ? " -> PUB" : " -> RETRY");
  return published;
}

void serviceRejectedDeviceCommands() {
  if (!rejectedCommandQueue) return;
  PendingDeviceCommand command = {};
  if (xQueuePeek(rejectedCommandQueue, &command, 0) != pdTRUE) return;
  if (!publishDeviceCommandAck(command, "FAILED", "STATION_COMMAND_QUEUE_FULL")) return;
  xQueueReceive(rejectedCommandQueue, &command, 0);
}

uint8_t commandActionCode(const char* action) {
  if (strcmp(action, "BUZZER_ON") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_ON);
  if (strcmp(action, "BUZZER_OFF") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_OFF);
  if (strcmp(action, "BUZZER_TEST") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_TEST);
  return 0;
}

const char* commandAckErrorName(uint8_t errorCode) {
  switch (errorCode) {
    case static_cast<uint8_t>(MedPulseCommandAckError::NONE): return nullptr;
    case static_cast<uint8_t>(MedPulseCommandAckError::INVALID_FRAME): return "DEVICE_INVALID_FRAME";
    case static_cast<uint8_t>(MedPulseCommandAckError::UNSUPPORTED_ACTION): return "DEVICE_UNSUPPORTED_ACTION";
    case static_cast<uint8_t>(MedPulseCommandAckError::EXECUTION_FAILED): return "DEVICE_EXECUTION_FAILED";
    case static_cast<uint8_t>(MedPulseCommandAckError::DUPLICATE_COMMAND): return "DEVICE_DUPLICATE_COMMAND";
    default: return "DEVICE_UNKNOWN_ERROR";
  }
}

void serviceGattCommandAcks() {
  if (!gattAckQueue) return;

  GattCommandAckMessage ack = {};
  while (xQueueReceive(gattAckQueue, &ack, 0) == pdTRUE) {
    if (!inflightDeviceCommand.active
        || inflightDeviceCommand.token != ack.token
        || strcmp(inflightDeviceCommand.command.deviceId, ack.deviceId) != 0) {
      Serial.println("[COMMAND ACK] ACK cu hoac khong khop, bo qua.");
      continue;
    }
    inflightDeviceCommand.ackReceived = true;
    inflightDeviceCommand.ackStatus = ack.status;
    inflightDeviceCommand.ackErrorCode = ack.errorCode;
    Serial.print("[COMMAND ACK] Nhan ACK tu thiet bi token=");
    Serial.print(ack.token, HEX);
    Serial.print(" status=");
    Serial.print(ack.status);
    Serial.print(" error=");
    Serial.println(ack.errorCode);
  }

  if (!inflightDeviceCommand.active) return;
  if (!inflightDeviceCommand.ackReceived
      && millis() - inflightDeviceCommand.sentAtMs >= MEDPULSE_GATT_COMMAND_ACK_TIMEOUT_MS) {
    inflightDeviceCommand.ackReceived = true;
    inflightDeviceCommand.ackTimedOut = true;
    inflightDeviceCommand.ackStatus = static_cast<uint8_t>(MedPulseCommandAckStatus::FAILED);
  }
  if (!inflightDeviceCommand.ackReceived) return;

  const bool executed = !inflightDeviceCommand.ackTimedOut
      && inflightDeviceCommand.ackStatus == static_cast<uint8_t>(MedPulseCommandAckStatus::EXECUTED);
  const char* errorMessage = inflightDeviceCommand.ackTimedOut
      ? "DEVICE_ACK_TIMEOUT"
      : commandAckErrorName(inflightDeviceCommand.ackErrorCode);
  if (!publishDeviceCommandAck(
          inflightDeviceCommand.command,
          executed ? "EXECUTED" : "FAILED",
          executed ? nullptr : (errorMessage ? errorMessage : "DEVICE_REPORTED_FAILED"))) return;

  Serial.print("[COMMAND ACK] ");
  Serial.print(inflightDeviceCommand.command.commandId);
  Serial.println(executed ? " -> EXECUTED" : " -> FAILED");
  memset(&inflightDeviceCommand, 0, sizeof(inflightDeviceCommand));
}

void executePendingDeviceCommand() {
  if (inflightDeviceCommand.active || !deviceCommandQueue) return;
  PendingDeviceCommand command = {};
  if (xQueueReceive(deviceCommandQueue, &command, 0) != pdTRUE) return;
  if (millis() - command.receivedAtMs >= MEDPULSE_STATION_COMMAND_MAX_QUEUE_MS) {
    publishDeviceCommandAck(command, "FAILED", "STATION_COMMAND_QUEUE_TIMEOUT");
    return;
  }

  TrackedBleDevice target = {};
  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    if (trackedBleDevices[index].used
        && strcmp(trackedBleDevices[index].deviceId, command.deviceId) == 0) {
      target = trackedBleDevices[index];
      break;
    }
  }
  portEXIT_CRITICAL(&bleStateMux);

  if (!target.used || !target.online || target.gattState != GATT_READY
      || !target.gattClient || !target.gattClient->isConnected() || !target.gattCommandCharacteristic) {
    Serial.print("[COMMAND] GATT_NOT_READY device=");
    Serial.print(command.deviceId);
    Serial.print(" used=");
    Serial.print(target.used ? 1 : 0);
    Serial.print(" online=");
    Serial.print(target.online ? 1 : 0);
    Serial.print(" gattState=");
    Serial.print(target.gattState);
    Serial.print(" client=");
    Serial.print(target.gattClient ? 1 : 0);
    Serial.print(" connected=");
    Serial.print((target.gattClient && target.gattClient->isConnected()) ? 1 : 0);
    Serial.print(" cmdChar=");
    Serial.println(target.gattCommandCharacteristic ? 1 : 0);
    if (!publishDeviceCommandAck(command, "FAILED", "GATT_NOT_READY")) {
      xQueueSendToFront(deviceCommandQueue, &command, 0);
    }
    return;
  }

  uint8_t commandFrame[MEDPULSE_GATT_COMMAND_FRAME_LENGTH] = {};
  commandFrame[0] = MEDPULSE_FRAME_MAGIC_M;
  commandFrame[1] = MEDPULSE_FRAME_MAGIC_P;
  commandFrame[2] = MEDPULSE_COMMAND_PROTOCOL_VERSION;
  commandFrame[3] = MEDPULSE_FRAME_TYPE_COMMAND;
  const uint32_t commandToken = commandTokenForId(command.commandId);
  writeUint32Le(commandFrame + 4, commandToken);
  commandFrame[8] = commandActionCode(command.action);
  commandFrame[9] = 0;
  writeUint32Le(commandFrame + 10, command.durationMs);
  const uint16_t crc = medpulseCrc16Ccitt(commandFrame, 14);
  commandFrame[14] = crc & 0xFF;
  commandFrame[15] = (crc >> 8) & 0xFF;

  memset(&inflightDeviceCommand, 0, sizeof(inflightDeviceCommand));
  inflightDeviceCommand.active = true;
  inflightDeviceCommand.token = commandToken;
  inflightDeviceCommand.sentAtMs = millis();
  inflightDeviceCommand.command = command;

  bool delivered = false;
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  delivered = target.gattCommandCharacteristic->writeValue(
      commandFrame, sizeof(commandFrame), true);
#else
  // Arduino-ESP32 2.x cung cap writeValue() kieu void. Write With Response
  // van cho den GATT response; ACK Notify v2 moi la xac nhan thuc thi cuoi.
  target.gattCommandCharacteristic->writeValue(
      commandFrame, sizeof(commandFrame), true);
  delivered = target.gattClient->isConnected();
#endif

  if (!delivered) memset(&inflightDeviceCommand, 0, sizeof(inflightDeviceCommand));
  publishDeviceCommandAck(command, delivered ? "DELIVERED" : "FAILED",
                          delivered ? nullptr : "GATT_WRITE_FAILED");
  Serial.print("[COMMAND] ");
  Serial.print(command.commandId);
  Serial.print(" action=");
  Serial.print(command.action);
  Serial.print(" device=");
  Serial.print(command.deviceId);
  Serial.println(delivered ? " -> DELIVERED" : " -> FAILED");
}

void publishStationHeartbeat() {
  const unsigned long now = millis();
  if (now - lastStationHeartbeatMs < STATION_HEARTBEAT_INTERVAL_MS) return;
  lastStationHeartbeatMs = now;

  ensureWiFi();
  ensureMQTT();
  if (!mqtt.connected()) return;

  String topic = medpulseStationTopic(MQTT_ROOT_TOPIC, STATION_ID, "heartbeat");
  uint8_t trackedDevices = 0;
  uint8_t readyGattConnections = 0;
  portENTER_CRITICAL(&bleStateMux);
  for (uint8_t index = 0; index < MAX_TRACKED_BLE_DEVICES; index++) {
    if (!trackedBleDevices[index].used) continue;
    trackedDevices++;
    if (trackedBleDevices[index].gattState == GATT_READY) readyGattConnections++;
  }
  portEXIT_CRITICAL(&bleStateMux);

  char payload[512];
  snprintf(payload, sizeof(payload),
           "{"
           "\"protocolVersion\":%u,"
           "\"stationId\":\"%s\","
           "\"event\":\"STATION_HEARTBEAT\","
           "\"online\":true,"
           "\"bleScanner\":true,"
           "\"wifiRssi\":%d,"
           "\"uptimeMs\":%lu,"
           "\"freeHeap\":%lu,"
           "\"trackedBleDevices\":%u,"
           "\"gattReadyConnections\":%u,"
           "\"malformedGattFrames\":%lu,"
           "\"droppedGattFrames\":%lu,"
           "\"max30102Ready\":%s,"
           "\"mlx90614Ready\":%s,"
           "\"rc522Ready\":%s"
           "}",
           MEDPULSE_TELEMETRY_PROTOCOL_VERSION,
           STATION_ID,
           WiFi.RSSI(),
           now,
           static_cast<unsigned long>(ESP.getFreeHeap()),
           trackedDevices,
           readyGattConnections,
           static_cast<unsigned long>(malformedGattEventCount),
           static_cast<unsigned long>(droppedGattEventCount),
           max30102Ready ? "true" : "false",
           mlx90614Ready ? "true" : "false",
           rc522Ready ? "true" : "false");
  mqtt.publish(topic.c_str(), payload, false);
}

// ==================================================
// RFID
// ==================================================
String uidToString(MFRC522::Uid uid) {
  String result = "";

  for (byte i = 0; i < uid.size; i++) {
    if (uid.uidByte[i] < 0x10) {
      result += "0";
    }

    result += String(uid.uidByte[i], HEX);

    if (i < uid.size - 1) {
      result += " ";
    }
  }

  result.toUpperCase();
  return result;
}

String uidToPatientId(String uid) {
  String clean = uid;
  clean.replace(" ", "");

  return "RFID-" + clean;
}

void startMeasureSession(String uid, String patientId) {
  currentUID = uid;
  currentPatientId = patientId;

  resetTempFilter();
  resetHR();

  measureStartMs = millis();
  measurementSessionStartedAtMs = measureStartMs;
  lastMLXMs = 0;
  lastPrintMs = 0;

  measureState = MEASURING;

  Serial.println();
  Serial.println("====================================");
  Serial.println("[RFID] Da quet the thanh cong");
  Serial.print("[RFID] UID        = ");
  Serial.println(currentUID);
  Serial.print("[RFID] Patient ID = ");
  Serial.println(currentPatientId);
  Serial.println("[SYSTEM] Bat dau do nhiet do + nhip tim");
  Serial.println("Dat ngon tay len MAX30102 va dua co tay gan MLX90614.");
  Serial.println("====================================");
}

void handleRFID() {
  if (!rc522Ready || !max30102Ready || !mlx90614Ready) return;
  if (measureState != WAIT_RFID) {
    return;
  }

  if (!rfid.PICC_IsNewCardPresent()) {
    return;
  }

  if (!rfid.PICC_ReadCardSerial()) {
    return;
  }

  String uid = uidToString(rfid.uid);
  String patientId = uidToPatientId(uid);

  startMeasureSession(uid, patientId);

  rfid.PICC_HaltA();
  rfid.PCD_StopCrypto1();
}

// ==================================================
// MLX90614 5-SAMPLE PROCESS
// ==================================================
float calculateStableTemperatureFrom5Samples() {
  float sorted[TEMP_SAMPLE_SIZE];

  for (int i = 0; i < TEMP_SAMPLE_SIZE; i++) {
    sorted[i] = tempSamples[i];
  }

  for (int i = 0; i < TEMP_SAMPLE_SIZE - 1; i++) {
    for (int j = i + 1; j < TEMP_SAMPLE_SIZE; j++) {
      if (sorted[j] < sorted[i]) {
        float temp = sorted[i];
        sorted[i] = sorted[j];
        sorted[j] = temp;
      }
    }
  }

  // Bỏ mẫu thấp nhất và cao nhất, lấy trung bình 3 mẫu giữa.
  float stableValue = (sorted[1] + sorted[2] + sorted[3]) / 3.0f;

  return stableValue;
}

void processMLX() {
  if (!mlx90614Ready) return;
  if (tempStable) {
    return;
  }

  if (millis() - lastMLXMs < TEMP_SAMPLE_INTERVAL_MS) {
    return;
  }

  lastMLXMs = millis();

  float rawTemp = mlx.readObjectTempC();
  float calibratedTemp = rawTemp + TEMP_OFFSET_C;

  lastTempC = calibratedTemp;

  if (!isfinite(calibratedTemp) || calibratedTemp < 30.0f || calibratedTemp > 45.0f) {
    Serial.printf("[MLX90614] Raw=%.1f C | Cal=%.1f C >> BO QUA MAU LOI\n",
                  rawTemp,
                  calibratedTemp);
    if (++mlxInvalidSampleCount >= 5) {
      mlx90614Ready = false;
      mlxInvalidSampleCount = 0;
      resetTempFilter();
      Serial.println("[MLX90614] Qua nhieu mau loi; chuyen DEGRADED va se khoi tao lai.");
    }
    return;
  }
  mlxInvalidSampleCount = 0;

  tempSamples[tempSampleCount] = calibratedTemp;
  tempSampleCount++;

  Serial.printf("[MLX90614] Sample %d/5 | Raw=%.1f C | Cal=%.1f C\n",
                tempSampleCount,
                rawTemp,
                calibratedTemp);

  if (tempSampleCount < TEMP_SAMPLE_SIZE) {
    return;
  }

  float minT = tempSamples[0];
  float maxT = tempSamples[0];

  for (int i = 1; i < TEMP_SAMPLE_SIZE; i++) {
    if (tempSamples[i] < minT) minT = tempSamples[i];
    if (tempSamples[i] > maxT) maxT = tempSamples[i];
  }

  float rangeT = maxT - minT;
  float calculatedTemp = calculateStableTemperatureFrom5Samples();

  bool rangeOk = rangeT <= TEMP_MAX_RANGE_ACCEPT;
  bool humanRangeOk = calculatedTemp >= TEMP_MIN_VALID && calculatedTemp <= TEMP_MAX_VALID;

  Serial.println("========== MLX90614 5-SAMPLE RESULT ==========");
  Serial.printf("Samples: %.1f | %.1f | %.1f | %.1f | %.1f\n",
                tempSamples[0],
                tempSamples[1],
                tempSamples[2],
                tempSamples[3],
                tempSamples[4]);
  Serial.printf("Min=%.1f | Max=%.1f | Range=%.2f | StableTemp=%.1f\n",
                minT,
                maxT,
                rangeT,
                calculatedTemp);

  if (rangeOk && humanRangeOk) {
    stableTempC = calculatedTemp;
    tempStable = true;

    Serial.printf("[MLX90614] NHIET DO ON DINH = %.1f C\n", stableTempC);
    Serial.println("===============================================");
  } else {
    Serial.println("[MLX90614] CHUA ON DINH, DO LAI 5 MAU MOI...");
    Serial.println("===============================================");

    tempSampleCount = 0;
    tempStable = false;
    stableTempC = -999.0;
  }
}

// ==================================================
// MAX30102 PROCESS
// ==================================================
void processMAX() {
  if (!max30102Ready) return;
  long red = 0;
  long ir = 0;

  if (!max30102_readSample(red, ir)) {
    return;
  }

  lastRed = red;
  lastIr = ir;

  if (ir < 1000) {
    if (fingerPresent) {
      Serial.println("[MAX30102] Ngon tay roi ra, reset nhip tim.");
      resetHR();
      measureStartMs = millis();
    }

    return;
  }

  if (!fingerPresent) {
    fingerPresent = true;
    fingerSince = millis();

    dcFilter = ir;
    acValue = 0;

    Serial.printf("[MAX30102] Phat hien ngon tay. RED=%ld IR=%ld. Warm-up 3s...\n", red, ir);
  }

  if (!filterReady && millis() - fingerSince >= 3000) {
    filterReady = true;
    lastPeakMs = millis();

    Serial.println("[MAX30102] Filter on dinh. Bat dau tinh BPM.");
  }

  processBeat(ir);
  processSpo2(red, ir);
}

// ==================================================
// MEASURING STATUS + READY CHECK
// ==================================================
void printMeasureStatus() {
  if (millis() - lastPrintMs < 1000) {
    return;
  }

  lastPrintMs = millis();

  Serial.printf("[DO] Patient=%s | TempNow=%.1f C | TempStable=%s | StableTemp=%.1f C | BPM=%.1f | SpO2=%.1f (%s) | IR=%ld | AC=%ld\n",
                currentPatientId.c_str(),
                lastTempC,
                tempStable ? "YES" : "NO",
                stableTempC,
                beatAvg,
                stableSpo2,
                spo2Stable ? "READY" : "WAIT",
                lastIr,
                acValue);
}

bool isMeasurementReady() {
  if (!max30102Ready || !mlx90614Ready) return false;
  bool tempValid = tempStable && stableTempC >= TEMP_MIN_VALID && stableTempC <= TEMP_MAX_VALID;
  bool bpmValid = filterReady && beatAvg >= 40.0f && beatAvg <= 180.0f;
  bool spo2Valid = spo2Stable && stableSpo2 >= 70.0f && stableSpo2 <= 100.0f;

  // Không gửi quá sớm, dù đã đủ mẫu.
  bool enoughTime = millis() - measureStartMs >= 12000;

  return tempValid && bpmValid && spo2Valid && enoughTime;
}

// ==================================================
// MQTT PUBLISH
// ==================================================
bool publishVitalsToServer() {
  ensureWiFi();
  ensureMQTT();

  if (!mqtt.connected()) {
    Serial.println("[MQTT] Chua ket noi duoc, khong publish.");
    return false;
  }

  String topic = String(MQTT_ROOT_TOPIC) + "/" + currentPatientId + "/vitals";

  int heartRateValue = round(beatAvg);
  int measuredSpo2 = round(stableSpo2);

  int rssi = WiFi.RSSI();

  char payload[512];

  snprintf(payload, sizeof(payload),
           "{"
           "\"heartRate\":%d,"
           "\"spo2\":%d,"
           "\"temp\":%.1f,"
           "\"firmwareStatus\":\"IDLE\","
           "\"spo2Valid\":true,"
           "\"batteryValid\":false,"
           "\"signalLost\":false,"
           "\"rssi\":%d"
           "}",
           heartRateValue,
           measuredSpo2,
           stableTempC,
           rssi);

  bool ok = mqtt.publish(topic.c_str(), payload);

  Serial.println();
  Serial.println("========== MQTT PUBLISH ==========");
  Serial.print("[TOPIC] ");
  Serial.println(topic);
  Serial.print("[PAYLOAD] ");
  Serial.println(payload);
  Serial.print("[RESULT] ");
  Serial.println(ok ? "OK" : "FAILED");
  Serial.println("==================================");

  if (ok) lastPublishMs = millis();
  return ok;
}

// ==================================================
// MAIN PROCESS
// ==================================================
void handleMeasuring() {
  if (measureState != MEASURING) {
    return;
  }

  if (millis() - measurementSessionStartedAtMs >= MEDPULSE_MEASUREMENT_SESSION_TIMEOUT_MS) {
    Serial.println("[SYSTEM] Huy phien do do qua timeout; san sang nhan the moi.");
    currentUID = "";
    currentPatientId = "";
    resetTempFilter();
    resetHR();
    measureState = WAIT_RFID;
    return;
  }

  processMLX();
  processMAX();
  printMeasureStatus();

  if (isMeasurementReady()) {
    const unsigned long now = millis();
    if (now - lastVitalsPublishAttemptMs < MEDPULSE_VITALS_PUBLISH_RETRY_MS) return;
    lastVitalsPublishAttemptMs = now;
    Serial.println();
    Serial.println("[SYSTEM] Da do xong nhiet do, nhip tim va SpO2.");
    Serial.println("[SYSTEM] Dang gui goi tin len MQTT server...");

    if (publishVitalsToServer()) measureState = PUBLISH_DONE;
  }
}

void handlePublishDone() {
  if (measureState != PUBLISH_DONE) {
    return;
  }

  if (millis() - lastPublishMs >= 5000) {
    Serial.println();
    Serial.println("[SYSTEM] San sang quet the tiep theo.");
    Serial.println("====================================");

    currentUID = "";
    currentPatientId = "";

    resetTempFilter();
    resetHR();

    measureState = WAIT_RFID;
  }
}

bool initializeRc522() {
  rfid.PCD_Init();
  const byte version = rfid.PCD_ReadRegister(MFRC522::VersionReg);
  Serial.printf("[RC522] VersionReg = 0x%02X\n", version);
  return version != 0x00 && version != 0xFF;
}

void serviceSensorRecovery() {
  if (measureState != WAIT_RFID || (max30102Ready && mlx90614Ready && rc522Ready)) return;
  const unsigned long now = millis();
  if (now - lastSensorRecoveryAttemptMs < MEDPULSE_SENSOR_RETRY_INTERVAL_MS) return;
  lastSensorRecoveryAttemptMs = now;

  if (!max30102Ready) {
    Serial.println("[RECOVERY] Thu khoi tao lai MAX30102...");
    max30102Ready = max30102_init();
  }
  if (!mlx90614Ready) {
    Serial.println("[RECOVERY] Thu khoi tao lai MLX90614...");
    mlx90614Ready = mlx.begin();
  }
  if (!rc522Ready) {
    Serial.println("[RECOVERY] Thu khoi tao lai RC522...");
    rc522Ready = initializeRc522();
  }
}

// ==================================================
// SETUP
// ==================================================
void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("====================================");
  Serial.println(" MedPulse ESP32 Gateway");
  Serial.println(" RC522 + MAX30102 + MLX90614 + BLE + MQTT");
  Serial.println("====================================");

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);

  Serial.println("[MAX30102] Init...");
  max30102Ready = max30102_init();
  Serial.println(max30102Ready ? "[MAX30102] OK" : "[MAX30102] DEGRADED - se tu retry");

  Serial.println("[MLX90614] Init...");
  mlx90614Ready = mlx.begin();
  Serial.println(mlx90614Ready ? "[MLX90614] OK" : "[MLX90614] DEGRADED - se tu retry");

  Serial.println("[RC522] Init...");
  SPI.begin(RC522_SCK_PIN, RC522_MISO_PIN, RC522_MOSI_PIN, RC522_SS_PIN);
  rc522Ready = initializeRc522();
  Serial.println(rc522Ready ? "[RC522] OK" : "[RC522] DEGRADED - se tu retry");

  connectWiFi();

  initializeDeviceCommandQueue();
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  mqtt.setSocketTimeout(3);
  mqtt.setKeepAlive(30);
  mqtt.setCallback(handleMqttMessage);
  ensureMQTT();

  initializeBleScanner();

  Serial.println();
  Serial.println("[SYSTEM] San sang.");
  Serial.println("Hay quet the RFID de bat dau do.");
  Serial.println("====================================");
}

// ==================================================
// LOOP
// ==================================================
void loop() {
  ensureWiFi();
  ensureMQTT();

  if (mqtt.connected()) {
    mqtt.loop();
  }

  handleBlePresenceEvents();
  handleGattTransportUpdates();
  handleBleVitalsEvents();
  handleGattEvents();
  serviceRejectedDeviceCommands();
  serviceGattCommandAcks();
  executePendingDeviceCommand();
  publishStationHeartbeat();
  maintainBleScanner();
  maintainGattConnections();

  handleRFID();
  handleMeasuring();
  handlePublishDone();
  serviceSensorRecovery();

  delay(10);
}
