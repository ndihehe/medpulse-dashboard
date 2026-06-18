#include <WiFi.h>
#include <PubSubClient.h>

#include <Wire.h>
#include <Adafruit_MLX90614.h>

#include <SPI.h>
#include <MFRC522.h>

// ==================================================
// WIFI CONFIG
// ==================================================
// Lưu ý: ESP32 chỉ dùng WiFi 2.4GHz, không dùng được 5GHz.
const char* WIFI_SSID = "Thuy van 1";
const char* WIFI_PASS = "Saigonact0209";

// Nếu chạy Wokwi thì dùng:
// const char* WIFI_SSID = "Wokwi-GUEST";
// const char* WIFI_PASS = "";

// ==================================================
// MQTT CONFIG
// ==================================================
const char* MQTT_HOST = "broker.hivemq.com";
const int   MQTT_PORT = 1883;

// Backend đang nghe: medpulse_duy/+/vitals
const char* MQTT_ROOT_TOPIC = "medpulse_duy";

WiFiClient espClient;
PubSubClient mqtt(espClient);

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

  isPeak = false;
  lastPeakMs = 0;

  lastRed = 0;
  lastIr = 0;

  for (byte i = 0; i < RATE_SIZE; i++) {
    rates[i] = 0;
  }
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
  Serial.print("[WiFi] Connecting to ");
  Serial.println(WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  unsigned long start = millis();

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");

    if (millis() - start > 20000) {
      Serial.println();
      Serial.println("[WiFi] Timeout. Kiem tra WiFi 2.4GHz, SSID va password.");
      start = millis();
    }
  }

  Serial.println();
  Serial.print("[WiFi] Connected. IP = ");
  Serial.println(WiFi.localIP());
}

void ensureWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  Serial.println("[WiFi] Lost connection. Reconnecting...");
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASS);
}

void ensureMQTT() {
  if (mqtt.connected()) {
    return;
  }

  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  Serial.print("[MQTT] Connecting to ");
  Serial.println(MQTT_HOST);

  String clientId = "ESP32_KIOSK_" + String((uint32_t)ESP.getEfuseMac(), HEX);

  while (!mqtt.connected()) {
    if (mqtt.connect(clientId.c_str())) {
      Serial.println("[MQTT] Connected!");
    } else {
      Serial.print("[MQTT] Failed, state=");
      Serial.println(mqtt.state());
      delay(1000);
    }
  }
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
  // Sửa theo UID thẻ thật của nhóm bạn.
  if (uid == "01 02 03 04") return "RFID-1005";
  if (uid == "F5 87 64 03") return "RFID-1002";

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

  if (calibratedTemp < 30.0f || calibratedTemp > 45.0f) {
    Serial.printf("[MLX90614] Raw=%.1f C | Cal=%.1f C >> BO QUA MAU LOI\n",
                  rawTemp,
                  calibratedTemp);
    return;
  }

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
}

// ==================================================
// MEASURING STATUS + READY CHECK
// ==================================================
void printMeasureStatus() {
  if (millis() - lastPrintMs < 1000) {
    return;
  }

  lastPrintMs = millis();

  Serial.printf("[DO] Patient=%s | TempNow=%.1f C | TempStable=%s | StableTemp=%.1f C | BPM=%.1f | IR=%ld | AC=%ld\n",
                currentPatientId.c_str(),
                lastTempC,
                tempStable ? "YES" : "NO",
                stableTempC,
                beatAvg,
                lastIr,
                acValue);
}

bool isMeasurementReady() {
  bool tempValid = tempStable && stableTempC >= TEMP_MIN_VALID && stableTempC <= TEMP_MAX_VALID;
  bool bpmValid = filterReady && beatAvg >= 40.0f && beatAvg <= 180.0f;

  // Không gửi quá sớm, dù đã đủ mẫu.
  bool enoughTime = millis() - measureStartMs >= 12000;

  return tempValid && bpmValid && enoughTime;
}

// ==================================================
// MQTT PUBLISH
// ==================================================
void publishVitalsToServer() {
  ensureWiFi();
  ensureMQTT();

  if (!mqtt.connected()) {
    Serial.println("[MQTT] Chua ket noi duoc, khong publish.");
    return;
  }

  String topic = String(MQTT_ROOT_TOPIC) + "/" + currentPatientId + "/vitals";

  int heartRateValue = round(beatAvg);

  // Code hiện tại chưa tính SpO2 thật, gửi mặc định 98 để web có dữ liệu.
  int spo2Value = 98;

  int rssi = WiFi.RSSI();

  char payload[512];

  snprintf(payload, sizeof(payload),
           "{"
           "\"heartRate\":%d,"
           "\"spo2\":%d,"
           "\"temp\":%.1f,"
           "\"firmwareStatus\":\"IDLE\","
           "\"battery\":92,"
           "\"charging\":false,"
           "\"signalLost\":false,"
           "\"rssi\":%d"
           "}",
           heartRateValue,
           spo2Value,
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

  lastPublishMs = millis();
}

// ==================================================
// MAIN PROCESS
// ==================================================
void handleMeasuring() {
  if (measureState != MEASURING) {
    return;
  }

  processMLX();
  processMAX();
  printMeasureStatus();

  if (isMeasurementReady()) {
    Serial.println();
    Serial.println("[SYSTEM] Da do xong nhiet do va nhip tim.");
    Serial.println("[SYSTEM] Dang gui goi tin len MQTT server...");

    publishVitalsToServer();

    measureState = PUBLISH_DONE;
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

// ==================================================
// SETUP
// ==================================================
void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("====================================");
  Serial.println(" MedPulse ESP32 Gateway");
  Serial.println(" RC522 + MAX30102 + MLX90614 + MQTT");
  Serial.println("====================================");

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);

  Serial.println("[MAX30102] Init...");
  if (!max30102_init()) {
    Serial.println("[ERROR] MAX30102 init failed!");
    while (1) delay(1000);
  }
  Serial.println("[MAX30102] OK");

  Serial.println("[MLX90614] Init...");
  if (!mlx.begin()) {
    Serial.println("[ERROR] MLX90614 init failed!");
    while (1) delay(1000);
  }
  Serial.println("[MLX90614] OK");

  Serial.println("[RC522] Init...");
  SPI.begin(RC522_SCK_PIN, RC522_MISO_PIN, RC522_MOSI_PIN, RC522_SS_PIN);
  rfid.PCD_Init();

  byte version = rfid.PCD_ReadRegister(MFRC522::VersionReg);
  Serial.printf("[RC522] VersionReg = 0x%02X\n", version);

  if (version == 0x00 || version == 0xFF) {
    Serial.println("[WARNING] RC522 co the sai day SPI hoac mat nguon.");
  } else {
    Serial.println("[RC522] OK");
  }

  connectWiFi();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  ensureMQTT();

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

  handleRFID();
  handleMeasuring();
  handlePublishDone();

  delay(10);
}