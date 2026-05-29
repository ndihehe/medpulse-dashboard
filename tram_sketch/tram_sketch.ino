/**
 * MedPulse IoT — TRẠM GIƯỜNG BỆNH
 * ══════════════════════════════════════════════════
 * Hardware : ESP32 + RC522 + MAX30102 + MLX90614
 * Chức năng:
 *   1. RC522  — quét thẻ RFID → lấy patient_id
 *   2. MAX30102 — đo nhịp tim (BPM) + SpO2
 *   3. MLX90614 — đo nhiệt độ cơ thể (°C)
 *   4. WiFi + MQTT — publish lên HiveMQ → Dashboard
 *
 * MQTT Topics (thay RFID-1001 bằng ID thẻ thật):
 *   medpulse/health/{id}/heart_rate
 *   medpulse/health/{id}/spo2
 *   medpulse/health/{id}/temp
 *   medpulse/health/{id}/battery
 *   medpulse/health/{id}/safe
 *   medpulse/health/{id}/signal_lost
 * ══════════════════════════════════════════════════
 */

#include <Wire.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_MLX90614.h>
#include <MFRC522.h>
#include <SPI.h>

// ─────────────────────────────────────────────────
// WiFi & MQTT
// ─────────────────────────────────────────────────
#define WIFI_SSID     "Wokwi-GUEST"
#define WIFI_PASSWORD ""

#define MQTT_BROKER   "broker.hivemq.com"
#define MQTT_PORT     1883
#define MQTT_CLIENT   "medpulse-tram-wokwi"

// ─────────────────────────────────────────────────
// PIN CONFIG
// ─────────────────────────────────────────────────
// I2C (MAX30102 + MLX90614)
#define I2C_SDA  21
#define I2C_SCL  22

// SPI (RC522)
#define RC522_SS   5
#define RC522_RST  27

// ─────────────────────────────────────────────────
// MAX30102 REGISTERS
// ─────────────────────────────────────────────────
#define MAX30102_ADDR    0x57
#define REG_FIFO_WR_PTR  0x04
#define REG_OVR_COUNTER  0x05
#define REG_FIFO_RD_PTR  0x06
#define REG_FIFO_DATA    0x07
#define REG_MODE_CONFIG  0x09
#define REG_SPO2_CONFIG  0x0A
#define REG_LED1_PA      0x0C
#define REG_LED2_PA      0x0D
#define REG_PART_ID      0xFF

// ─────────────────────────────────────────────────
// OBJECTS
// ─────────────────────────────────────────────────
WiFiClient       espClient;
PubSubClient     mqtt(espClient);
Adafruit_MLX90614 mlx;
MFRC522          rfid(RC522_SS, RC522_RST);

// ─────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────
String  currentPatientId = "";
bool    patientPresent   = false;

// Beat detection
const byte RATE_SIZE = 4;
byte  rates[RATE_SIZE];
byte  rateSpot  = 0;
long  lastBeat  = 0;
int   beatAvg   = 0;
long  prevIR    = 0;
bool  rising    = false;

unsigned long lastPublish   = 0;
unsigned long lastRfidCheck = 0;
unsigned long patientSeenAt = 0;

#define PUBLISH_INTERVAL_MS   2000
#define RFID_CHECK_INTERVAL   1000
#define PATIENT_TIMEOUT_MS   30000   // 30s không quét lại → coi là rời đi

// ─────────────────────────────────────────────────
// MAX30102 HELPERS
// ─────────────────────────────────────────────────
void writeReg(uint8_t reg, uint8_t val) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission();
}

uint8_t readReg(uint8_t reg) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.endTransmission();
  Wire.requestFrom(MAX30102_ADDR, 1);
  return Wire.available() ? Wire.read() : 0;
}

void readBytes(uint8_t reg, uint8_t *buf, uint8_t len) {
  Wire.beginTransmission(MAX30102_ADDR);
  Wire.write(reg);
  Wire.endTransmission();
  Wire.requestFrom(MAX30102_ADDR, (int)len);
  for (uint8_t i = 0; i < len && Wire.available(); i++) buf[i] = Wire.read();
}

bool max30102_init() {
  uint8_t id = readReg(REG_PART_ID);
  Serial.printf("[MAX30102] Part ID: 0x%02X\n", id);
  if (id != 0x15) return false;
  writeReg(REG_MODE_CONFIG, 0x40); delay(100);
  writeReg(REG_MODE_CONFIG, 0x03);
  writeReg(REG_SPO2_CONFIG, 0x27);
  writeReg(REG_LED1_PA,     0x1F);
  writeReg(REG_LED2_PA,     0x1F);
  writeReg(REG_FIFO_WR_PTR, 0x00);
  writeReg(REG_OVR_COUNTER, 0x00);
  writeReg(REG_FIFO_RD_PTR, 0x00);
  return true;
}

long max30102_readIR() {
  uint8_t wr = readReg(REG_FIFO_WR_PTR);
  uint8_t rd = readReg(REG_FIFO_RD_PTR);
  if (wr == rd) return -1;
  uint8_t buf[6];
  readBytes(REG_FIFO_DATA, buf, 6);
  writeReg(REG_FIFO_RD_PTR, (rd + 1) & 0x1F);
  long ir = ((long)(buf[3] & 0x03) << 16) | ((long)buf[4] << 8) | buf[5];
  return ir;
}

// ─────────────────────────────────────────────────
// BEAT DETECTION
// ─────────────────────────────────────────────────
void detectBeat(long ir) {
  if (ir < 0) return;
  bool nowRising = (ir > prevIR);
  if (!nowRising && rising && ir > 50000) {
    long now   = millis();
    long delta = now - lastBeat;
    lastBeat   = now;
    if (delta > 250 && delta < 3000) {
      float bpm = 60000.0f / delta;
      rates[rateSpot++] = (byte)bpm;
      rateSpot %= RATE_SIZE;
      beatAvg = 0;
      for (byte i = 0; i < RATE_SIZE; i++) beatAvg += rates[i];
      beatAvg /= RATE_SIZE;
    }
  }
  rising = nowRising;
  prevIR = ir;
}

// Ước tính SpO2 đơn giản (demo)
int estimateSpO2(int bpm) {
  if (bpm == 0) return 0;
  if (bpm >= 60 && bpm <= 100) return random(96, 100);
  if (bpm > 100) return random(93, 97);
  return random(90, 95);
}

// ─────────────────────────────────────────────────
// RFID
// ─────────────────────────────────────────────────
String readRfidUID() {
  if (!rfid.PICC_IsNewCardPresent()) return "";
  if (!rfid.PICC_ReadCardSerial())   return "";

  // Map UID bytes → RFID-100X (1001..1016)
  // Dùng byte cuối của UID để chọn slot 1-16
  uint8_t slot = (rfid.uid.uidByte[rfid.uid.size - 1] % 16) + 1;
  String pid = "RFID-10";
  if (slot < 10) pid += "0";
  pid += String(slot);

  rfid.PICC_HaltA();
  rfid.PCD_StopCrypto1();
  return pid;
}

// ─────────────────────────────────────────────────
// WIFI + MQTT
// ─────────────────────────────────────────────────
void connectWifi() {
  Serial.print("[WiFi] Connecting");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500); Serial.print(".");
  }
  Serial.println("\n[WiFi] Connected: " + WiFi.localIP().toString());
}

void connectMqtt() {
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  while (!mqtt.connected()) {
    Serial.print("[MQTT] Connecting...");
    if (mqtt.connect(MQTT_CLIENT)) {
      Serial.println(" OK");
    } else {
      Serial.printf(" FAILED (%d), retry 3s\n", mqtt.state());
      delay(3000);
    }
  }
}

void publishValue(String patientId, const char* metric, float value) {
  if (!mqtt.connected()) connectMqtt();
  String topic = "medpulse/health/" + patientId + "/" + metric;
  String payload = String(value, 1);
  mqtt.publish(topic.c_str(), payload.c_str(), true);
  Serial.printf("  → %s = %s\n", topic.c_str(), payload.c_str());
}

// ─────────────────────────────────────────────────
// SETUP
// ─────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n══════════════════════════════");
  Serial.println("  MedPulse — TRẠM GIƯỜNG BỆNH ");
  Serial.println("══════════════════════════════");

  // I2C
  Wire.begin(I2C_SDA, I2C_SCL);

  // MAX30102
  Serial.print("[MAX30102] Init... ");
  if (!max30102_init()) {
    Serial.println("FAILED — kiểm tra kết nối!");
  } else {
    Serial.println("OK");
  }

  // MLX90614
  Serial.print("[MLX90614] Init... ");
  if (!mlx.begin()) {
    Serial.println("FAILED — kiểm tra kết nối!");
  } else {
    Serial.println("OK");
  }

  // RC522
  SPI.begin();
  rfid.PCD_Init();
  Serial.println("[RC522]    OK — chờ quét thẻ...");

  // WiFi + MQTT
  connectWifi();
  connectMqtt();

  Serial.println("\n[SYSTEM] Sẵn sàng. Quét thẻ bệnh nhân để bắt đầu.");
  Serial.println("──────────────────────────────");
}

// ─────────────────────────────────────────────────
// LOOP
// ─────────────────────────────────────────────────
void loop() {
  mqtt.loop();

  unsigned long now = millis();

  // ── RFID: kiểm tra thẻ ────────────────────────
  if (now - lastRfidCheck >= RFID_CHECK_INTERVAL) {
    lastRfidCheck = now;
    String uid = readRfidUID();
    if (uid != "") {
      if (uid != currentPatientId) {
        Serial.println("\n[RFID] Thẻ mới: " + uid);
        currentPatientId = uid;
        patientPresent   = true;
        // Reset beat data khi có bệnh nhân mới
        beatAvg   = 0;
        lastBeat  = 0;
        rateSpot  = 0;
        memset(rates, 0, sizeof(rates));
      }
      patientSeenAt = now;
      patientPresent = true;
    }
  }

  // Timeout: không quét thẻ quá 30s → bệnh nhân rời đi
  if (patientPresent && currentPatientId != "") {
    if (now - patientSeenAt > PATIENT_TIMEOUT_MS) {
      Serial.println("[RFID] Bệnh nhân " + currentPatientId + " đã rời đi.");
      // Publish signal_lost = 1
      publishValue(currentPatientId, "signal_lost", 1.0);
      publishValue(currentPatientId, "safe", 0.0);
      currentPatientId = "";
      patientPresent   = false;
    }
  }

  // ── MAX30102: đọc liên tục ─────────────────────
  long ir = max30102_readIR();
  detectBeat(ir);

  // ── Publish theo chu kỳ ───────────────────────
  if (patientPresent && currentPatientId != "") {
    if (now - lastPublish >= PUBLISH_INTERVAL_MS) {
      lastPublish = now;

      float temp  = mlx.readObjectTempC();
      int   bpm   = beatAvg;
      int   spo2  = estimateSpO2(bpm);

      Serial.println("\n[PUBLISH] " + currentPatientId);
      publishValue(currentPatientId, "heart_rate",  (float)bpm);
      publishValue(currentPatientId, "spo2",        (float)spo2);
      publishValue(currentPatientId, "temp",        temp);
      publishValue(currentPatientId, "battery",     85.0);
      publishValue(currentPatientId, "safe",        1.0);
      publishValue(currentPatientId, "signal_lost", 0.0);

      // Log trạng thái
      Serial.printf("  BPM=%d | SpO2=%d%% | Temp=%.1f°C\n", bpm, spo2, temp);
      if (bpm == 0)        Serial.println("  [!] Chờ đủ dữ liệu nhịp tim...");
      else if (bpm < 60)   Serial.println("  [!] CẢNH BÁO: Nhịp tim CHẬM");
      else if (bpm > 100)  Serial.println("  [!] CẢNH BÁO: Nhịp tim NHANH");
      else                 Serial.println("  [OK] Nhịp tim bình thường");
      if (temp >= 38.5f)   Serial.println("  [!] CẢNH BÁO: SỐT CAO");
      else if (temp >= 37.5f) Serial.println("  [!] CẢNH BÁO: Sốt nhẹ");
    }
  }
}
