/**
 * MedPulse IoT — THIẾT BỊ ĐEO BỆNH NHÂN
 * ══════════════════════════════════════════════════
 * Hardware : ESP32-C3 Mini + MPU6050
 * Chức năng:
 *   1. MPU6050 — phát hiện té ngã (state machine 4 giai đoạn)
 *   2. WiFi + MQTT — publish lên HiveMQ → Dashboard
 *
 * Patient ID cố định vì thiết bị theo người:
 *   → Đổi PATIENT_ID thành ID thẻ RFID của bệnh nhân
 *
 * MQTT Topics:
 *   medpulse/health/{id}/fall
 *   medpulse/health/{id}/signal_lost
 *   medpulse/health/{id}/battery
 *   medpulse/health/{id}/safe
 * ══════════════════════════════════════════════════
 */

#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include "MPU6050.h"

// ─────────────────────────────────────────────────
// CONFIG — ĐỔI THEO BỆNH NHÂN
// ─────────────────────────────────────────────────
#define PATIENT_ID    "RFID-1001"   // ← đổi ID tương ứng với thẻ RFID ở trạm

// ─────────────────────────────────────────────────
// WiFi & MQTT
// ─────────────────────────────────────────────────
#define WIFI_SSID     "Wokwi-GUEST"
#define WIFI_PASSWORD ""

#define MQTT_BROKER   "broker.hivemq.com"
#define MQTT_PORT     1883
#define MQTT_CLIENT   "medpulse-thietbi-wokwi"

// ─────────────────────────────────────────────────
// PIN CONFIG
// ─────────────────────────────────────────────────
#define SDA_PIN    8
#define SCL_PIN    9
#define INT_PIN    4
#define ALERT_PIN  10

// ─────────────────────────────────────────────────
// FALL DETECTION THRESHOLDS
// ─────────────────────────────────────────────────
#define FALL_ACCEL_LOW_THRESHOLD   0.5f
#define FALL_ACCEL_HIGH_THRESHOLD  1.8f
#define IMPACT_THRESHOLD           2.2f
#define MOTIONLESS_ACCEL_DELTA     0.12f
#define MOTIONLESS_GYRO_DELTA      10.0f
#define FALL_DURATION_MS           80
#define IMPACT_WINDOW_MS           3000
#define MOTIONLESS_DURATION_MS     2000
#define ALERT_DURATION_MS          5000
#define SAMPLE_RATE_MS             10

// Publish heartbeat mỗi 3 giây kể cả khi không té ngã
#define HEARTBEAT_INTERVAL_MS      3000

// ─────────────────────────────────────────────────
// STATE MACHINE
// ─────────────────────────────────────────────────
enum FallState {
  STATE_IDLE,
  STATE_FALLING,
  STATE_IMPACT,
  STATE_MOTIONLESS,
  STATE_ALERT
};

// ─────────────────────────────────────────────────
// STRUCTS
// ─────────────────────────────────────────────────
struct SensorData {
  float ax, ay, az;
  float gx, gy, gz;
  float accelMagnitude;
  unsigned long timestamp;
};

struct MotionStats {
  float accelDelta;
  float gyroDelta;
};

// ─────────────────────────────────────────────────
// GLOBALS
// ─────────────────────────────────────────────────
WiFiClient   espClient;
PubSubClient mqtt(espClient);
MPU6050      mpu;

SensorData   currentData;
SensorData   prevData;
MotionStats  motionStats;
FallState    currentState    = STATE_IDLE;

volatile bool intTriggered   = false;
unsigned long stateEntryTime = 0;
unsigned long fallDetectedTime = 0;
unsigned long impactTime     = 0;
unsigned long motionlessStart = 0;
unsigned long lastSampleTime = 0;
unsigned long lastHeartbeat  = 0;
bool          alertActive    = false;
int           fallCount      = 0;
int           currentFallStatus = 0;   // 0 = bình thường, 1 = té ngã

// ─────────────────────────────────────────────────
// ISR
// ─────────────────────────────────────────────────
void IRAM_ATTR mpuISR() {
  intTriggered = true;
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

void publishValue(const char* metric, float value) {
  if (!mqtt.connected()) connectMqtt();
  String topic   = "medpulse/health/" + String(PATIENT_ID) + "/" + metric;
  String payload = String(value, 0);
  mqtt.publish(topic.c_str(), payload.c_str(), true);
}

// ─────────────────────────────────────────────────
// SENSOR
// ─────────────────────────────────────────────────
void readSensor() {
  int16_t ax_raw, ay_raw, az_raw, gx_raw, gy_raw, gz_raw;
  mpu.getMotion6(&ax_raw, &ay_raw, &az_raw, &gx_raw, &gy_raw, &gz_raw);
  currentData.ax = ax_raw / 8192.0f;
  currentData.ay = ay_raw / 8192.0f;
  currentData.az = az_raw / 8192.0f;
  currentData.gx = gx_raw / 65.5f;
  currentData.gy = gy_raw / 65.5f;
  currentData.gz = gz_raw / 65.5f;
  currentData.accelMagnitude = sqrt(
    currentData.ax * currentData.ax +
    currentData.ay * currentData.ay +
    currentData.az * currentData.az
  );
  currentData.timestamp = millis();
}

float magnitude(float x, float y, float z) {
  return sqrt(x*x + y*y + z*z);
}

// ─────────────────────────────────────────────────
// FALL DETECTION LOGIC
// ─────────────────────────────────────────────────
bool detectFall() {
  float mag = currentData.accelMagnitude;
  if (mag < FALL_ACCEL_LOW_THRESHOLD) return true;
  if (mag > FALL_ACCEL_HIGH_THRESHOLD && motionStats.gyroDelta > 40) return true;
  return false;
}

bool detectImpact() {
  return currentData.accelMagnitude > IMPACT_THRESHOLD;
}

bool detectMotionless() {
  return motionStats.accelDelta < MOTIONLESS_ACCEL_DELTA &&
         motionStats.gyroDelta  < MOTIONLESS_GYRO_DELTA;
}

bool detectRecovery() {
  return currentData.accelMagnitude > 0.8f &&
         currentData.accelMagnitude < 1.3f &&
         motionStats.gyroDelta > 40;
}

// ─────────────────────────────────────────────────
// ALERT
// ─────────────────────────────────────────────────
void triggerAlert() {
  fallCount++;
  alertActive       = true;
  currentFallStatus = 1;
  digitalWrite(ALERT_PIN, HIGH);

  Serial.println("\n████████████████████████████");
  Serial.printf(" TÉ NGÃ XÁC NHẬN (#%d)\n", fallCount);
  Serial.println(" GỬI CẢNH BÁO KHẨN CẤP");
  Serial.println("████████████████████████████\n");

  // Publish ngay lập tức khi có té ngã
  publishValue("fall",        1.0);
  publishValue("signal_lost", 0.0);
  publishValue("safe",        1.0);
  publishValue("battery",     75.0);
  Serial.println("[MQTT] Đã gửi fall=1 lên dashboard");
}

void resetStateMachine() {
  currentState      = STATE_IDLE;
  stateEntryTime    = 0;
  fallDetectedTime  = 0;
  impactTime        = 0;
  motionlessStart   = 0;
  alertActive       = false;
  currentFallStatus = 0;
  Serial.println("[SYSTEM] RESET — Trạng thái bình thường\n");
}

// ─────────────────────────────────────────────────
// STATE MACHINE
// ─────────────────────────────────────────────────
void stateMachine() {
  unsigned long now = millis();

  switch (currentState) {

  case STATE_IDLE:
    if (detectFall()) {
      currentState   = STATE_FALLING;
      stateEntryTime = now;
      fallDetectedTime = now;
      Serial.println("[GĐ 1] Phát hiện rơi tự do / chuyển động bất thường");
    }
    break;

  case STATE_FALLING:
    if (now - stateEntryTime >= FALL_DURATION_MS) {
      currentState   = STATE_IMPACT;
      stateEntryTime = now;
      Serial.println("[GĐ 2] Chờ va chạm...");
    }
    if (currentData.accelMagnitude > 0.8f &&
        currentData.accelMagnitude < 1.3f &&
        motionStats.gyroDelta < 20) {
      Serial.println("[GĐ 1] Báo động giả — bỏ qua");
      currentState = STATE_IDLE;
    }
    break;

  case STATE_IMPACT:
    if (detectImpact()) {
      currentState    = STATE_MOTIONLESS;
      impactTime      = now;
      motionlessStart = now;
      stateEntryTime  = now;
      Serial.println("[GĐ 2] Va chạm phát hiện!");
    } else if (now - stateEntryTime > IMPACT_WINDOW_MS) {
      Serial.println("[GĐ 2] Hết thời gian chờ va chạm");
      currentState = STATE_IDLE;
    }
    break;

  case STATE_MOTIONLESS:
    if (detectMotionless()) {
      if (now - motionlessStart >= MOTIONLESS_DURATION_MS) {
        currentState = STATE_ALERT;
        triggerAlert();
      }
    } else {
      motionlessStart = now;
      if (detectRecovery()) {
        Serial.println("[PHỤC HỒI] Bệnh nhân tự đứng dậy");
        currentState = STATE_IDLE;
      }
    }
    if (now - stateEntryTime > 8000) {
      Serial.println("[GĐ 3] Hết thời gian — Reset");
      currentState = STATE_IDLE;
    }
    break;

  case STATE_ALERT:
    if (detectRecovery()) {
      Serial.println("[PHỤC HỒI] Bệnh nhân đứng dậy sau cảnh báo");
      digitalWrite(ALERT_PIN, LOW);
      alertActive = false;
      // Publish fall=0 khi phục hồi
      publishValue("fall", 0.0);
      Serial.println("[MQTT] Đã gửi fall=0 (phục hồi)");
      resetStateMachine();
    }
    break;
  }
}

// ─────────────────────────────────────────────────
// DEBUG PRINT
// ─────────────────────────────────────────────────
void printStatus() {
  static unsigned long lastPrint = 0;
  if (millis() - lastPrint < 500) return;
  lastPrint = millis();
  const char* stateNames[] = {"IDLE","FALLING","IMPACT","MOTIONLESS","ALERT"};
  Serial.printf("ACC:%.2fg ΔA:%.2f ΔG:%.2f STATE:%s FALL:%d\n",
    currentData.accelMagnitude,
    motionStats.accelDelta,
    motionStats.gyroDelta,
    stateNames[currentState],
    currentFallStatus
  );
}

// ─────────────────────────────────────────────────
// SETUP
// ─────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n══════════════════════════════════════");
  Serial.println("  MedPulse — THIẾT BỊ ĐEO BỆNH NHÂN  ");
  Serial.printf ("  Patient ID: %s\n", PATIENT_ID);
  Serial.println("══════════════════════════════════════");

  pinMode(ALERT_PIN, OUTPUT);
  digitalWrite(ALERT_PIN, LOW);
  pinMode(INT_PIN, INPUT);
  attachInterrupt(digitalPinToInterrupt(INT_PIN), mpuISR, RISING);

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(400000);

  Serial.print("[MPU6050] Init... ");
  mpu.initialize();
  if (!mpu.testConnection()) {
    Serial.println("FAILED!");
    while (1) { digitalWrite(ALERT_PIN, !digitalRead(ALERT_PIN)); delay(200); }
  }
  Serial.println("OK");

  mpu.setFullScaleAccelRange(MPU6050_ACCEL_FS_4);
  mpu.setFullScaleGyroRange(MPU6050_GYRO_FS_500);
  mpu.setDLPFMode(MPU6050_DLPF_BW_42);
  mpu.setIntDataReadyEnabled(true);
  delay(100);
  readSensor();
  prevData = currentData;

  connectWifi();
  connectMqtt();

  // Gửi trạng thái ban đầu
  publishValue("fall",        0.0);
  publishValue("signal_lost", 0.0);
  publishValue("safe",        1.0);
  publishValue("battery",     80.0);

  Serial.println("[SYSTEM] Sẵn sàng giám sát té ngã.");
  Serial.println("──────────────────────────────────────");
}

// ─────────────────────────────────────────────────
// LOOP
// ─────────────────────────────────────────────────
void loop() {
  mqtt.loop();

  unsigned long now = millis();

  // ── Sample MPU6050 ────────────────────────────
  bool shouldSample = intTriggered;
  if (intTriggered) intTriggered = false;
  if (now - lastSampleTime >= SAMPLE_RATE_MS) shouldSample = true;
  if (!shouldSample) return;
  lastSampleTime = now;

  readSensor();

  motionStats.accelDelta = abs(currentData.accelMagnitude - prevData.accelMagnitude);
  float gyroCur  = magnitude(currentData.gx, currentData.gy, currentData.gz);
  float gyroPrev = magnitude(prevData.gx, prevData.gy, prevData.gz);
  motionStats.gyroDelta = abs(gyroCur - gyroPrev);

  printStatus();
  stateMachine();

  // ── Alert timeout ─────────────────────────────
  if (alertActive && (now - impactTime > ALERT_DURATION_MS)) {
    Serial.println("[ALERT] Hết thời gian — không có phản hồi");
    digitalWrite(ALERT_PIN, LOW);
    alertActive = false;
    resetStateMachine();
  }

  // ── Heartbeat publish (trạng thái bình thường) ─
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS && !alertActive) {
    lastHeartbeat = now;
    publishValue("fall",        0.0);
    publishValue("signal_lost", 0.0);
    publishValue("safe",        1.0);
    publishValue("battery",     80.0);
  }

  prevData = currentData;
}
