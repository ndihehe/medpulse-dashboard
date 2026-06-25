/**
 * MedPulse — THIET BI DEO BENH NHAN (CHI PHAN CHUC NANG RIENG CUA THE DEO)
 * ======================================================
 * Hardware : ESP32-C3 Super Mini + MPU6050 (GY-521 8 chan) + Buzzer active-LOW
 *            (dung theo MedPulse_Hardware_Guide.docx)
 *
 * MUC TIEU CUA BAN NAY:
 *   Chi tap trung lam dung 2 chuc nang thuoc rieng ve thiet bi deo:
 *     1) Thuat toan phat hien te nga 4 giai doan qua MPU6050.
 *     2) Kich hoat buzzer 2 tang:
 *        - Sau khi phat hien va cham nghi te nga: keu 1 tieng TIT ngan.
 *        - Neu sau do nam bat dong du 30s: phat coi theo tin hieu SOS lap lien tuc.
 *        - Chi huy/tat canh bao khi co chuyen dong PHUC HOI du lau
 *          nhu nguoi te dang nam roi ngoi day/dung day/di.
 *   Hai chuc nang nay PHAI chay day du va dung ngay bay gio, khong phu
 *   thuoc vao bat ky lop ket noi/giao tiep nao.
 *
 * PHAN KET NOI / GIAO TIEP:
 *   BLE Advertising + GATT la kenh uu tien. Neu mat GATT, thiet bi tu bat
 *   Wi-Fi/MQTT de gui su kien va nhan lenh truc tiep tu backend.
 *   Cac diem tich hop nghiep vu:
 *     - initConnectivity()         -> goi 1 lan trong setup()
 *     - sendAlertEvent(bool)       -> goi khi te nga duoc xac nhan / khi
 *                                      benh nhan phuc hoi sau canh bao
 *     - sendPresenceHeartbeat()    -> goi dinh ky khi dang IDLE (de sau
 *                                      nay bao "thiet bi con song" qua
 *                                      BLE/MQTT)
 *   Phan ket noi khong thay doi nguong hay state machine te nga.
 *
 * Sơ đồ chân (theo Hardware Guide):
 *   SDA=8, SCL=9 (pull-up 4.7k lên 3.3V), INT=4, Buzzer Signal=5 (LOW=kêu)
 * ======================================================
 */

#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <esp_wifi.h>
#include <esp_coexist.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLE2902.h>
#include <esp_arduino_version.h>
#include "MPU6050.h"
#include "device_config.h"
#include "medpulse_protocol.h"

// ─────────────────────────────────────────────────
// PIN CONFIG — đúng theo MedPulse_Hardware_Guide.docx
// ─────────────────────────────────────────────────
#define SDA_PIN     8   // GY-521 SDA
#define SCL_PIN     9   // GY-521 SCL
#define INT_PIN     4   // GY-521 INT (ngắt MPU6050)
#define BUZZER_PIN  5   // Buzzer module — KÍCH MỨC LOW (ngược buzzer thường)

// Buzzer module có board mạch riêng, tích hợp transistor sẵn → không cần
// transistor/điện trở ngoài. Logic: LOW = kêu, HIGH = tắt.
#define BUZZER_ACTIVE_LEVEL   LOW
#define BUZZER_INACTIVE_LEVEL HIGH

// ─────────────────────────────────────────────────
// FALL DETECTION THRESHOLDS (giữ nguyên thuật toán gốc — không đổi)
// ─────────────────────────────────────────────────
#define FALL_ACCEL_LOW_THRESHOLD   0.5f
#define FALL_ACCEL_HIGH_THRESHOLD  1.8f
#define IMPACT_THRESHOLD           2.2f
#define MOTIONLESS_ACCEL_DELTA     0.12f
#define MOTIONLESS_GYRO_DELTA      10.0f
#define FALL_DURATION_MS           80
#define IMPACT_WINDOW_MS           3000

// Sau va cham, phai nam bat dong lien tuc 30 giay moi xac nhan te nga.
#define MOTIONLESS_DURATION_MS     30000UL

// Phan biet 2 loai:
// 1) Mat bat dong / rung ngan: chi dem lai 30s, KHONG ve IDLE ngay.
// 2) Chuyen dong phuc hoi du lau: nguoi te nga ngoi day/dung day/di -> moi ve IDLE.
// Luu y: khong chi dung accelMagnitude vi khi xoay/lac, tong gia toc van co the gan 1g.
// Vi vay can them accelVectorDelta va gyroAbs de bat duoc chuyen dong that.
#define MOTIONLESS_VECTOR_DELTA    0.06f
#define MOTIONLESS_GYRO_ABS        12.0f
#define MOVEMENT_VECTOR_DELTA      0.10f
#define MOVEMENT_GYRO_DELTA        12.0f
#define MOVEMENT_GYRO_ABS          25.0f
#define STRONG_MOVEMENT_VECTOR     0.18f
#define STRONG_MOVEMENT_GYRO_ABS   60.0f

// Muon huy canh bao / ve IDLE thi phai co chuyen dong keo dai,
// giong hanh vi thuc te: nam -> ngoi day -> dung day/di.
// Khong dung chuyen dong ngan 100-300ms de huy nua.
#define RECOVERY_MOVEMENT_CONFIRM_MS  3500UL
#define RECOVERY_LOST_TOLERANCE_MS    700UL
#define RECOVERY_VECTOR_DELTA         0.08f
#define RECOVERY_GYRO_ABS             20.0f
#define RECOVERY_ACCEL_LOW            0.78f
#define RECOVERY_ACCEL_HIGH           1.32f

// Sau va cham co mot khoang on dinh ngan: bo qua rung do va cham + tieng TIT ban dau.
// Neu khong co khoang nay, 1-2 mau du lieu nhieu ngay sau va cham co the lam state quay ve IDLE.
#define POST_IMPACT_GRACE_MS        1500UL

// Khong tu bo case chi vi sau va cham bi rung/khong on dinh.
// Neu nguoi nam yen: se dem du 30s roi bao SOS.
// Neu nguoi that su hoi phuc: phai chuyen dong du lau moi ve IDLE.

// Neu dang dem bat dong ma bi mat dieu kien bat dong trong thoi gian ngan,
// coi la nhieu/rung tam thoi va dem lai, KHONG huy ngay ve IDLE.
#define STILLNESS_LOST_TOLERANCE_MS 800UL

// Bo qua 1 giay dau sau khi buzzer bat de tranh rung nhe luc vua kich coi.
#define ALERT_IGNORE_MOVEMENT_MS   1000UL

// Buzzer 2 tang:
// 1) PRE-ALERT: 1 tieng TIT ngan sau khi phat hien va cham nghi te nga.
// 2) ALERT: phat SOS lap lien tuc sau khi bat dong du 30 giay.
#define PREALERT_BEEP_MS           160UL
#define SOS_DOT_MS                 180UL
#define SOS_DASH_MS                540UL
#define SOS_SYMBOL_GAP_MS          180UL
#define SOS_LETTER_GAP_MS          540UL
#define SOS_CYCLE_GAP_MS           1400UL

#define SAMPLE_RATE_MS             10

// ─────────────────────────────────────────────────
// STATE MACHINE (4 giai đoạn: fall -> impact -> bất động 30s -> alert)
// ─────────────────────────────────────────────────
enum FallState {
  STATE_IDLE,
  STATE_FALLING,
  STATE_IMPACT,
  STATE_MOTIONLESS,
  STATE_ALERT
};

enum BuzzerMode {
  BUZZER_SILENT,
  BUZZER_PREALERT_BEEP,
  BUZZER_SOS,
  BUZZER_REMOTE_TEST
};

struct SensorData {
  float ax, ay, az;
  float gx, gy, gz;
  float accelMagnitude;
};

struct MotionStats {
  float accelDelta;        // do lech do lon gia toc |mag_now - mag_prev|
  float accelVectorDelta;  // do lech vector gia toc: bat duoc xoay/lac du tong van gan 1g
  float gyroDelta;
  float gyroAbs;
};

// ─────────────────────────────────────────────────
// GLOBALS
// ─────────────────────────────────────────────────
MPU6050      mpu;
bool         mpuReady = false;
unsigned long lastMpuHealthCheckMs = 0;
const unsigned long MPU_HEALTH_CHECK_INTERVAL_MS = 30000UL;

SensorData   currentData;
SensorData   prevData;
MotionStats  motionStats;
FallState    currentState     = STATE_IDLE;

volatile bool intTriggered    = false;
unsigned long stateEntryTime  = 0;
unsigned long impactTime      = 0;
unsigned long motionlessStart = 0;
unsigned long motionlessLostStart = 0;
unsigned long movementStart   = 0;
unsigned long recoveryStart   = 0;
unsigned long recoveryLastMotion = 0;
unsigned long alertStartTime  = 0;
unsigned long lastSampleTime  = 0;
bool          alertActive     = false;
int           fallCount       = 0;

BuzzerMode    buzzerMode      = BUZZER_SILENT;
unsigned long buzzerStepStart = 0;
uint8_t       sosStepIndex    = 0;

// Connectivity state. BLE/GATT la kenh uu tien; Wi-Fi/MQTT chi bat khi mat GATT.
WiFiClient deviceWifiClient;
PubSubClient deviceMqtt(deviceWifiClient);
Preferences commandPreferences;
BLEServer* medpulseBleServer = nullptr;
BLECharacteristic* medpulseEventCharacteristic = nullptr;
BLEAdvertising* medpulseAdvertising = nullptr;
volatile bool gattConnected = false;
volatile bool gattWasConnected = false;
unsigned long lastGattDisconnectedAtMs = 0;
unsigned long lastWifiAttemptMs = 0;
unsigned long wifiAttemptStartedAtMs = 0;
unsigned long wifiScanStartedAtMs = 0;
volatile unsigned long wifiCycleStartedAtMs = 0;
bool wifiScanActive = false;
volatile bool wifiConnectionAttemptActive = false;
volatile bool wifiConnectionAttemptFailed = false;
volatile uint8_t wifiLastDisconnectReason = 0;
volatile unsigned long wifiLastDisconnectAtMs = 0;
volatile int8_t wifiLastDisconnectRssi = 0;
volatile uint16_t wifiDisconnectEventCount = 0;
volatile uint32_t wifiAttemptSequence = 0;
unsigned long lastMqttAttemptMs = 0;
unsigned long lastDirectStatusMs = 0;
unsigned long remoteBuzzerStopAtMs = 0;
uint8_t eventSequence = 0;

struct PendingConnectivityEvent {
  uint8_t type;
  uint8_t sequence;
  int8_t battery;
  uint8_t flags;
  uint32_t deviceUptimeSeconds;
};

struct PendingDeviceCommand {
  uint32_t token;
  uint8_t action;
  uint32_t durationMs;
  bool viaMqtt;
  char commandId[65];
};

QueueHandle_t connectivityEventQueue = nullptr;
QueueHandle_t deviceCommandQueue = nullptr;
static constexpr uint8_t COMMAND_TOKEN_HISTORY_SIZE = 8;
uint32_t commandTokenHistory[COMMAND_TOKEN_HISTORY_SIZE] = {};
uint8_t commandTokenHistoryIndex = 0;

// SOS = ... --- ...
// Mang ben duoi luan phien ON/OFF: index chan = bat coi, index le = tat coi.
const uint16_t SOS_PATTERN_MS[] = {
  SOS_DOT_MS,  SOS_SYMBOL_GAP_MS,
  SOS_DOT_MS,  SOS_SYMBOL_GAP_MS,
  SOS_DOT_MS,  SOS_LETTER_GAP_MS,

  SOS_DASH_MS, SOS_SYMBOL_GAP_MS,
  SOS_DASH_MS, SOS_SYMBOL_GAP_MS,
  SOS_DASH_MS, SOS_LETTER_GAP_MS,

  SOS_DOT_MS,  SOS_SYMBOL_GAP_MS,
  SOS_DOT_MS,  SOS_SYMBOL_GAP_MS,
  SOS_DOT_MS,  SOS_CYCLE_GAP_MS
};
const uint8_t SOS_PATTERN_STEPS = sizeof(SOS_PATTERN_MS) / sizeof(SOS_PATTERN_MS[0]);

// ─────────────────────────────────────────────────
// ISR
// ─────────────────────────────────────────────────
void IRAM_ATTR mpuISR() {
  intTriggered = true;
}

// ─────────────────────────────────────────────────
// BUZZER
// ─────────────────────────────────────────────────
void buzzerOn()  { digitalWrite(BUZZER_PIN, BUZZER_ACTIVE_LEVEL); }
void buzzerOff() { digitalWrite(BUZZER_PIN, BUZZER_INACTIVE_LEVEL); }

void stopBuzzerPattern() {
  buzzerMode      = BUZZER_SILENT;
  buzzerStepStart = 0;
  sosStepIndex    = 0;
  buzzerOff();
}

void startPreAlertBeep() {
  // 1 tieng TIT ngan de bao thiet bi vua nghi ngo co te nga.
  // Chua gui alert that su o buoc nay.
  buzzerMode      = BUZZER_PREALERT_BEEP;
  buzzerStepStart = millis();
  sosStepIndex    = 0;
  buzzerOn();
}

void startSosBuzzer() {
  // Phat SOS lap lien tuc: ... --- ...
  buzzerMode      = BUZZER_SOS;
  buzzerStepStart = millis();
  sosStepIndex    = 0;
  buzzerOn();
}

void updateBuzzerPattern() {
  unsigned long now = millis();

  if (buzzerMode == BUZZER_SILENT) {
    return;
  }

  if (buzzerMode == BUZZER_PREALERT_BEEP) {
    if (now - buzzerStepStart >= PREALERT_BEEP_MS) {
      stopBuzzerPattern();
    } else {
      buzzerOn();
    }
    return;
  }

  if (buzzerMode == BUZZER_REMOTE_TEST) {
    buzzerOn();
    return;
  }

  if (buzzerMode == BUZZER_SOS) {
    while (now - buzzerStepStart >= SOS_PATTERN_MS[sosStepIndex]) {
      buzzerStepStart += SOS_PATTERN_MS[sosStepIndex];
      sosStepIndex = (sosStepIndex + 1) % SOS_PATTERN_STEPS;
    }

    if ((sosStepIndex % 2) == 0) {
      buzzerOn();
    } else {
      buzzerOff();
    }
  }
}

// ─────────────────────────────────────────────────
// KẾT NỐI / GIAO TIẾP MEDPULSE
// ─────────────────────────────────────────────────
#include "device_connectivity.h"

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
}

float magnitude(float x, float y, float z) {
  return sqrt(x * x + y * y + z * z);
}

float vectorDelta3(float x1, float y1, float z1, float x2, float y2, float z2) {
  float dx = x1 - x2;
  float dy = y1 - y2;
  float dz = z1 - z2;
  return sqrt(dx * dx + dy * dy + dz * dz);
}

// ─────────────────────────────────────────────────
// FALL DETECTION LOGIC (giữ nguyên thuật toán gốc)
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
  return motionStats.accelVectorDelta < MOTIONLESS_VECTOR_DELTA &&
         motionStats.gyroAbs          < MOTIONLESS_GYRO_ABS;
}

bool detectMovement() {
  return motionStats.accelVectorDelta >= MOVEMENT_VECTOR_DELTA ||
         motionStats.gyroDelta        >= MOVEMENT_GYRO_DELTA   ||
         motionStats.gyroAbs          >= MOVEMENT_GYRO_ABS;
}

bool detectStrongMovement() {
  return motionStats.accelVectorDelta >= STRONG_MOVEMENT_VECTOR ||
         motionStats.gyroAbs          >= STRONG_MOVEMENT_GYRO_ABS ||
         currentData.accelMagnitude   < 0.75f ||
         currentData.accelMagnitude   > 1.35f;
}

bool movementConfirmed(unsigned long now) {
  if (!detectMovement()) {
    movementStart = 0;
    return false;
  }

  if (movementStart == 0) {
    movementStart = now;
    return false;
  }

  // Ham nay chi de debug/tuong thich ban cu; logic chinh hien dung recoveryMovementConfirmed().
  return false;
}

void resetRecoveryTracking() {
  recoveryStart = 0;
  recoveryLastMotion = 0;
}

bool detectRecoveryMovementRaw() {
  // Chuyen dong phuc hoi: khong phai rung 1 mau, ma co dau hieu thay doi tu the/di chuyen.
  // Dung nguong vua phai de bat duoc hanh vi ngoi day/dung day/di,
  // nhung khong huy do rung nhe sau va cham hoặc rung do buzzer.
  return motionStats.accelVectorDelta >= RECOVERY_VECTOR_DELTA ||
         motionStats.gyroAbs          >= RECOVERY_GYRO_ABS     ||
         currentData.accelMagnitude   <  RECOVERY_ACCEL_LOW    ||
         currentData.accelMagnitude   >  RECOVERY_ACCEL_HIGH;
}

bool recoveryMovementConfirmed(unsigned long now) {
  bool moving = detectRecoveryMovementRaw();

  if (moving) {
    if (recoveryStart == 0) {
      recoveryStart = now;
      Serial.println("[PHUC HOI?] Phat hien chuyen dong — can duy tri vai giay moi ve IDLE");
    }
    recoveryLastMotion = now;
  } else {
    // Cho phep ngat nhip ngan khi nguoi dang ngoi day/dung day.
    // Neu dung yen qua lau thi coi nhu chua phuc hoi du.
    if (recoveryStart != 0 && now - recoveryLastMotion > RECOVERY_LOST_TOLERANCE_MS) {
      resetRecoveryTracking();
    }
  }

  return recoveryStart != 0 && (now - recoveryStart >= RECOVERY_MOVEMENT_CONFIRM_MS);
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
  alertActive    = true;
  alertStartTime = millis();
  movementStart  = 0;
  resetRecoveryTracking();
  startSosBuzzer();

  Serial.println("\n████████████████████████████");
  Serial.printf (" TE NGA XAC NHAN (#%d)\n", fallCount);
  Serial.println(" DA BAT DONG DU 30 GIAY");
  Serial.println(" BUZZER: PHAT TIN HIEU SOS LAP LIEN TUC");
  Serial.println("████████████████████████████\n");

  sendAlertEvent(true);
}

void resetStateMachine() {
  bool wasAlert = alertActive;
  currentState    = STATE_IDLE;
  stateEntryTime  = 0;
  impactTime      = 0;
  motionlessStart = 0;
  motionlessLostStart = 0;
  movementStart   = 0;
  resetRecoveryTracking();
  alertStartTime  = 0;
  alertActive     = false;
  stopBuzzerPattern();
  if (wasAlert) {
    sendAlertEvent(false);
  }
  Serial.println("[SYSTEM] RESET — Trang thai binh thuong\n");
}

void cancelPendingFall(const char* reason) {
  currentState    = STATE_IDLE;
  stateEntryTime  = 0;
  impactTime      = 0;
  motionlessStart = 0;
  motionlessLostStart = 0;
  movementStart   = 0;
  resetRecoveryTracking();
  alertStartTime  = 0;
  alertActive     = false;
  stopBuzzerPattern();
  Serial.printf("[HUY CANH BAO] %s — quay ve IDLE\n", reason);
}

// ─────────────────────────────────────────────────
// STATE MACHINE (giữ nguyên cấu trúc gốc)
// ─────────────────────────────────────────────────
void stateMachine() {
  unsigned long now = millis();

  switch (currentState) {

  case STATE_IDLE:
    if (detectFall()) {
      currentState   = STATE_FALLING;
      stateEntryTime = now;
      Serial.println("[GD 1] Phat hien roi tu do / chuyen dong bat thuong");
    }
    break;

  case STATE_FALLING:
    if (now - stateEntryTime >= FALL_DURATION_MS) {
      currentState   = STATE_IMPACT;
      stateEntryTime = now;
      Serial.println("[GD 2] Cho va cham...");
      break;
    }

    if (currentData.accelMagnitude > 0.8f &&
        currentData.accelMagnitude < 1.3f &&
        motionStats.gyroDelta < 20) {
      Serial.println("[GD 1] Bao dong gia — bo qua");
      currentState = STATE_IDLE;
    }
    break;

  case STATE_IMPACT:
    if (detectImpact()) {
      currentState    = STATE_MOTIONLESS;
      impactTime      = now;
      motionlessStart = 0;       // chi bat dau dem 30s khi sensor that su nam yen
      motionlessLostStart = 0;
      movementStart   = 0;
      resetRecoveryTracking();
      stateEntryTime  = now;
      startPreAlertBeep();
      sendPreAlertEvent();
      Serial.println("[GD 2] Va cham phat hien — TIT 1 lan, sau do cho nam yen 30s...");
    } else if (now - stateEntryTime > IMPACT_WINDOW_MS) {
      Serial.println("[GD 2] Het thoi gian cho va cham");
      currentState = STATE_IDLE;
    }
    break;

  case STATE_MOTIONLESS:
    // Sau va cham + tieng TIT, bo qua mot khoang ngan de sensor het rung/het dao dong.
    // Trong khoang nay KHONG duoc huy ve IDLE, vi luc vua cham dat thuong con nhieu mau du lieu lon.
    if (now - stateEntryTime < POST_IMPACT_GRACE_MS) {
      motionlessStart     = 0;
      motionlessLostStart = 0;
      movementStart       = 0;
      break;
    }

    // Sau khoang grace moi bat dau xet chuyen dong phuc hoi.
    // KHONG ve IDLE chi vi co 1 cu lac ngan. Phai chuyen dong du lau
    // nhu nguoi nam -> ngoi day -> dung day/di moi huy case te nga.
    if (recoveryMovementConfirmed(now)) {
      cancelPendingFall("Chuyen dong phuc hoi du lau sau va cham");
      break;
    }

    if (detectMotionless()) {
      movementStart       = 0;
      motionlessLostStart = 0;

      if (motionlessStart == 0) {
        motionlessStart = now;
        Serial.println("[GD 3] Bat dau dem bat dong 30s...");
      }

      if (now - motionlessStart >= MOTIONLESS_DURATION_MS) {
        currentState = STATE_ALERT;
        triggerAlert();
      }
    } else {
      // Co chuyen dong/nghieng/rung nhung chua du lau de xem la phuc hoi.
      // Neu truoc do dang dem bat dong, chi dem lai 30s; KHONG quay ve IDLE ngay.
      if (motionlessStart != 0) {
        if (motionlessLostStart == 0) {
          motionlessLostStart = now;
        }

        if (now - motionlessLostStart >= STILLNESS_LOST_TOLERANCE_MS) {
          Serial.println("[GD 3] Mat bat dong — dem lai 30s, chua ve IDLE vi chua phuc hoi du lau");
          motionlessStart     = 0;
          motionlessLostStart = 0;
        }
      }
    }
    break;

  case STATE_ALERT:
    // Buzzer phat SOS lien tuc. Khong tat chi vi rung/lac ngan.
    // Chi tat khi co chuyen dong phuc hoi du lau: ngoi day/dung day/di.
    if (now - alertStartTime > ALERT_IGNORE_MOVEMENT_MS && recoveryMovementConfirmed(now)) {
      Serial.println("[PHUC HOI] Chuyen dong du lau trong luc SOS — tat canh bao");
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
  if (wifiScanActive || wifiConnectionAttemptActive) return;
  lastPrint = millis();
  const char* stateNames[] = {"IDLE", "FALLING", "IMPACT", "MOTIONLESS", "ALERT"};
  unsigned long recoveryMs = (recoveryStart == 0) ? 0 : (millis() - recoveryStart);
  Serial.printf("ACC:%.2fg dMag:%.2f dVec:%.2f dG:%.2f gAbs:%.2f rec:%lums STATE:%s\n",
    currentData.accelMagnitude,
    motionStats.accelDelta,
    motionStats.accelVectorDelta,
    motionStats.gyroDelta,
    motionStats.gyroAbs,
    recoveryMs,
    stateNames[currentState]
  );
}

bool initializeMpuSensor() {
  Serial.print("[MPU6050] Init... ");
  mpu.initialize();
  if (!mpu.testConnection()) {
    Serial.println("FAILED - BLE/MQTT van tiep tuc hoat dong");
    return false;
  }
  mpu.setFullScaleAccelRange(MPU6050_ACCEL_FS_4);
  mpu.setFullScaleGyroRange(MPU6050_GYRO_FS_500);
  mpu.setDLPFMode(MPU6050_DLPF_BW_42);
  mpu.setIntDataReadyEnabled(true);
  delay(100);
  readSensor();
  prevData = currentData;
  Serial.println("OK");
  return true;
}

void serviceMpuHealth() {
  const unsigned long now = millis();
  if (now - lastMpuHealthCheckMs < MPU_HEALTH_CHECK_INTERVAL_MS) return;
  lastMpuHealthCheckMs = now;

  if (mpuReady && !mpu.testConnection()) {
    mpuReady = false;
    enqueueConnectivityEvent(MedPulseEventType::HARDWARE_FAULT);
    Serial.println("[MPU6050] Mat ket noi; dang chay che do ket noi an toan.");
    return;
  }
  if (!mpuReady && initializeMpuSensor()) {
    mpuReady = true;
    enqueueConnectivityEvent(MedPulseEventType::HARDWARE_RECOVERED);
    Serial.println("[MPU6050] Da phuc hoi.");
  }
}

// ─────────────────────────────────────────────────
// SETUP
// ─────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println("\n========================================");
  Serial.println("  MedPulse — THIET BI DEO BENH NHAN");
  Serial.println("  BLE GATT + MQTT fallback da bat");
  Serial.println("========================================");

  pinMode(BUZZER_PIN, OUTPUT);
  buzzerOff();
  pinMode(INT_PIN, INPUT);
  attachInterrupt(digitalPinToInterrupt(INT_PIN), mpuISR, RISING);

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(400000);
  initConnectivity();
  mpuReady = initializeMpuSensor();
  lastMpuHealthCheckMs = millis();
  enqueueConnectivityEvent(mpuReady
      ? MedPulseEventType::HARDWARE_RECOVERED
      : MedPulseEventType::HARDWARE_FAULT);

  Serial.println("[SYSTEM] San sang giam sat te nga (standalone).");
  Serial.println("----------------------------------------");
}

// ─────────────────────────────────────────────────
// LOOP
// ─────────────────────────────────────────────────
unsigned long lastHeartbeatMs = 0;
#define HEARTBEAT_INTERVAL_MS 2000

void loop() {
  unsigned long now = millis();

  // Buzzer dung pattern khong-blocking, phai update ca khi chua toi chu ky doc MPU.
  updateBuzzerPattern();
  serviceConnectivity();
  serviceMpuHealth();

  if (!mpuReady) {
    if (now - lastHeartbeatMs >= HEARTBEAT_INTERVAL_MS) {
      lastHeartbeatMs = now;
      sendPresenceHeartbeat();
    }
    return;
  }

  // ── Sample MPU6050 ────────────────────────────
  bool shouldSample = intTriggered;
  if (intTriggered) intTriggered = false;
  if (now - lastSampleTime >= SAMPLE_RATE_MS) shouldSample = true;
  if (!shouldSample) return;
  lastSampleTime = now;

  readSensor();

  motionStats.accelDelta = fabsf(currentData.accelMagnitude - prevData.accelMagnitude);
  motionStats.accelVectorDelta = vectorDelta3(
    currentData.ax, currentData.ay, currentData.az,
    prevData.ax, prevData.ay, prevData.az
  );
  float gyroCur  = magnitude(currentData.gx, currentData.gy, currentData.gz);
  float gyroPrev = magnitude(prevData.gx, prevData.gy, prevData.gz);
  motionStats.gyroAbs   = gyroCur;
  motionStats.gyroDelta = fabsf(gyroCur - gyroPrev);

  printStatus();
  stateMachine();

  // Khong con timeout tu tat buzzer.
  // Buzzer SOS chi tat khi state ALERT phat hien chuyen dong phuc hoi du lau.

  // ── Heartbeat định kỳ khi an toàn ─────────────────────────────────────────
  if (!alertActive && now - lastHeartbeatMs >= HEARTBEAT_INTERVAL_MS) {
    lastHeartbeatMs = now;
    sendPresenceHeartbeat();
  }

  prevData = currentData;
}
