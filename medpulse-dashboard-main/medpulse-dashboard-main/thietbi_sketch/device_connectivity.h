#pragma once

// File nay duoc include sau phan buzzer/state globals trong thietbi_sketch.ino.

// BLEAdvertising khong co API kiem tra trang thai on dinh giua cac ban
// Arduino-ESP32, nen theo doi moi lan start/stop trong firmware.
volatile bool medpulseAdvertisingRunning = false;
bool medpulseWifiConfigValid = false;
bool medpulseWifiCoexPreferred = false;
bool medpulseWifiStaticDiagnosticsPrinted = false;
bool medpulseWifiTestStyleConfigured = false;

void preferWifiRadioForAttempt() {
  if (medpulseWifiCoexPreferred) return;
  const esp_err_t result = esp_coex_preference_set(ESP_COEX_PREFER_WIFI);
  if (result == ESP_OK) {
    medpulseWifiCoexPreferred = true;
    Serial.println("[RADIO] Uu tien Wi-Fi trong luc scan/auth.");
  } else {
    Serial.printf("[RADIO] Khong the uu tien Wi-Fi: 0x%x.\n", static_cast<unsigned int>(result));
  }
}

void restoreBalancedRadioAfterWifiAttempt() {
  if (!medpulseWifiCoexPreferred) return;
  const esp_err_t result = esp_coex_preference_set(ESP_COEX_PREFER_BALANCE);
  if (result == ESP_OK) {
    medpulseWifiCoexPreferred = false;
    Serial.println("[RADIO] Tra lai che do can bang Wi-Fi/BLE.");
  } else {
    Serial.printf("[RADIO] Khong the tra lai che do can bang: 0x%x.\n", static_cast<unsigned int>(result));
  }
}

void pauseBleAdvertisingForWifiAuth() {
  if (!medpulseAdvertising || !medpulseAdvertisingRunning) return;
  medpulseAdvertising->stop();
  medpulseAdvertisingRunning = false;
  Serial.println("[BLE] Tam dung advertising de Wi-Fi scan/auth.");
}

void resumeBleAdvertisingAfterWifiAttempt() {
  restoreBalancedRadioAfterWifiAttempt();
  if (gattConnected || !medpulseAdvertising || medpulseAdvertisingRunning) return;
  medpulseAdvertising->start();
  medpulseAdvertisingRunning = true;
  Serial.println("[BLE] Bat lai advertising sau Wi-Fi attempt.");
}

void configureWifiLikeStandaloneTest() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(false);
  WiFi.setTxPower(WIFI_POWER_15dBm);

  const esp_err_t psResult = esp_wifi_set_ps(WIFI_PS_NONE);
  const esp_err_t protocolResult = esp_wifi_set_protocol(
      WIFI_IF_STA, WIFI_PROTOCOL_11B | WIFI_PROTOCOL_11G | WIFI_PROTOCOL_11N);
  const esp_err_t bandwidthResult = esp_wifi_set_bandwidth(WIFI_IF_STA, WIFI_BW_HT20);

  if (!medpulseWifiTestStyleConfigured) {
    medpulseWifiTestStyleConfigured = true;
    Serial.printf("[WiFiTestFlow] sleep=off, autoReconnect=off, txPower=15dBm, ps=0x%x, protocol=0x%x, bw=0x%x\n",
        static_cast<unsigned int>(psResult),
        static_cast<unsigned int>(protocolResult),
        static_cast<unsigned int>(bandwidthResult));
  }
}

bool isValidDeviceId(const char* value) {
  if (!value) return false;
  const size_t length = strlen(value);
  // Local Name nam trong BLE scan response (toi da 29 byte payload ten).
  if (length == 0 || length > 20 || strlen(MEDPULSE_BLE_NAME_PREFIX) + length > 29) return false;
  for (size_t index = 0; index < length; index++) {
    const char c = value[index];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
        || (c >= '0' && c <= '9') || c == '-' || c == '_')) return false;
  }
  return true;
}

bool isHexWifiPsk(const char* value) {
  if (!value || strlen(value) != 64) return false;
  for (size_t index = 0; index < 64; index++) {
    const char c = value[index];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) return false;
  }
  return true;
}

bool isValidWifiConfig() {
  const size_t ssidLength = strlen(MEDPULSE_WIFI_SSID);
  const size_t passwordLength = strlen(MEDPULSE_WIFI_PASSWORD);
  if (ssidLength == 0 || ssidLength > 32 || strcmp(MEDPULSE_WIFI_SSID, "YOUR_WIFI_NAME") == 0) return false;
  if (strcmp(MEDPULSE_WIFI_PASSWORD, "YOUR_WIFI_PASSWORD") == 0) return false;
  if (passwordLength == 0) return true;  // Cho phep AP mo.
  if (passwordLength == 64) return isHexWifiPsk(MEDPULSE_WIFI_PASSWORD);
  if (passwordLength < 8 || passwordLength > 63) return false;
  for (size_t index = 0; index < passwordLength; index++) {
    const uint8_t c = static_cast<uint8_t>(MEDPULSE_WIFI_PASSWORD[index]);
    if (c < 32 || c > 126) return false;
  }
  return true;
}

int currentBatteryPercent() {
  const int configured = MEDPULSE_FIXED_BATTERY_PERCENT;
  return configured >= 0 && configured <= 100 ? configured : -1;
}

bool commandTokenWasExecuted(uint32_t token) {
  if (token == 0) return false;
  for (uint8_t index = 0; index < COMMAND_TOKEN_HISTORY_SIZE; index++) {
    if (commandTokenHistory[index] == token) return true;
  }
  return false;
}

void rememberExecutedCommandToken(uint32_t token) {
  if (token == 0 || commandTokenWasExecuted(token)) return;
  commandTokenHistory[commandTokenHistoryIndex] = token;
  commandTokenHistoryIndex = (commandTokenHistoryIndex + 1) % COMMAND_TOKEN_HISTORY_SIZE;
  commandPreferences.putBytes("cmdTokens", commandTokenHistory, sizeof(commandTokenHistory));
  commandPreferences.putUChar("cmdIndex", commandTokenHistoryIndex);
}

void sendGattCommandAck(uint32_t token, MedPulseCommandAckStatus status, MedPulseCommandAckError error) {
  if (!gattConnected || !medpulseEventCharacteristic) {
    Serial.printf("[COMMAND GATT] Khong the gui ACK token=0x%08lx vi GATT chua ket noi.\n",
        static_cast<unsigned long>(token));
    return;
  }
  uint8_t frame[MEDPULSE_GATT_ACK_FRAME_LENGTH] = {};
  frame[0] = MEDPULSE_FRAME_MAGIC_M;
  frame[1] = MEDPULSE_FRAME_MAGIC_P;
  frame[2] = MEDPULSE_COMMAND_PROTOCOL_VERSION;
  frame[3] = MEDPULSE_FRAME_TYPE_COMMAND_ACK;
  medpulseWriteUint32Le(frame + 4, token);
  frame[8] = static_cast<uint8_t>(status);
  frame[9] = static_cast<uint8_t>(error);
  const uint16_t crc = medpulseCrc16Ccitt(frame, 10);
  frame[10] = crc & 0xFF;
  frame[11] = (crc >> 8) & 0xFF;
  medpulseEventCharacteristic->setValue(frame, sizeof(frame));
  medpulseEventCharacteristic->notify();
  Serial.printf("[COMMAND GATT] Da gui ACK token=0x%08lx status=%u error=%u.\n",
      static_cast<unsigned long>(token), static_cast<unsigned int>(status), static_cast<unsigned int>(error));
}

bool decodeGattCommand(const uint8_t* data, size_t length, PendingDeviceCommand& command) {
  if (!data || length != MEDPULSE_GATT_COMMAND_FRAME_LENGTH) return false;
  if (data[0] != MEDPULSE_FRAME_MAGIC_M || data[1] != MEDPULSE_FRAME_MAGIC_P
      || data[2] != MEDPULSE_COMMAND_PROTOCOL_VERSION || data[3] != MEDPULSE_FRAME_TYPE_COMMAND) return false;
  const uint16_t expectedCrc = static_cast<uint16_t>(data[14])
      | (static_cast<uint16_t>(data[15]) << 8);
  if (medpulseCrc16Ccitt(data, 14) != expectedCrc || data[9] != 0) return false;
  const uint8_t action = data[8];
  if (action < static_cast<uint8_t>(MedPulseCommandAction::BUZZER_ON)
      || action > static_cast<uint8_t>(MedPulseCommandAction::BUZZER_TEST)) return false;
  const uint32_t durationMs = medpulseReadUint32Le(data + 10);
  if (durationMs > 300000UL) return false;
  command = {};
  command.token = medpulseReadUint32Le(data + 4);
  command.action = action;
  command.durationMs = durationMs;
  command.viaMqtt = false;
  return command.token != 0;
}

void enqueueGattCommand(const uint8_t* data, size_t length) {
  PendingDeviceCommand command = {};
  if (!decodeGattCommand(data, length, command)) {
    const uint32_t token = data && length >= 8 ? medpulseReadUint32Le(data + 4) : 0;
    Serial.printf("[COMMAND GATT] Frame khong hop le, length=%u, token=0x%08lx.\n",
        static_cast<unsigned int>(length), static_cast<unsigned long>(token));
    sendGattCommandAck(token, MedPulseCommandAckStatus::FAILED, MedPulseCommandAckError::INVALID_FRAME);
    return;
  }
  if (!deviceCommandQueue || xQueueSend(deviceCommandQueue, &command, 0) != pdTRUE) {
    Serial.printf("[COMMAND GATT] Queue day, token=0x%08lx.\n", static_cast<unsigned long>(command.token));
    sendGattCommandAck(command.token, MedPulseCommandAckStatus::FAILED, MedPulseCommandAckError::EXECUTION_FAILED);
  } else {
    Serial.printf("[COMMAND GATT] Da nhan lenh token=0x%08lx action=%u duration=%lu.\n",
        static_cast<unsigned long>(command.token), static_cast<unsigned int>(command.action),
        static_cast<unsigned long>(command.durationMs));
  }
}

void enqueueConnectivityEvent(MedPulseEventType eventType);

class MedPulseCommandCallbacks : public BLECharacteristicCallbacks {
 public:
  void onWrite(BLECharacteristic* characteristic) override {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
    enqueueGattCommand(characteristic->getData(), characteristic->getLength());
#else
    std::string value = characteristic->getValue();
    enqueueGattCommand(reinterpret_cast<const uint8_t*>(value.data()), value.size());
#endif
  }
};

class MedPulseServerCallbacks : public BLEServerCallbacks {
 public:
  void onConnect(BLEServer* server) override {
    (void)server;
    gattConnected = true;
    gattWasConnected = true;
    medpulseAdvertisingRunning = false;
    enqueueConnectivityEvent(mpuReady
        ? MedPulseEventType::HARDWARE_RECOVERED
        : MedPulseEventType::HARDWARE_FAULT);
    Serial.println("[BLE] Tram da ket noi GATT.");
  }

  void onDisconnect(BLEServer* server) override {
    (void)server;
    gattConnected = false;
    lastGattDisconnectedAtMs = millis();
    if (medpulseAdvertising && !medpulseAdvertisingRunning) {
      medpulseAdvertising->start();
      medpulseAdvertisingRunning = true;
    }
    Serial.println("[BLE] Mat GATT; advertising lai.");
  }
};

void configureBleAdvertising() {
  const int battery = currentBatteryPercent();
  uint8_t presenceFrame[5] = {
    MEDPULSE_FRAME_MAGIC_M, MEDPULSE_FRAME_MAGIC_P, MEDPULSE_FRAME_VERSION,
    MEDPULSE_FRAME_TYPE_PRESENCE,
    static_cast<uint8_t>(battery < 0 ? MEDPULSE_BATTERY_UNKNOWN : battery)
  };
  BLEAdvertisementData advertisementData;
  advertisementData.setCompleteServices(BLEUUID(MEDPULSE_GATT_SERVICE_UUID));
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  advertisementData.setManufacturerData(String(reinterpret_cast<char*>(presenceFrame), sizeof(presenceFrame)));
#else
  advertisementData.setManufacturerData(std::string(reinterpret_cast<char*>(presenceFrame), sizeof(presenceFrame)));
#endif
  medpulseAdvertising->setAdvertisementData(advertisementData);
  BLEAdvertisementData scanResponseData;
  scanResponseData.setName((String(MEDPULSE_BLE_NAME_PREFIX) + MEDPULSE_DEVICE_ID).c_str());
  medpulseAdvertising->setScanResponseData(scanResponseData);
  medpulseAdvertising->setMinInterval(0x0320);
  medpulseAdvertising->setMaxInterval(0x0640);
}

void initializeBleTransport() {
  const String localName = String(MEDPULSE_BLE_NAME_PREFIX) + MEDPULSE_DEVICE_ID;
  BLEDevice::init(localName.c_str());
  medpulseBleServer = BLEDevice::createServer();
  medpulseBleServer->setCallbacks(new MedPulseServerCallbacks());
  BLEService* service = medpulseBleServer->createService(MEDPULSE_GATT_SERVICE_UUID);
  medpulseEventCharacteristic = service->createCharacteristic(
      MEDPULSE_GATT_EVENT_UUID, BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);
  medpulseEventCharacteristic->addDescriptor(new BLE2902());
  BLECharacteristic* commandCharacteristic = service->createCharacteristic(
      MEDPULSE_GATT_COMMAND_UUID, BLECharacteristic::PROPERTY_WRITE);
  commandCharacteristic->setCallbacks(new MedPulseCommandCallbacks());
  service->start();
  medpulseAdvertising = BLEDevice::getAdvertising();
  configureBleAdvertising();
  medpulseAdvertising->start();
  medpulseAdvertisingRunning = true;
  Serial.printf("[BLE] Advertising %s\n", localName.c_str());
}

void enqueueConnectivityEvent(MedPulseEventType eventType) {
  if (!connectivityEventQueue) return;
  const int battery = currentBatteryPercent();
  PendingConnectivityEvent event = {};
  event.type = static_cast<uint8_t>(eventType);
  event.sequence = eventSequence++;
  event.battery = battery < 0 ? -1 : static_cast<int8_t>(battery);
  event.flags = (alertActive ? MEDPULSE_GATT_EVENT_FLAG_FALL : 0)
      | (buzzerMode != BUZZER_SILENT ? MEDPULSE_GATT_EVENT_FLAG_BUZZER : 0);
  event.deviceUptimeSeconds = millis() / 1000UL;
  if (xQueueSend(connectivityEventQueue, &event, 0) != pdTRUE) {
    PendingConnectivityEvent discarded;
    xQueueReceive(connectivityEventQueue, &discarded, 0);
    xQueueSend(connectivityEventQueue, &event, 0);
  }
}

bool notifyGattEvent(const PendingConnectivityEvent& event) {
  if (!gattConnected || !medpulseEventCharacteristic) return false;
  uint8_t frame[MEDPULSE_GATT_EVENT_FRAME_LENGTH] = {};
  frame[0] = MEDPULSE_FRAME_MAGIC_M;
  frame[1] = MEDPULSE_FRAME_MAGIC_P;
  frame[2] = MEDPULSE_COMMAND_PROTOCOL_VERSION;
  frame[3] = MEDPULSE_FRAME_TYPE_EVENT;
  frame[4] = event.sequence;
  frame[5] = event.type;
  frame[6] = event.battery < 0 ? MEDPULSE_BATTERY_UNKNOWN : static_cast<uint8_t>(event.battery);
  frame[7] = event.flags;
  medpulseWriteUint32Le(frame + 8, event.deviceUptimeSeconds);
  medpulseEventCharacteristic->setValue(frame, sizeof(frame));
  medpulseEventCharacteristic->notify();
  return true;
}

const char* eventTypeName(MedPulseEventType eventType) {
  switch (eventType) {
    case MedPulseEventType::HEARTBEAT: return "HEARTBEAT";
    case MedPulseEventType::PREALERT: return "PREALERT";
    case MedPulseEventType::FALL_ALERT: return "FALL_ALERT";
    case MedPulseEventType::RECOVERED: return "RECOVERED";
    case MedPulseEventType::HARDWARE_FAULT: return "HARDWARE_FAULT";
    case MedPulseEventType::HARDWARE_RECOVERED: return "HARDWARE_RECOVERED";
    default: return "HEARTBEAT";
  }
}

void publishDirectStatus(bool online) {
  if (!deviceMqtt.connected()) return;
  const String topic = medpulseDirectDeviceTopic(MEDPULSE_MQTT_ROOT_TOPIC, MEDPULSE_DEVICE_ID, "status");
  char payload[256];
  snprintf(payload, sizeof(payload),
      "{\"protocolVersion\":%u,\"deviceId\":\"%s\",\"transport\":\"DIRECT_MQTT\",\"online\":%s,\"wifiRssi\":%d,\"sensorReady\":%s}",
      MEDPULSE_COMMAND_PROTOCOL_VERSION, MEDPULSE_DEVICE_ID, online ? "true" : "false", WiFi.RSSI(),
      mpuReady ? "true" : "false");
  deviceMqtt.publish(topic.c_str(), payload, false);
  lastDirectStatusMs = millis();
}

bool publishDirectEvent(const PendingConnectivityEvent& event) {
  if (!deviceMqtt.connected()) return false;
  const String topic = medpulseDirectDeviceTopic(MEDPULSE_MQTT_ROOT_TOPIC, MEDPULSE_DEVICE_ID, "events");
  const MedPulseEventType eventType = static_cast<MedPulseEventType>(event.type);
  char payload[384];
  snprintf(payload, sizeof(payload),
      "{\"protocolVersion\":%u,\"deviceId\":\"%s\",\"sequence\":%u,\"event\":\"%s\","
      "\"battery\":%d,\"fallDetected\":%s,\"charging\":false,\"buzzerActive\":%s,\"deviceUptimeSeconds\":%lu}",
      MEDPULSE_COMMAND_PROTOCOL_VERSION, MEDPULSE_DEVICE_ID, event.sequence, eventTypeName(eventType), event.battery,
      (event.flags & MEDPULSE_GATT_EVENT_FLAG_FALL) ? "true" : "false",
      (event.flags & MEDPULSE_GATT_EVENT_FLAG_BUZZER) ? "true" : "false", event.deviceUptimeSeconds);
  return deviceMqtt.publish(topic.c_str(), payload, false);
}

void publishDirectCommandAck(const PendingDeviceCommand& command, const char* status, const char* error = nullptr) {
  if (!deviceMqtt.connected() || !command.viaMqtt || strlen(command.commandId) == 0) return;
  const String topic = medpulseDirectDeviceTopic(MEDPULSE_MQTT_ROOT_TOPIC, MEDPULSE_DEVICE_ID, "acks");
  char payload[384];
  if (error) {
    snprintf(payload, sizeof(payload),
        "{\"protocolVersion\":%u,\"commandId\":\"%s\",\"deviceId\":\"%s\",\"status\":\"%s\",\"transport\":\"DIRECT_MQTT\",\"error\":\"%s\"}",
        MEDPULSE_COMMAND_PROTOCOL_VERSION, command.commandId, MEDPULSE_DEVICE_ID, status, error);
  } else {
    snprintf(payload, sizeof(payload),
        "{\"protocolVersion\":%u,\"commandId\":\"%s\",\"deviceId\":\"%s\",\"status\":\"%s\",\"transport\":\"DIRECT_MQTT\"}",
        MEDPULSE_COMMAND_PROTOCOL_VERSION, command.commandId, MEDPULSE_DEVICE_ID, status);
  }
  deviceMqtt.publish(topic.c_str(), payload, false);
}

uint8_t commandActionFromName(const char* action) {
  if (strcmp(action, "BUZZER_ON") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_ON);
  if (strcmp(action, "BUZZER_OFF") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_OFF);
  if (strcmp(action, "BUZZER_TEST") == 0) return static_cast<uint8_t>(MedPulseCommandAction::BUZZER_TEST);
  return 0;
}

const char* wifiDisconnectReasonName(uint8_t reason) {
  return WiFi.STA.disconnectReasonName(static_cast<wifi_err_reason_t>(reason));
}

const char* wifiAuthModeName(wifi_auth_mode_t authMode) {
  switch (authMode) {
    case WIFI_AUTH_OPEN: return "OPEN";
    case WIFI_AUTH_WEP: return "WEP";
    case WIFI_AUTH_WPA_PSK: return "WPA_PSK";
    case WIFI_AUTH_WPA2_PSK: return "WPA2_PSK";
    case WIFI_AUTH_WPA_WPA2_PSK: return "WPA_WPA2_PSK";
    case WIFI_AUTH_WPA3_PSK: return "WPA3_PSK";
    case WIFI_AUTH_WPA2_WPA3_PSK: return "WPA2_WPA3_PSK";
    case WIFI_AUTH_OWE: return "OWE";
    default: return "OTHER";
  }
}

void printWifiStaticDiagnostics() {
  if (medpulseWifiStaticDiagnosticsPrinted) return;
  medpulseWifiStaticDiagnosticsPrinted = true;

  wifi_country_t country = {};
  int8_t txPower = 0;
  uint8_t protocolBitmap = 0;
  uint8_t staMac[6] = {};
  WiFi.macAddress(staMac);
  const esp_err_t countryResult = esp_wifi_get_country(&country);
  const esp_err_t txResult = esp_wifi_get_max_tx_power(&txPower);
  const esp_err_t protocolResult = esp_wifi_get_protocol(WIFI_IF_STA, &protocolBitmap);

  Serial.printf("[WiFiDiag] SDK=%s, STA_MAC=%02X:%02X:%02X:%02X:%02X:%02X, heap=%lu, minHeap=%lu\n",
      ESP.getSdkVersion(), staMac[0], staMac[1], staMac[2], staMac[3], staMac[4], staMac[5],
      static_cast<unsigned long>(ESP.getFreeHeap()), static_cast<unsigned long>(ESP.getMinFreeHeap()));
  Serial.printf("[WiFiDiag] SSID_len=%u, password_len=%u, mode=%d, sleep=%d, autoReconnect=%d\n",
      static_cast<unsigned int>(strlen(MEDPULSE_WIFI_SSID)),
      static_cast<unsigned int>(strlen(MEDPULSE_WIFI_PASSWORD)), static_cast<int>(WiFi.getMode()),
      static_cast<int>(WiFi.getSleep()), WiFi.getAutoReconnect() ? 1 : 0);
  Serial.printf("[WiFiDiag] country=%s%.2s, channels=%u..%u, countryMaxTx=%d dBm, "
                "txPower=%s%d (quarter-dBm), protocol=%s0x%02X\n",
      countryResult == ESP_OK ? "" : "ERR/", countryResult == ESP_OK ? country.cc : "??",
      countryResult == ESP_OK ? country.schan : 0,
      countryResult == ESP_OK ? country.schan + country.nchan - 1 : 0,
      countryResult == ESP_OK ? country.max_tx_power : 0,
      txResult == ESP_OK ? "" : "ERR/", txResult == ESP_OK ? txPower : 0,
      protocolResult == ESP_OK ? "" : "ERR/", protocolResult == ESP_OK ? protocolBitmap : 0);
}

void printWifiStaConfigDiagnostics() {
  wifi_config_t config = {};
  const esp_err_t result = esp_wifi_get_config(WIFI_IF_STA, &config);
  if (result != ESP_OK) {
    Serial.printf("[WiFiDiag] esp_wifi_get_config failed: 0x%x.\n", static_cast<unsigned int>(result));
    return;
  }
  Serial.printf("[WiFiDiag] STA config: channel=%u, bssid_set=%u, scan=%d, sort=%d, "
                "minAuth=%d/%s, PMF(capable=%u required=%u)\n",
      config.sta.channel, config.sta.bssid_set, static_cast<int>(config.sta.scan_method),
      static_cast<int>(config.sta.sort_method), static_cast<int>(config.sta.threshold.authmode),
      wifiAuthModeName(config.sta.threshold.authmode), config.sta.pmf_cfg.capable,
      config.sta.pmf_cfg.required);
}

void handleDeviceWiFiEvent(WiFiEvent_t event, WiFiEventInfo_t info) {
  const unsigned long now = millis();
  const unsigned long elapsed = wifiCycleStartedAtMs == 0 ? 0 : now - wifiCycleStartedAtMs;
  if (event == ARDUINO_EVENT_WIFI_STA_START) {
    Serial.printf("[WiFiEvt #%lu +%lums] STA_START, mode=%d\n",
        wifiAttemptSequence, elapsed, static_cast<int>(WiFi.getMode()));
    return;
  }
  if (event == ARDUINO_EVENT_WIFI_STA_CONNECTED) {
    const wifi_event_sta_connected_t& connected = info.wifi_sta_connected;
    Serial.printf("[WiFiEvt #%lu +%lums] STA_CONNECTED, SSID='%.*s', "
                  "BSSID=%02X:%02X:%02X:%02X:%02X:%02X, channel=%u, auth=%d/%s, aid=%u\n",
        wifiAttemptSequence, elapsed, connected.ssid_len,
        reinterpret_cast<const char*>(connected.ssid),
        connected.bssid[0], connected.bssid[1], connected.bssid[2],
        connected.bssid[3], connected.bssid[4], connected.bssid[5], connected.channel,
        static_cast<int>(connected.authmode), wifiAuthModeName(connected.authmode), connected.aid);
    return;
  }
  if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) {
    Serial.printf("[WiFiEvt #%lu +%lums] GOT_IP\n", wifiAttemptSequence, elapsed);
    return;
  }
  if (event == ARDUINO_EVENT_WIFI_STA_LOST_IP) {
    Serial.printf("[WiFiEvt #%lu +%lums] LOST_IP\n", wifiAttemptSequence, elapsed);
    return;
  }
  if (event == ARDUINO_EVENT_WIFI_STA_STOP) {
    Serial.printf("[WiFiEvt #%lu +%lums] STA_STOP\n", wifiAttemptSequence, elapsed);
    return;
  }
  if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
    const wifi_event_sta_disconnected_t& disconnected = info.wifi_sta_disconnected;
    wifiDisconnectEventCount++;
    Serial.printf("[WiFiEvt #%lu +%lums] DISCONNECTED[%u], reason=%u/%s, RSSI=%d, "
                  "SSID='%.*s', BSSID=%02X:%02X:%02X:%02X:%02X:%02X, active=%d\n",
        wifiAttemptSequence, elapsed, wifiDisconnectEventCount, disconnected.reason,
        wifiDisconnectReasonName(disconnected.reason), disconnected.rssi,
        disconnected.ssid_len, reinterpret_cast<const char*>(disconnected.ssid),
        disconnected.bssid[0], disconnected.bssid[1], disconnected.bssid[2],
        disconnected.bssid[3], disconnected.bssid[4], disconnected.bssid[5],
        wifiConnectionAttemptActive ? 1 : 0);

    // Callback Wi-Fi chi ghi lai ket qua; main loop quyet dinh retry.
    if (wifiConnectionAttemptActive) {
      wifiLastDisconnectReason = disconnected.reason;
      wifiLastDisconnectRssi = disconnected.rssi;
      wifiLastDisconnectAtMs = now;
      wifiConnectionAttemptFailed = true;
    }
  }
}

void handleDirectMqttMessage(char* topicChars, byte* payloadBytes, unsigned int length) {
  const String expectedTopic = medpulseDirectDeviceTopic(MEDPULSE_MQTT_ROOT_TOPIC, MEDPULSE_DEVICE_ID, "commands");
  if (String(topicChars) != expectedTopic || length == 0 || length >= 512) return;
  JsonDocument document;
  if (deserializeJson(document, payloadBytes, length)
      || document["protocolVersion"] != MEDPULSE_COMMAND_PROTOCOL_VERSION
      || document["deviceId"] != MEDPULSE_DEVICE_ID) return;
  const char* commandId = document["commandId"] | "";
  const char* actionName = document["action"] | "";
  const uint32_t durationMs = document["durationMs"] | 0;
  const uint8_t action = commandActionFromName(actionName);
  if (strlen(commandId) == 0 || strlen(commandId) > 64 || action == 0 || durationMs > 300000UL) return;
  PendingDeviceCommand command = {};
  command.token = medpulseFnv1a(commandId);
  command.action = action;
  command.durationMs = durationMs;
  command.viaMqtt = true;
  strncpy(command.commandId, commandId, sizeof(command.commandId) - 1);
  if (command.token == 0) {
    publishDirectCommandAck(command, "FAILED", "INVALID_COMMAND_TOKEN");
    return;
  }
  if (!deviceCommandQueue || xQueueSend(deviceCommandQueue, &command, 0) != pdTRUE) {
    publishDirectCommandAck(command, "FAILED", "DEVICE_COMMAND_QUEUE_FULL");
  }
}

void ensureDirectConnectivity() {
  const unsigned long now = millis();
  const wl_status_t wifiStatus = WiFi.status();

  if (!medpulseWifiConfigValid) return;

  if (wifiStatus == WL_CONNECTED) {
    if (wifiScanActive) {
      WiFi.scanDelete();
      wifiScanActive = false;
    }
    const bool completedWifiAttempt = wifiConnectionAttemptActive;
    if (completedWifiAttempt) {
      wifiConnectionAttemptActive = false;
      wifiConnectionAttemptFailed = false;
      wifiLastDisconnectReason = 0;
      wifiLastDisconnectAtMs = 0;
    }
    // Goi vo dieu kien de phuc hoi radio/advertising ca trong truong hop driver
    // ket noi xong tu retry noi bo truoc khi main loop cap nhat attempt flag.
    resumeBleAdvertisingAfterWifiAttempt();
    if (completedWifiAttempt) {
      Serial.printf("[WiFi] Da ket noi, IP=%s, RSSI=%d dBm, channel=%d, BSSID=%s\n",
          WiFi.localIP().toString().c_str(), WiFi.RSSI(), WiFi.channel(), WiFi.BSSIDstr().c_str());
    }
    if (deviceMqtt.connected()) return;
    if (now - lastMqttAttemptMs < MEDPULSE_MQTT_RECONNECT_INTERVAL_MS) return;
    lastMqttAttemptMs = now;
    const String clientId = String("MEDPULSE_DEVICE_") + MEDPULSE_DEVICE_ID;
    if (!deviceMqtt.connect(clientId.c_str())) return;
    const String commandTopic = medpulseDirectDeviceTopic(MEDPULSE_MQTT_ROOT_TOPIC, MEDPULSE_DEVICE_ID, "commands");
    deviceMqtt.subscribe(commandTopic.c_str(), 1);
    publishDirectStatus(true);
    Serial.println("[MQTT] Kenh du phong da ket noi.");
    return;
  }

  // Arduino-ESP32 3.x tu retry mot lan sau disconnect dau tien. Giu attempt
  // hoat dong them mot khoang settle de retry noi bo co the hoan tat; BLE van
  // duoc tam dung trong toan bo cua so nay.
  if (wifiConnectionAttemptActive) {
    if (wifiConnectionAttemptFailed
        && now - wifiLastDisconnectAtMs >= MEDPULSE_WIFI_DISCONNECT_SETTLE_MS) {
      const uint8_t reason = wifiLastDisconnectReason;
      wifiConnectionAttemptFailed = false;
      wifiConnectionAttemptActive = false;
      lastWifiAttemptMs = now;
      WiFi.disconnect(false, false);
      resumeBleAdvertisingAfterWifiAttempt();
      Serial.printf("[WiFiDiag #%lu] FAIL sau %lums: reason=%u/%s, lastRSSI=%d, "
                    "events=%u, status=%d, mode=%d; thu lai sau %lus.\n",
          wifiAttemptSequence, now - wifiCycleStartedAtMs, reason, wifiDisconnectReasonName(reason),
          wifiLastDisconnectRssi, wifiDisconnectEventCount, static_cast<int>(wifiStatus),
          static_cast<int>(WiFi.getMode()),
          MEDPULSE_WIFI_RECONNECT_INTERVAL_MS / 1000UL);
      return;
    }

    if (now - wifiAttemptStartedAtMs < MEDPULSE_WIFI_CONNECT_TIMEOUT_MS) return;

    Serial.printf("[WiFiDiag #%lu] TIMEOUT sau %lums: status=%d, mode=%d, reason=%u/%s, "
                  "lastRSSI=%d, events=%u, scanActive=%d, coexWifi=%d.\n",
        wifiAttemptSequence, now - wifiCycleStartedAtMs, static_cast<int>(wifiStatus),
        static_cast<int>(WiFi.getMode()), wifiLastDisconnectReason,
        wifiDisconnectReasonName(wifiLastDisconnectReason), wifiLastDisconnectRssi,
        wifiDisconnectEventCount, wifiScanActive ? 1 : 0, medpulseWifiCoexPreferred ? 1 : 0);
    wifiConnectionAttemptFailed = false;
    wifiConnectionAttemptActive = false;
    lastWifiAttemptMs = now;
    WiFi.disconnect(false, false);
    resumeBleAdvertisingAfterWifiAttempt();
    return;
  }

  if (now - lastWifiAttemptMs < MEDPULSE_WIFI_RECONNECT_INTERVAL_MS) return;

  lastWifiAttemptMs = now;
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  configureWifiLikeStandaloneTest();
  const unsigned long attemptNow = millis();
  wifiAttemptSequence++;
  wifiCycleStartedAtMs = attemptNow;
  wifiDisconnectEventCount = 0;
  wifiLastDisconnectReason = 0;
  wifiLastDisconnectRssi = 0;
  wifiLastDisconnectAtMs = 0;
  printWifiStaticDiagnostics();
  Serial.printf("[WiFiDiag #%lu] CYCLE start: uptime=%lums, status=%d, mode=%d, "
                "heap=%lu, BLE_adv=%d, GATT=%d\n",
      wifiAttemptSequence, attemptNow, static_cast<int>(wifiStatus), static_cast<int>(WiFi.getMode()),
      static_cast<unsigned long>(ESP.getFreeHeap()), medpulseAdvertisingRunning ? 1 : 0,
      gattConnected ? 1 : 0);
  preferWifiRadioForAttempt();
  pauseBleAdvertisingForWifiAuth();
  Serial.printf("[WiFi] Dang quet SSID '%s'...\n", MEDPULSE_WIFI_SSID);
  wifiScanStartedAtMs = attemptNow;
  wifiScanActive = true;
  const int16_t scanResult = WiFi.scanNetworks(false, true);
  wifiScanActive = false;
  Serial.printf("[WiFiDiag #%lu] SCAN end sau %lums: result=%d, status=%d, heap=%lu\n",
      wifiAttemptSequence, millis() - wifiScanStartedAtMs, scanResult,
      static_cast<int>(WiFi.status()), static_cast<unsigned long>(ESP.getFreeHeap()));
  if (scanResult < 0) {
    WiFi.scanDelete();
    wifiScanStartedAtMs = 0;
    lastWifiAttemptMs = now;
    resumeBleAdvertisingAfterWifiAttempt();
    Serial.println("[WiFi] Quet AP that bai; se thu lai.");
    return;
  }

  int bestIndex = -1;
  int32_t bestRssi = -128;
  for (int index = 0; index < scanResult; index++) {
    if (WiFi.SSID(index) == MEDPULSE_WIFI_SSID && WiFi.RSSI(index) > bestRssi) {
      bestIndex = index;
      bestRssi = WiFi.RSSI(index);
    }
  }
  if (bestIndex < 0) {
    WiFi.scanDelete();
    wifiScanStartedAtMs = 0;
    lastWifiAttemptMs = now;
    resumeBleAdvertisingAfterWifiAttempt();
    Serial.printf("[WiFiDiag #%lu] Khong tim thay SSID '%s' trong %d ket qua.\n",
        wifiAttemptSequence, MEDPULSE_WIFI_SSID, scanResult);
    return;
  }

  uint8_t bestBssid[6] = {};
  WiFi.BSSID(bestIndex, bestBssid);
  const int32_t bestChannel = WiFi.channel(bestIndex);
  const wifi_auth_mode_t bestAuth = WiFi.encryptionType(bestIndex);
  Serial.printf("[WiFiDiag #%lu] AP: RSSI=%ld dBm, channel=%ld, auth=%d/%s, "
                "BSSID=%02X:%02X:%02X:%02X:%02X:%02X\n",
      wifiAttemptSequence, static_cast<long>(bestRssi), static_cast<long>(bestChannel),
      static_cast<int>(bestAuth), wifiAuthModeName(bestAuth),
      bestBssid[0], bestBssid[1], bestBssid[2], bestBssid[3], bestBssid[4], bestBssid[5]);
  WiFi.scanDelete();
  wifiScanStartedAtMs = 0;

  wifiAttemptStartedAtMs = millis();
  wifiConnectionAttemptActive = true;
  wifiConnectionAttemptFailed = false;
  wifiLastDisconnectReason = 0;
  wifiLastDisconnectAtMs = 0;
  wifiLastDisconnectRssi = 0;
  Serial.printf("[WiFiDiag #%lu] AUTH start: SSID='%s', scanChannel=%ld, connectChannel=auto, lockBSSID=0, uptime=%lums\n",
      wifiAttemptSequence, MEDPULSE_WIFI_SSID, static_cast<long>(bestChannel), wifiAttemptStartedAtMs);
  configureWifiLikeStandaloneTest();
  const wl_status_t beginStatus = WiFi.begin(
      MEDPULSE_WIFI_SSID, MEDPULSE_WIFI_PASSWORD, 0, nullptr, true);
  Serial.printf("[WiFiDiag #%lu] WiFi.begin returned status=%d, mode=%d\n",
      wifiAttemptSequence, static_cast<int>(beginStatus), static_cast<int>(WiFi.getMode()));
  printWifiStaConfigDiagnostics();
  if (beginStatus == WL_CONNECT_FAILED) {
    wifiConnectionAttemptActive = false;
    lastWifiAttemptMs = millis();
    WiFi.disconnect(false, false);
    resumeBleAdvertisingAfterWifiAttempt();
    Serial.println("[WiFi] WiFi.begin() bi tu choi ngay lap tuc.");
  }
}

void executePendingDeviceCommands() {
  if (!deviceCommandQueue) return;
  PendingDeviceCommand command;
  while (xQueueReceive(deviceCommandQueue, &command, 0) == pdTRUE) {
    if (commandTokenWasExecuted(command.token)) {
      if (command.viaMqtt) publishDirectCommandAck(command, "EXECUTED");
      else sendGattCommandAck(command.token, MedPulseCommandAckStatus::EXECUTED, MedPulseCommandAckError::NONE);
      continue;
    }
    const MedPulseCommandAction action = static_cast<MedPulseCommandAction>(command.action);
    if (action == MedPulseCommandAction::BUZZER_ON) {
      startSosBuzzer();
      remoteBuzzerStopAtMs = command.durationMs > 0 ? millis() + command.durationMs : 0;
      Serial.printf("[COMMAND] BUZZER_ON token=0x%08lx duration=%lu.\n",
          static_cast<unsigned long>(command.token), static_cast<unsigned long>(command.durationMs));
    } else if (action == MedPulseCommandAction::BUZZER_OFF) {
      remoteBuzzerStopAtMs = 0;
      stopBuzzerPattern();
      Serial.printf("[COMMAND] BUZZER_OFF token=0x%08lx.\n", static_cast<unsigned long>(command.token));
    } else if (action == MedPulseCommandAction::BUZZER_TEST) {
      buzzerMode = BUZZER_REMOTE_TEST;
      buzzerStepStart = millis();
      buzzerOn();
      remoteBuzzerStopAtMs = millis() + (command.durationMs > 0 ? command.durationMs : 1000UL);
      Serial.printf("[COMMAND] BUZZER_TEST token=0x%08lx duration=%lu.\n",
          static_cast<unsigned long>(command.token), static_cast<unsigned long>(command.durationMs));
    } else {
      if (command.viaMqtt) publishDirectCommandAck(command, "FAILED", "UNSUPPORTED_ACTION");
      else sendGattCommandAck(command.token, MedPulseCommandAckStatus::FAILED, MedPulseCommandAckError::UNSUPPORTED_ACTION);
      continue;
    }
    rememberExecutedCommandToken(command.token);
    if (command.viaMqtt) publishDirectCommandAck(command, "EXECUTED");
    else sendGattCommandAck(command.token, MedPulseCommandAckStatus::EXECUTED, MedPulseCommandAckError::NONE);
  }
}

void serviceConnectivity() {
  const unsigned long now = millis();
  executePendingDeviceCommands();
  if (remoteBuzzerStopAtMs != 0 && static_cast<long>(now - remoteBuzzerStopAtMs) >= 0) {
    remoteBuzzerStopAtMs = 0;
    if (!alertActive) stopBuzzerPattern();
  }

  if (gattConnected) {
    restoreBalancedRadioAfterWifiAttempt();
    if (deviceMqtt.connected()) {
      publishDirectStatus(false);
      deviceMqtt.disconnect();
    }
    if (WiFi.getMode() != WIFI_OFF) {
      // Danh dau attempt da ket thuc truoc khi chu dong tat Wi-Fi. Lan fallback
      // tiep theo se bat dau mot attempt moi sau khoang cho quy dinh.
      wifiConnectionAttemptActive = false;
      wifiConnectionAttemptFailed = false;
      wifiScanActive = false;
      wifiScanStartedAtMs = 0;
      WiFi.scanDelete();
      wifiLastDisconnectReason = 0;
      wifiLastDisconnectAtMs = 0;
      wifiAttemptStartedAtMs = 0;
      lastWifiAttemptMs = now;
      WiFi.disconnect(true);
      WiFi.mode(WIFI_OFF);
    }
  } else if (now - lastGattDisconnectedAtMs >= MEDPULSE_DIRECT_FALLBACK_DELAY_MS) {
    ensureDirectConnectivity();
    if (deviceMqtt.connected()) {
      deviceMqtt.loop();
      if (now - lastDirectStatusMs >= MEDPULSE_DIRECT_STATUS_INTERVAL_MS) publishDirectStatus(true);
    }
  }

  PendingConnectivityEvent pending;
  uint8_t handled = 0;
  while (connectivityEventQueue && handled < 4 && xQueueReceive(connectivityEventQueue, &pending, 0) == pdTRUE) {
    const bool sent = gattConnected ? notifyGattEvent(pending) : publishDirectEvent(pending);
    if (!sent) {
      xQueueSendToFront(connectivityEventQueue, &pending, 0);
      break;
    }
    handled++;
  }
}

void initConnectivity() {
  if (!isValidDeviceId(MEDPULSE_DEVICE_ID)) {
    Serial.println("[CONFIG] DEVICE_ID khong hop le; dung khoi dong ket noi.");
    return;
  }
  connectivityEventQueue = xQueueCreate(8, sizeof(PendingConnectivityEvent));
  deviceCommandQueue = xQueueCreate(4, sizeof(PendingDeviceCommand));
  commandPreferences.begin("medpulse", false);
  if (commandPreferences.getBytesLength("cmdTokens") == sizeof(commandTokenHistory)) {
    commandPreferences.getBytes("cmdTokens", commandTokenHistory, sizeof(commandTokenHistory));
  }
  commandTokenHistoryIndex = commandPreferences.getUChar("cmdIndex", 0) % COMMAND_TOKEN_HISTORY_SIZE;
  deviceMqtt.setServer(MEDPULSE_MQTT_HOST, MEDPULSE_MQTT_PORT);
  deviceMqtt.setCallback(handleDirectMqttMessage);
  deviceMqtt.setSocketTimeout(3);
  WiFi.onEvent(handleDeviceWiFiEvent);
  medpulseWifiConfigValid = isValidWifiConfig();
  if (!medpulseWifiConfigValid) {
    Serial.println("[CONFIG] SSID/mat khau Wi-Fi khong hop le; BLE van hoat dong.");
  }
  lastGattDisconnectedAtMs = millis();
  initializeBleTransport();
}

void sendPreAlertEvent() {
  enqueueConnectivityEvent(MedPulseEventType::PREALERT);
}

void sendAlertEvent(bool fallActive) {
  enqueueConnectivityEvent(fallActive ? MedPulseEventType::FALL_ALERT : MedPulseEventType::RECOVERED);
}

void sendPresenceHeartbeat() {
  static unsigned long lastEventHeartbeatMs = 0;
  const unsigned long now = millis();
  if (now - lastEventHeartbeatMs < MEDPULSE_GATT_EVENT_HEARTBEAT_MS) return;
  lastEventHeartbeatMs = now;
  enqueueConnectivityEvent(MedPulseEventType::HEARTBEAT);
}
