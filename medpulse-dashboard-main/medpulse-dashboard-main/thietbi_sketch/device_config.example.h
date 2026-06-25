#pragma once

// Sao chep file nay thanh device_config.h va thay gia tri cho tung thiet bi.
// DEVICE_ID phai duy nhat, dai toi da 20 ky tu, khong doi sau khi nap
// va chi gom A-Z, a-z, 0-9, - hoac _.

#define MEDPULSE_DEVICE_ID             "MINI-A1B2C3"
#define MEDPULSE_WIFI_SSID             "YOUR_WIFI_NAME"
#define MEDPULSE_WIFI_PASSWORD         "YOUR_WIFI_PASSWORD"
#define MEDPULSE_MQTT_HOST             "broker.hivemq.com"
#define MEDPULSE_MQTT_PORT             1883
#define MEDPULSE_MQTT_ROOT_TOPIC       "medpulse_duy"

// Thiet bi hien tai khong co mach do pin. Giu -1 de he thong bao "chua co du lieu".
// Chi dat 0..100 sau khi da noi va hieu chuan mach ADC pin that.
#define MEDPULSE_FIXED_BATTERY_PERCENT -1

// Sau khi mat GATT voi tram, cho het khoang nay moi bat Wi-Fi/MQTT du phong.
#define MEDPULSE_DIRECT_FALLBACK_DELAY_MS 15000UL
