#pragma once

// Sao chép file này thành station_config.h, sau đó thay các giá trị bên dưới.
// station_config.h đã được .gitignore để không đẩy mật khẩu Wi-Fi lên Git.

#define MEDPULSE_WIFI_SSID        "YOUR_WIFI_NAME"
#define MEDPULSE_WIFI_PASSWORD    "YOUR_WIFI_PASSWORD"

#define MEDPULSE_STATION_ID       "STATION-01"
#define MEDPULSE_MQTT_HOST        "broker.hivemq.com"
#define MEDPULSE_MQTT_PORT        1883
#define MEDPULSE_MQTT_ROOT_TOPIC  "medpulse_duy"
