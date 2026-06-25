# Cấu hình thiết bị đeo MedPulse

## 1. Phần cứng và board

- Board: ESP32-C3 Super Mini; trong Arduino IDE chọn `ESP32C3 Dev Module`.
- Chọn `Tools > Partition Scheme > Huge APP (3MB No OTA/1MB SPIFFS)`; BLE + Wi-Fi chiếm gần hết partition mặc định 1.2 MB.
- MPU6050: SDA GPIO 8, SCL GPIO 9, INT GPIO 4.
- Buzzer active-LOW: GPIO 5.
- Firmware hiện không có cảm biến nhịp tim/SpO₂/nhiệt độ và không có mạch ADC pin. Không phát dữ liệu giả.

## 2. Thư viện

Cài `esp32 by Espressif Systems` 3.x, `ArduinoJson` 7.x, `PubSubClient` và thư viện I2Cdevlib có `MPU6050.h`.
BLE, Wi-Fi và Preferences lấy từ ESP32 board package; không cài thêm thư viện `ESP32 BLE Arduino` bên ngoài.

## 3. Tạo cấu hình riêng

Trong thư mục `thietbi_sketch`, sao chép:

```text
device_config.example.h -> device_config.h
```

Sửa các giá trị:

```cpp
#define MEDPULSE_DEVICE_ID       "MINI-A1B2C3"
#define MEDPULSE_WIFI_SSID       "TEN_WIFI"
#define MEDPULSE_WIFI_PASSWORD   "MAT_KHAU_WIFI"
#define MEDPULSE_MQTT_HOST       "broker.hivemq.com"
#define MEDPULSE_MQTT_PORT       1883
#define MEDPULSE_MQTT_ROOT_TOPIC "medpulse_duy"
```

`DEVICE_ID` phải duy nhất, cố định, dài tối đa 20 ký tự và chỉ chứa chữ, số, `-`, `_`. Không ghi `PATIENT_ID` vào thiết bị; việc gán thiết bị cho bệnh nhân thực hiện trên dashboard.

`device_config.h` đã được `.gitignore`; không commit Wi-Fi/password.

## 4. Cơ chế truyền

1. Thiết bị quảng bá tên `MEDPULSE-<DEVICE_ID>` cùng manufacturer frame.
2. Trạm kết nối GATT, nhận `HEARTBEAT`, `PREALERT`, `FALL_ALERT`, `RECOVERED` và gửi lệnh còi.
3. Khi mất GATT 15 giây, thiết bị bật Wi-Fi/MQTT làm đường dự phòng.
4. Khi GATT trở lại, thiết bị báo MQTT offline rồi tắt Wi-Fi để ưu tiên BLE.
5. Token lệnh gần nhất được lưu trong NVS để cùng một lệnh không bật còi hai lần qua BLE và MQTT.
6. Nếu MPU6050 lỗi, BLE/MQTT vẫn hoạt động, thiết bị gửi `HARDWARE_FAULT` và thử khởi tạo lại mỗi 30 giây.

Topic trực tiếp:

```text
medpulse_duy/devices/<DEVICE_ID>/status
medpulse_duy/devices/<DEVICE_ID>/events
medpulse_duy/devices/<DEVICE_ID>/commands
medpulse_duy/devices/<DEVICE_ID>/acks
```

## 5. Nạp và kiểm tra

Mở đúng `thietbi_sketch.ino`, kiểm tra có các tab `device_config.h`, `device_connectivity.h`, `medpulse_protocol.h`, sau đó Verify và Upload.

Serial Monitor 115200 phải thấy:

```text
[BLE] Advertising MEDPULSE-MINI-A1B2C3
[BLE] Tram da ket noi GATT.
```

Khi tắt trạm và chờ 15 giây:

```text
[MQTT] Kenh du phong da ket noi.
```

Trên MQTT Explorer có thể theo dõi `medpulse_duy/devices/<DEVICE_ID>/#`. Tất cả message phải để retain tắt.
