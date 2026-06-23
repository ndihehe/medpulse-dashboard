# Cấu hình thiết bị di động MedPulse

Tài liệu này là hợp đồng giao tiếp MedPulse v1 giữa thiết bị di động, trạm ESP32, MQTT, backend và dashboard. Không tự thay đổi tên, topic, thứ tự byte hoặc đơn vị.

## 1. Quy tắc đặt tên

- `DEVICE_ID`: 1–24 ký tự, chỉ gồm `A-Z`, `a-z`, `0-9`, `-`, `_`.
- Mỗi bo mạch phải có `DEVICE_ID` duy nhất và không đổi sau khi khởi động lại.
- Khuyến nghị: `MINI-` + 6 ký tự cuối của MAC, ví dụ `MINI-A1B2C3`.
- BLE Local Name bắt buộc: `MEDPULSE-<DEVICE_ID>`, ví dụ `MEDPULSE-MINI-A1B2C3`.
- `STATION_ID`: 1–50 ký tự cùng tập ký tự trên, ví dụ `STATION-01`.
- `PATIENT_ID` không được ghi cứng trong thiết bị BLE. Việc gán thiết bị–bệnh nhân thực hiện trên dashboard/backend.

## 2. Chế độ BLE qua trạm — chế độ khuyến nghị

Thiết bị di động là BLE Advertiser. Trạm là BLE Scanner và MQTT Gateway. Không pair, không bonding và không cần GATT trong giao thức v1.

Thiết bị nên advertising mỗi 500–1000 ms. Trạm chỉ nhận gói có Local Name đúng prefix `MEDPULSE-` và Manufacturer Data hợp lệ.

### Frame hiện diện

5 byte:

| Byte | Giá trị | Ý nghĩa |
|---|---:|---|
| 0 | `0x4D` | `M` |
| 1 | `0x50` | `P` |
| 2 | `0x01` | Phiên bản BLE frame |
| 3 | `0x00` | Loại frame hiện diện |
| 4 | `0..100` | Pin phần trăm |

Ví dụ pin 92%: `4D 50 01 00 5C`.

### Frame sinh hiệu

12 byte:

| Byte | Kiểu | Ý nghĩa |
|---|---|---|
| 0 | `0x4D` | `M` |
| 1 | `0x50` | `P` |
| 2 | `0x01` | Phiên bản BLE frame |
| 3 | `0x10` | Loại frame sinh hiệu |
| 4 | `uint8` | `sequence`, tăng sau mỗi lần đo, cho phép quay vòng 255→0 |
| 5 | `uint8` | Pin `0..100` |
| 6 | `uint8` | Nhịp tim `25..240 bpm` |
| 7 | `uint8` | SpO₂ `50..100%` |
| 8–9 | `int16 LE` | Nhiệt độ °C nhân 10; 36,5°C = 365 = `6D 01` |
| 10 | bit field | bit0 té ngã; bit1 đang sạc; bit2 dữ liệu đo hợp lệ |
| 11 | `0x00` | Dự phòng, bắt buộc ghi 0 |

Bit2 phải bằng 1. Trạm bỏ qua frame có giá trị ngoài phạm vi hoặc bit2 bằng 0.

Ví dụ: sequence 7, pin 92, nhịp tim 72, SpO₂ 98, 36,5°C, không té ngã, không sạc:

```text
4D 50 01 10 07 5C 48 62 6D 01 04 00
```

Mã tạo frame:

```cpp
uint8_t frame[12] = {
  0x4D, 0x50, 0x01, 0x10,
  sequence,
  batteryPercent,
  heartRate,
  spo2,
  static_cast<uint8_t>(tempX10 & 0xFF),
  static_cast<uint8_t>((tempX10 >> 8) & 0xFF),
  static_cast<uint8_t>((fallDetected ? 0x01 : 0) |
                       (charging ? 0x02 : 0) |
                       0x04),
  0x00
};
```

Chỉ tăng `sequence` khi có kết quả đo mới. Việc phát lặp lại cùng sequence giúp chống mất sóng; trạm và backend sẽ không lưu trùng.

## 3. MQTT do trạm phát

Broker mặc định hiện tại:

```text
Host: broker.hivemq.com
TCP port: 1883
Root topic: medpulse_duy
Retain: false
QoS: 0
```

Cấu hình trạm: sao chép `station_config.example.h` thành `station_config.h`, sau đó khai báo các macro sau. `station_config.h` được bỏ qua bởi Git để không lộ mật khẩu Wi-Fi.

```cpp
#define MEDPULSE_WIFI_SSID        "YOUR_WIFI_NAME"
#define MEDPULSE_WIFI_PASSWORD    "YOUR_WIFI_PASSWORD"
#define MEDPULSE_STATION_ID       "STATION-01"
#define MEDPULSE_MQTT_HOST        "broker.hivemq.com"
#define MEDPULSE_MQTT_PORT        1883
#define MEDPULSE_MQTT_ROOT_TOPIC  "medpulse_duy"
```

Trên backend/Railway cấu hình:

```text
MQTT_BROKER_URL=ws://broker.hivemq.com:8000/mqtt
MQTT_ROOT_TOPIC=medpulse_duy
BLE_DEVICE_SEEN_STALE_MS=45000
BLE_ALERT_TIMEOUT_MS=180000
STATION_OFFLINE_TIMEOUT_MS=90000
APP_TIME_ZONE=Asia/Ho_Chi_Minh
```

`MQTT_ROOT_TOPIC` phải trùng tuyệt đối giữa firmware và backend. FE không kết nối MQTT; FE nhận REST/WebSocket từ chính host đang phục vụ `web.html`, do đó không cần khai báo broker trên trình duyệt.

### Trạng thái BLE

Topic:

```text
medpulse_duy/stations/<STATION_ID>/ble
```

Payload:

```json
{
  "protocolVersion": 1,
  "stationId": "STATION-01",
  "deviceId": "MINI-A1B2C3",
  "event": "SEEN",
  "bleStatus": "CONNECTED",
  "rssi": -65,
  "battery": 92,
  "stationUptimeMs": 31000,
  "lastSeenAgoMs": 20
}
```

Cặp hợp lệ: `SEEN/CONNECTED` và `LOST/DISCONNECTED`.

### Sinh hiệu BLE được gateway chuyển tiếp

Topic:

```text
medpulse_duy/stations/<STATION_ID>/devices/<DEVICE_ID>/vitals
```

Payload do trạm tạo:

```json
{
  "protocolVersion": 1,
  "stationId": "STATION-01",
  "deviceId": "MINI-A1B2C3",
  "sequence": 7,
  "heartRate": 72,
  "spo2": 98,
  "temp": 36.5,
  "battery": 92,
  "charging": false,
  "fallDetected": false,
  "firmwareStatus": "IDLE",
  "rssi": -65,
  "stationUptimeMs": 31000
}
```

Backend chỉ lưu khi:

1. `stationId` và `deviceId` khớp chính xác với topic.
2. Thiết bị đã được gán cho bệnh nhân.
3. Phiên thiết bị đang hoạt động.
4. Payload hợp lệ và sequence chưa được xử lý.

### Heartbeat trạm

Topic: `medpulse_duy/stations/<STATION_ID>/heartbeat`. Trạm phát 30 giây/lần. Backend coi trạm offline sau 90 giây không có heartbeat.

## 4. Chế độ thiết bị gửi MQTT trực tiếp

Chỉ dùng khi thiết bị có Wi-Fi và biết `PATIENT_ID` đã đăng ký.

Topic: `medpulse_duy/<PATIENT_ID>/vitals`.

```json
{
  "heartRate": 72,
  "spo2": 98,
  "temp": 36.5,
  "firmwareStatus": "IDLE",
  "battery": 92,
  "charging": false,
  "signalLost": false,
  "rssi": -65
}
```

Không publish dữ liệu mặc định khi chưa đo. Không bật retained message.

## 5. Lệnh server gửi xuống

BLE Advertising v1 là kênh một chiều, vì vậy không thể nhận lệnh từ trạm. Các topic sau được dành trước cho phiên bản GATT/MQTT hai chiều, nhưng chưa được firmware trạm v1 xử lý:

```text
medpulse_duy/devices/<DEVICE_ID>/commands
medpulse_duy/devices/<DEVICE_ID>/responses
```

Không triển khai thiết bị dựa vào hai topic này cho đến khi giao thức v2 được bổ sung.

## 6. Trình tự kiểm tra

1. Nạp firmware trạm, mở Serial Monitor 115200 baud.
2. Bật thiết bị với tên `MEDPULSE-<DEVICE_ID>` và frame hiện diện.
3. Xác nhận trạm in `[BLE MQTT] SEEN ... -> OK`.
4. Trên dashboard, gán `DEVICE_ID` cho đúng bệnh nhân và bắt đầu phiên.
5. Thiết bị phát frame sinh hiệu với sequence mới.
6. Xác nhận trạm in `[BLE VITALS MQTT] ... -> OK`.
7. Dashboard phải hiển thị nguồn `BLE qua STATION-01`; `vitals_log.measurement_source` phải là `BLE_GATEWAY`.

## 7. Môi trường biên dịch trạm

Sử dụng thư viện BLE đi kèm board package `esp32 by Espressif Systems`, hỗ trợ Arduino-ESP32 2.x/3.x. Không cài thêm thư viện `ESP32 BLE Arduino` bên ngoài có header trùng tên.

Broker public không phù hợp cho triển khai y tế thực. Khi chuyển sang production phải dùng TLS, username/password, ACL theo station/device và root topic riêng.
