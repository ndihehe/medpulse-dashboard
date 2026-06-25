# Cấu hình thiết bị di động MedPulse

Tài liệu này là hợp đồng giao tiếp MedPulse v1 giữa thiết bị di động, trạm ESP32, MQTT, backend và dashboard. Không tự thay đổi tên, topic, thứ tự byte hoặc đơn vị.

Trong firmware trạm, `medpulse_protocol.h` là nguồn chuẩn duy nhất cho version, prefix, frame type, UUID, timeout và hàm tạo topic. Không khai báo lặp lại các giá trị này trong sketch.

Phân lớp version:

- Telemetry/Advertising hiện tại: protocol v1 để tương thích backend đang chạy.
- Command, ACK và transport fallback: protocol v2.
- Trạm chỉ chuyển tiếp; backend mới được ánh xạ `deviceId` sang bệnh nhân và đánh giá cảnh báo y tế.
- Không tạo nhịp tim, SpO₂ hay nhiệt độ giả cho thẻ chỉ có MPU6050.

Trạm duy trì tối đa 3 kết nối GATT đồng thời. Việc connect/discover chạy trong worker FreeRTOS riêng để vòng lặp đo RFID, MAX30102, MLX90614 và MQTT không bị chặn. Khi mất kết nối, trạm chuyển sang backoff 5 giây rồi mới thử lại; scanner được tạm dừng trong thao tác GATT và tự chạy lại sau đó.

## 1. Quy tắc đặt tên

- `DEVICE_ID`: 1–24 ký tự, chỉ gồm `A-Z`, `a-z`, `0-9`, `-`, `_`.
- Mỗi bo mạch phải có `DEVICE_ID` duy nhất và không đổi sau khi khởi động lại.
- Khuyến nghị: `MINI-` + 6 ký tự cuối của MAC, ví dụ `MINI-A1B2C3`.
- BLE Local Name bắt buộc: `MEDPULSE-<DEVICE_ID>`, ví dụ `MEDPULSE-MINI-A1B2C3`.
- `STATION_ID`: 1–50 ký tự cùng tập ký tự trên, ví dụ `STATION-01`.
- `PATIENT_ID` không được ghi cứng trong thiết bị BLE. Việc gán thiết bị–bệnh nhân thực hiện trên dashboard/backend.

## 2. Chế độ BLE qua trạm — chế độ khuyến nghị

Telemetry v1 dùng BLE Advertising. Command v2 dùng GATT Write khi có trạm và MQTT trực tiếp khi thiết bị mất BLE.

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
BLE_GATT_READY_STALE_MS=45000
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

## 5. Sự kiện GATT Notify từ thiết bị

Thiết bị cung cấp Event characteristic có thuộc tính Notify:

```text
Service UUID: 7e400001-b5a3-f393-e0a9-e50e24dcca9e
Event characteristic UUID: 7e400002-b5a3-f393-e0a9-e50e24dcca9e
```

Mỗi Notify là đúng 12 byte, không dùng JSON để vẫn hoạt động với MTU BLE mặc định:

| Byte | Nội dung |
|---|---|
| 0..1 | Magic `4D 50` (`MP`) |
| 2 | Protocol version `02` |
| 3 | Frame type `20` |
| 4 | Sequence `0..255`, tăng sau mỗi sự kiện và cho phép quay vòng |
| 5 | Event: `0=HEARTBEAT`, `1=PREALERT`, `2=FALL_ALERT`, `3=RECOVERED`, `4=HARDWARE_FAULT`, `5=HARDWARE_RECOVERED` |
| 6 | Pin `0..100`, hoặc `FF` nếu không biết |
| 7 | Flags: bit0 té ngã, bit1 đang sạc, bit2 còi đang bật |
| 8..11 | Uptime thiết bị theo giây, unsigned 32-bit little-endian |

Trạm loại frame sai độ dài, magic, version, event, pin hoặc flags. Sequence trùng và gói cũ đến trễ cũng bị loại. Callback BLE chỉ giải mã và đưa vào queue; xử lý nghiệp vụ diễn ra ngoài callback để không chặn BLE stack.

Trạm chuyển sự kiện hợp lệ lên MQTT, không retained:

```text
medpulse_duy/stations/<STATION_ID>/devices/<DEVICE_ID>/events
```

```json
{
  "protocolVersion": 2,
  "stationId": "STATION-01",
  "deviceId": "MINI-A1B2C3",
  "sequence": 42,
  "event": "FALL_ALERT",
  "battery": 92,
  "fallDetected": true,
  "charging": false,
  "buzzerActive": true,
  "deviceUptimeSeconds": 3600,
  "rssi": -65,
  "stationUptimeMs": 7200000
}
```

Nếu publish thất bại tức thời, trạm đưa sự kiện về đầu queue và thử lại. Chỉ sau khi publish thành công trạm mới ghi nhận sequence. Backend tiếp tục chống trùng bằng cặp sequence/uptime được lưu trong `ble_devices`, vì vậy việc trạm gửi lại không tạo cảnh báo lặp.

Ánh xạ nghiệp vụ backend: `HEARTBEAT` chỉ cập nhật online, `PREALERT` chuyển trạng thái cảnh báo sớm, `FALL_ALERT` chuyển trạng thái nguy hiểm, `RECOVERED` đưa trạng thái té ngã về bình thường. `HARDWARE_FAULT` và `HARDWARE_RECOVERED` cập nhật tình trạng MPU6050 nhưng không giả thành cảnh báo té ngã. Sự kiện không chứa sinh hiệu nên không được ghi thành một lần đo trong `vitals_log`.

## 6. Lệnh server gửi xuống

Thiết bị phải cung cấp GATT service/characteristic sau và cho phép Write With Response:

```text
Service UUID: 7e400001-b5a3-f393-e0a9-e50e24dcca9e
Command characteristic UUID: 7e400003-b5a3-f393-e0a9-e50e24dcca9e
```

Topic cloud gửi qua trạm:

```text
medpulse_duy/stations/<STATION_ID>/devices/<DEVICE_ID>/commands
```

Topic cloud gửi trực tiếp khi thiết bị đã chuyển sang Wi-Fi:

```text
medpulse_duy/devices/<DEVICE_ID>/commands
```

Thiết bị direct MQTT phải gửi heartbeat tối thiểu 30 giây/lần:

```text
Topic: medpulse_duy/devices/<DEVICE_ID>/status
{"protocolVersion":2,"deviceId":"MINI-A1B2C3","transport":"DIRECT_MQTT","online":true,"wifiRssi":-62}
```

Khi mất GATT, sự kiện té ngã/phục hồi được gửi trực tiếp, không retained:

```text
Topic: medpulse_duy/devices/<DEVICE_ID>/events
{"protocolVersion":2,"deviceId":"MINI-A1B2C3","sequence":7,"event":"FALL_ALERT","battery":-1,"fallDetected":true,"charging":false,"buzzerActive":true,"deviceUptimeSeconds":120}
```

Trạm phát trạng thái GATT ngay khi connect/disconnect và lặp lại mỗi 30 giây khi GATT sẵn sàng:

```text
Topic: medpulse_duy/stations/<STATION_ID>/devices/<DEVICE_ID>/transport
{"protocolVersion":2,"stationId":"STATION-01","deviceId":"MINI-A1B2C3","transport":"BLE_GATEWAY","online":true,"gattReady":true,"rssi":-64,"stationUptimeMs":120000}
```

Backend chọn duy nhất một transport chủ động theo thứ tự:

1. `BLE_GATEWAY` khi trạm online và có heartbeat GATT mới trong 45 giây.
2. `DIRECT_MQTT` khi BLE không sẵn sàng và heartbeat MQTT trực tiếp còn hạn.
3. `OFFLINE` khi cả hai kênh đều không sẵn sàng.

Nếu cả BLE và MQTT trực tiếp cùng online, backend luôn ưu tiên BLE để tránh gửi một lệnh qua hai kênh. Khi phiên theo dõi bị kết thúc, backend không chọn transport nào dù thiết bị vẫn còn kết nối vật lý.

Nếu BLE mất trong lúc lệnh đang `DISPATCHED` hoặc `DELIVERED` mà MQTT trực tiếp đã online, backend gửi lại chính `commandId` đó qua kênh trực tiếp. Bộ nhớ chống trùng trên thiết bị phải dùng chung cho cả BLE và MQTT để một lệnh chỉ tác động phần cứng một lần.

Thiết bị di động tự quản lý fallback: khi BLE đang kết nối với trạm thì dùng GATT; sau khi mất BLE mới bật Wi-Fi/MQTT, subscribe topic lệnh trực tiếp và gửi `DIRECT_MQTT online` mỗi 30 giây. Khi BLE kết nối lại, thiết bị gửi `DIRECT_MQTT online=false` trước khi ngắt MQTT. Cloud không thể ra lệnh chuyển kênh cho một thiết bị đang hoàn toàn offline.

Payload command v2:

```json
{
  "protocolVersion": 2,
  "commandId": "CMD-1710000000000-A1B2C3",
  "deviceId": "MINI-A1B2C3",
  "action": "BUZZER_ON",
  "durationMs": 10000,
  "expiresAt": "2026-06-23T10:30:00.000Z"
}
```

Action hợp lệ: `BUZZER_ON`, `BUZZER_OFF`, `BUZZER_TEST`. Thiết bị phải lưu `commandId` gần nhất để không thực thi trùng.

JSON chỉ tồn tại trên MQTT. Trạm không ghi nguyên JSON xuống BLE vì payload có thể vượt MTU mặc định. Trạm băm `commandId` thành token FNV-1a 32-bit và ghi frame nhị phân đúng 16 byte vào Command characteristic:

| Byte | Nội dung |
|---|---|
| 0..1 | Magic `4D 50` |
| 2 | Protocol version `02` |
| 3 | Frame type `30` |
| 4..7 | Command token uint32 little-endian |
| 8 | Action: `1=BUZZER_ON`, `2=BUZZER_OFF`, `3=BUZZER_TEST` |
| 9 | Reserved, bắt buộc `00` |
| 10..13 | `durationMs` uint32 little-endian |
| 14..15 | CRC16-CCITT của byte 0..13, little-endian |

Thiết bị phải dùng Write With Response, kiểm tra toàn bộ frame và chỉ ACK `EXECUTED` sau khi đã áp dụng lệnh. ACK được Notify qua Event characteristic bằng frame 12 byte:

| Byte | Nội dung |
|---|---|
| 0..1 | Magic `4D 50` |
| 2 | Protocol version `02` |
| 3 | Frame type `31` |
| 4..7 | Command token nhận từ frame lệnh |
| 8 | Status: `1=EXECUTED`, `2=FAILED` |
| 9 | Error: `0=NONE`, `1=INVALID_FRAME`, `2=UNSUPPORTED_ACTION`, `3=EXECUTION_FAILED`, `4=DUPLICATE_COMMAND` |
| 10..11 | CRC16-CCITT của byte 0..9, little-endian |

Thiết bị lưu các token gần nhất. Nếu nhận lại token đã thực thi thành công, không bật còi lần nữa và trả lại ACK `EXECUTED`. Trạm đối chiếu đồng thời token và `deviceId`; ACK sai thiết bị, sai token hoặc sai CRC bị bỏ. Sau 15 giây không có ACK, trạm báo `FAILED/DEVICE_ACK_TIMEOUT` cho backend. Lệnh nằm trong queue trạm quá 60 giây bị từ chối để không thực thi một cảnh báo đã cũ.

Vector kiểm tra liên thông: với `commandId=CMD-1710000000000-A1B2C3`, action `BUZZER_ON`, `durationMs=10000`, token phải là `0x43395424`. Frame command là `4D50023024543943010010270000A919`; ACK `EXECUTED` tương ứng là `4D500231245439430100BA7E`.
Frame command/ACK là dữ liệu nhị phân và có byte `00`, nên firmware thiết bị phải đọc bằng buffer + length thật (`getData()`/`getLength()` trên Arduino-ESP32 3.x). Không đọc command qua `String`/`c_str()` vì có thể làm hỏng hoặc cắt frame trước khi decode.

ACK dùng chung cho cả hai transport:

```text
Topic: medpulse_duy/devices/<DEVICE_ID>/acks
{"protocolVersion":2,"commandId":"CMD-1710000000000-A1B2C3","deviceId":"MINI-A1B2C3","status":"EXECUTED","transport":"DIRECT_MQTT"}
```

Trạm dùng `transport=BLE_GATEWAY` và thêm `stationId`. `DELIVERED` chỉ có nghĩa Write With Response thành công; `EXECUTED` chỉ được gửi sau ACK thật từ thiết bị; `FAILED` dùng khi write lỗi, thiết bị từ chối hoặc hết timeout.

Backend ưu tiên BLE nếu trạm và thiết bị đang online. Nếu GATT thất bại hoặc lệnh đã `DISPATCHED`/`DELIVERED` nhưng không có ACK terminal sau `DEVICE_COMMAND_ACK_TIMEOUT_MS` (mặc định 20 giây), backend đánh giá lại transport và gửi lại cùng `commandId` qua kênh đang online (`BLE_GATEWAY` hoặc `DIRECT_MQTT`). Nếu chưa có kênh, lệnh được giữ `PENDING` và retry định kỳ cho tới khi có transport hoặc hết hạn 5 phút.

## 7. Phục hồi lỗi và chẩn đoán

- Wi-Fi retry tối đa một lần mỗi 10 giây; MQTT retry tối đa một lần mỗi 5 giây. Không có vòng `while` chờ mạng trong `loop()`.
- MQTT socket timeout là 3 giây. Mất cloud không dừng BLE scan, GATT hay việc đọc cảm biến.
- MAX30102, MLX90614 hoặc RC522 lỗi khi khởi động không làm treo trạm. Trạm chạy ở chế độ degraded và thử khởi tạo lại mỗi 30 giây khi không có phiên đo.
- Phiên đo không thể giữ trạm vô thời hạn: sau 180 giây chưa hoàn tất hoặc chưa gửi được, trạm hủy phiên và sẵn sàng nhận thẻ mới.
- Gói đo tại trạm chỉ được gửi sau khi nhịp tim, SpO₂ và nhiệt độ đều được đo hợp lệ. Không tạo giá trị pin giả khi chưa có mạch ADC pin.
- Heartbeat trạm có thêm free heap, số thiết bị theo dõi, số GATT ready, frame lỗi/rơi và trạng thái ba cảm biến để chẩn đoán.
- Sau MQTT reconnect, trạm phát lại presence và transport snapshot ngay, không chờ chu kỳ tiếp theo.
- Khi test lệnh còi, kiểm tra serial theo thứ tự: backend `Gui lenh thiet bi`, trạm `[COMMAND MQTT] Da nhan lenh`, trạm `[COMMAND] ... -> DELIVERED`, thiết bị `[COMMAND GATT] Da nhan lenh`, thiết bị `[COMMAND GATT] Da gui ACK`, trạm `[COMMAND ACK MQTT] ... -> PUB`, backend `Nhan ACK lenh`.

Chạy kiểm tra contract không cần phần cứng, broker hoặc database:

```bash
npm test
```

## 8. Trình tự kiểm tra phần cứng

1. Nạp firmware trạm, mở Serial Monitor 115200 baud.
2. Bật thiết bị với tên `MEDPULSE-<DEVICE_ID>` và frame hiện diện.
3. Xác nhận trạm in `[BLE MQTT] SEEN ... -> OK`.
4. Trên dashboard, gán `DEVICE_ID` cho đúng bệnh nhân và bắt đầu phiên.
5. Thiết bị phát frame sinh hiệu với sequence mới.
6. Xác nhận trạm in `[BLE VITALS MQTT] ... -> OK`.
7. Dashboard phải hiển thị nguồn `BLE qua STATION-01`; `vitals_log.measurement_source` phải là `BLE_GATEWAY`.

## 9. Môi trường biên dịch trạm

Sử dụng thư viện BLE đi kèm board package `esp32 by Espressif Systems`, hỗ trợ Arduino-ESP32 2.x/3.x, và `ArduinoJson` 7.x. Không cài thêm thư viện `ESP32 BLE Arduino` bên ngoài có header trùng tên.

Broker public không phù hợp cho triển khai y tế thực. Khi chuyển sang production phải dùng TLS, username/password, ACL theo station/device và root topic riêng.
