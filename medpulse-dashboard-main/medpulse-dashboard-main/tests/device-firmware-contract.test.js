'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sketch = read('thietbi_sketch/thietbi_sketch.ino');
const connectivity = read('thietbi_sketch/device_connectivity.h');
const protocol = read('thietbi_sketch/medpulse_protocol.h');
const stationProtocol = read('tram_sketch/medpulse_protocol.h');
const configExample = read('thietbi_sketch/device_config.example.h');
const station = read('tram_sketch/tram_sketch.ino');
const server = read('server.js');
const web = read('web.html');
const schema = read('medpulse_db.sql');

assert.match(sketch, /#include "device_config\.h"/);
assert.match(sketch, /#include "device_connectivity\.h"/);
assert.match(sketch, /serviceConnectivity\(\);[\s\S]*if \(!shouldSample\) return/,
    'Kết nối phải tiếp tục chạy cả khi chưa tới chu kỳ MPU');
assert.match(sketch, /sendPreAlertEvent\(\)/);
assert.doesNotMatch(sketch, /TODO:\s*(khởi tạo BLE|gửi tín hiệu té ngã|gửi tín hiệu "thiết bị còn hoạt động")/);

assert.match(configExample, /MEDPULSE_DEVICE_ID/);
assert.match(configExample, /MEDPULSE_FIXED_BATTERY_PERCENT\s+-1/);
assert.match(protocol, /MEDPULSE_GATT_SERVICE_UUID/);
assert.match(protocol, /medpulseCrc16Ccitt/);
assert.match(protocol, /medpulseFnv1a/);
for (const symbol of [
    'MEDPULSE_COMMAND_PROTOCOL_VERSION', 'MEDPULSE_FRAME_MAGIC_M', 'MEDPULSE_FRAME_MAGIC_P',
    'MEDPULSE_FRAME_TYPE_EVENT', 'MEDPULSE_FRAME_TYPE_COMMAND', 'MEDPULSE_FRAME_TYPE_COMMAND_ACK',
    'MEDPULSE_GATT_SERVICE_UUID', 'MEDPULSE_GATT_EVENT_UUID', 'MEDPULSE_GATT_COMMAND_UUID',
]) {
    const pattern = new RegExp(`${symbol}[^=]*=\\s*([^;]+);`);
    assert.equal(protocol.match(pattern)?.[1], stationProtocol.match(pattern)?.[1], `${symbol} bị lệch giữa trạm và thiết bị`);
}

assert.match(connectivity, /MEDPULSE_FRAME_TYPE_PRESENCE/);
assert.match(connectivity, /MEDPULSE_FRAME_TYPE_EVENT/);
assert.match(connectivity, /MEDPULSE_FRAME_TYPE_COMMAND_ACK/);
assert.match(connectivity, /decodeGattCommand/);
assert.match(connectivity, /ESP_ARDUINO_VERSION_MAJOR >= 3[\s\S]*getData\(\)[\s\S]*getLength\(\)/,
    'Arduino-ESP32 3.x phải đọc GATT command bằng buffer nhị phân, không qua String/c_str');
assert.match(connectivity, /event\.flags =/);
assert.match(connectivity, /notifyGattEvent\(const PendingConnectivityEvent& event\)/,
    'Sự kiện phải giữ snapshot tại thời điểm phát sinh');
assert.match(connectivity, /BUZZER_REMOTE_TEST/);
assert.match(connectivity, /commandPreferences\.putBytes/,
    'Token chống trùng phải tồn tại qua lần khởi động lại');
assert.match(connectivity, /MEDPULSE_DIRECT_FALLBACK_DELAY_MS/);
assert.match(connectivity, /medpulseDirectDeviceTopic[\s\S]*"events"/);
assert.match(connectivity, /command\.token == 0[\s\S]*INVALID_COMMAND_TOKEN/,
    'Direct MQTT command phải từ chối token 0 để chống trùng hoạt động đúng');
assert.doesNotMatch(connectivity, /"heartRate"|"spo2"|"temp"/,
    'Thiết bị chỉ có MPU6050 không được tạo sinh hiệu giả');
assert.doesNotMatch(connectivity, /WiFi\.disconnect\(true,\s*true\);\s*delay\(/,
    'Direct fallback không được block loop sau WiFi.disconnect');

assert.match(station, /raw\[4\] != MEDPULSE_BATTERY_UNKNOWN/,
    'Trạm phải chấp nhận thiết bị chưa có mạch đo pin');
assert.match(station, /medpulseDirectDeviceTopic\(MQTT_ROOT_TOPIC, command\.deviceId, "acks"\)/,
    'ACK lệnh qua trạm phải publish lên topic devices/<id>/acks để backend nhận chung với direct MQTT');
assert.match(station, /document\["transport"\] = "BLE_GATEWAY"/);
assert.match(station, /MEDPULSE_GATT_COMMAND_ACK_TIMEOUT_MS/,
    'Trạm phải timeout nếu thiết bị không ACK lệnh GATT');
assert.match(server, /`\$\{MQTT_ROOT_TOPIC\}\/devices\/\+\/events`/);
assert.match(server, /DIRECT_DEVICE_EVENT/);
assert.match(web, /DIRECT_DEVICE_STATUS[\s\S]*DIRECT_DEVICE_EVENT/);
for (const column of ['last_event_sequence', 'last_event_uptime_seconds', 'last_event_at', 'gatt_ready', 'last_gatt_status_at', 'sensor_ready']) {
    assert.match(schema, new RegExp(column), `Schema SQL thiếu cột ${column} của ble_devices`);
}
assert.doesNotMatch(sketch, /while\s*\(\s*1\s*\)/,
    'Lỗi MPU không được chặn BLE\/MQTT');
assert.match(sketch, /HARDWARE_FAULT/);

console.log('MedPulse device firmware contracts: PASS');
