'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const serverSource = read('server.js');
const webSource = read('web.html');
const stationSource = read('tram_sketch/tram_sketch.ino');
const protocolSource = read('tram_sketch/medpulse_protocol.h');

function extractFunction(source, name) {
    const start = source.indexOf(`function ${name}(`);
    assert.notEqual(start, -1, `Không tìm thấy function ${name}`);
    const openingBrace = source.indexOf('{', start);
    let depth = 0;
    for (let index = openingBrace; index < source.length; index++) {
        if (source[index] === '{') depth += 1;
        else if (source[index] === '}' && --depth === 0) return source.slice(start, index + 1);
    }
    throw new Error(`Function ${name} chưa đóng ngoặc`);
}

function crc16Ccitt(bytes) {
    let crc = 0xFFFF;
    for (const value of bytes) {
        crc ^= value << 8;
        for (let bit = 0; bit < 8; bit++) {
            crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) & 0xFFFF : (crc << 1) & 0xFFFF;
        }
    }
    return crc;
}

function fnv1a(value) {
    let hash = 0x811C9DC5;
    for (const byte of Buffer.from(value)) {
        hash ^= byte;
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash;
}

function testSyntax() {
    assert.doesNotThrow(() => new Function(serverSource), 'server.js sai cú pháp');
    const scripts = [...webSource.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)];
    for (const script of scripts) {
        if (script[1].trim()) assert.doesNotThrow(() => new Function(script[1]), 'JavaScript trong web.html sai cú pháp');
    }
}

function testGattCommandVectors() {
    const token = fnv1a('CMD-1710000000000-A1B2C3');
    assert.equal(token, 0x43395424);

    const command = Buffer.alloc(16);
    command.set([0x4D, 0x50, 0x02, 0x30]);
    command.writeUInt32LE(token, 4);
    command[8] = 1;
    command.writeUInt32LE(10000, 10);
    command.writeUInt16LE(crc16Ccitt(command.subarray(0, 14)), 14);
    assert.equal(command.toString('hex').toUpperCase(), '4D50023024543943010010270000A919');

    const ack = Buffer.alloc(12);
    ack.set([0x4D, 0x50, 0x02, 0x31]);
    ack.writeUInt32LE(token, 4);
    ack[8] = 1;
    ack.writeUInt16LE(crc16Ccitt(ack.subarray(0, 10)), 10);
    assert.equal(ack.toString('hex').toUpperCase(), '4D500231245439430100BA7E');

    ack[8] ^= 1;
    assert.notEqual(crc16Ccitt(ack.subarray(0, 10)), ack.readUInt16LE(10), 'CRC phải phát hiện frame bị sửa');
}

function testBackendContracts() {
    const COMMAND_PROTOCOL_VERSION = 2;
    const BLE_GATT_READY_STALE_MS = 45000;
    const DIRECT_DEVICE_TIMEOUT_MS = 90000;
    const bleDeviceStates = new Map();
    const stationStates = new Map();
    const directDeviceStates = new Map();
    const isValidProtocolIdentifier = (value, maxLength) => typeof value === 'string'
        && value.length >= 1 && value.length <= maxLength && /^[A-Za-z0-9_-]+$/.test(value);
    const toIsoOrNull = value => value ? new Date(value).toISOString() : null;

    const factory = new Function(
        'COMMAND_PROTOCOL_VERSION', 'BLE_GATT_READY_STALE_MS', 'DIRECT_DEVICE_TIMEOUT_MS',
        'bleDeviceStates', 'stationStates', 'directDeviceStates',
        'isValidProtocolIdentifier', 'toIsoOrNull',
        `${extractFunction(serverSource, 'validateGatewayTransportPayload')}
         ${extractFunction(serverSource, 'isBleGatewayReady')}
         ${extractFunction(serverSource, 'resolveDeviceTransportState')}
         ${extractFunction(serverSource, 'hasCompleteVitalsPayload')}
         return { validateGatewayTransportPayload, resolveDeviceTransportState, hasCompleteVitalsPayload };`
    );
    const contract = factory(
        COMMAND_PROTOCOL_VERSION, BLE_GATT_READY_STALE_MS, DIRECT_DEVICE_TIMEOUT_MS,
        bleDeviceStates, stationStates, directDeviceStates,
        isValidProtocolIdentifier, toIsoOrNull
    );

    const validTransport = {
        protocolVersion: 2, stationId: 'STATION-01', deviceId: 'MINI-A1',
        transport: 'BLE_GATEWAY', online: true, gattReady: true,
        rssi: -64, stationUptimeMs: 1000,
    };
    assert.equal(contract.validateGatewayTransportPayload(validTransport, 'STATION-01', 'MINI-A1'), null);
    assert.match(contract.validateGatewayTransportPayload({ ...validTransport, online: false }, 'STATION-01', 'MINI-A1'), /online/);

    const now = Date.now();
    const setTransportState = ({ gattAge = 1000, station = 'ONLINE', directAge = 1000, blocked = false } = {}) => {
        bleDeviceStates.set('MINI-A1', {
            deviceId: 'MINI-A1', stationId: 'STATION-01', gattReady: true,
            lastGattStatusAt: new Date(now - gattAge), sessionActive: true, sessionBlocked: blocked,
        });
        stationStates.set('STATION-01', { status: station });
        directDeviceStates.set('MINI-A1', { online: true, lastSeenAt: new Date(now - directAge), wifiRssi: -60 });
        return contract.resolveDeviceTransportState('MINI-A1', now).activeTransport;
    };
    assert.equal(setTransportState(), 'BLE_GATEWAY');
    assert.equal(setTransportState({ gattAge: 46000 }), 'DIRECT_MQTT');
    assert.equal(setTransportState({ station: 'OFFLINE' }), 'DIRECT_MQTT');
    assert.equal(setTransportState({ gattAge: 46000, directAge: 91000 }), 'OFFLINE');
    assert.equal(setTransportState({ blocked: true }), 'OFFLINE');

    assert.equal(contract.hasCompleteVitalsPayload({ heartRate: 70, spo2: 98, temp: 36.5 }), true);
    assert.equal(contract.hasCompleteVitalsPayload({ heartRate: 70, temp: 36.5 }), false);
}

function testStationResilienceContract() {
    assert.match(protocolSource, /MEDPULSE_WIFI_RECONNECT_INTERVAL_MS/);
    assert.match(protocolSource, /MEDPULSE_MQTT_RECONNECT_INTERVAL_MS/);
    assert.match(protocolSource, /MEDPULSE_SENSOR_RETRY_INTERVAL_MS/);
    assert.match(protocolSource, /MEDPULSE_MEASUREMENT_SESSION_TIMEOUT_MS/);
    assert.doesNotMatch(stationSource, /while\s*\(\s*!mqtt\.connected\(\)\s*\)/);
    assert.doesNotMatch(stationSource, /while\s*\(\s*WiFi\.status\(\)\s*!=\s*WL_CONNECTED\s*\)/);
    assert.equal((stationSource.match(/WiFi\.begin\(/g) || []).length, 1, 'Chỉ cấu hình Wi-Fi một lần');
    assert.match(stationSource, /WiFi\.reconnect\(\)/);
    assert.doesNotMatch(stationSource, /while\s*\(\s*1\s*\)/);
    assert.doesNotMatch(stationSource, /"spo2"\s*:\s*98/);
    assert.doesNotMatch(stationSource, /"battery"\s*:\s*92/);
    assert.equal(stationSource.includes('spo2Value'), false, 'Không được tạo SpO2 mặc định');
    assert.match(stationSource, /processSpo2\(red, ir\)/, 'Trạm phải tính SpO2 từ mẫu RED\/IR thực');
    assert.match(stationSource, /"\\"spo2\\":%d,"/, 'Gói MQTT phải chứa SpO2 đã đo');
    assert.match(stationSource, /return tempValid && bpmValid && spo2Valid && enoughTime/,
        'Không được publish khi thiếu một sinh hiệu bắt buộc');
    assert.match(serverSource, /if \(!isCompleteMeasurement\) \{[\s\S]*Bỏ qua gói đo chưa đầy đủ/,
        'Backend không được tạo đăng ký từ gói đo dở dang');
    const patientsListRoute = serverSource.slice(serverSource.indexOf("app.get('/api/patients'"), serverSource.indexOf("// [FIX] API /api/patients/:id"));
    assert.doesNotMatch(patientsListRoute, /Object\.values\(patientsState\)\.forEach\(resetPatientMeasurementState\)/,
        'API danh sách bệnh nhân không được reset trạng thái realtime mới hơn vitals_log');
    assert.equal(stationSource.includes('\\"battery\\":92'), false, 'Không được tạo pin mặc định');
    assert.equal(stationSource.includes('return "RFID-1005"'), false, 'Không được giữ ánh xạ RFID demo');
    assert.match(stationSource, /if \(!advertisedDevice\.haveManufacturerData\(\)\) return false/);
    assert.match(stationSource, /serviceSensorRecovery\(\)/);
    assert.match(stationSource, /mqtt\.setSocketTimeout\(3\)/);
    assert.match(stationSource, /ESP\.getFreeHeap\(\)/);
    assert.match(stationSource, /#if ESP_ARDUINO_VERSION_MAJOR >= 3[\s\S]*writeValue/);
}

function testRegistrationTimeoutContract() {
    assert.match(serverSource, /REGISTRATION_DB_TIMEOUT_MS/);
    const registrationInsert = extractFunction(serverSource, 'insertPatientProfile');
    assert.match(registrationInsert, /db\.query\(/, 'Đăng ký phải tái sử dụng MySQL pool');
    assert.doesNotMatch(registrationInsert, /mysql\.createConnection\(/,
        'Không được mở kết nối MySQL mới cho từng lần đăng ký');
    assert.match(serverSource, /Đăng ký bệnh nhân .*HTTP/);
    assert.match(webSource, /registrationController\.abort\(\)/);
    assert.match(webSource, /Backend hoặc database phản hồi quá chậm/);
}

function testAuthContract() {
    assert.match(serverSource, /app\.post\('\/api\/auth\/login'/, 'Backend phải có endpoint đăng nhập thật');
    assert.match(serverSource, /function requireApiAuth\(req, res, next\)/, 'Backend phải có middleware auth cho API ghi');
    assert.match(serverSource, /function requireMinimumRole\(minimumRole\)/, 'Backend phải enforce role cho thao tác nhạy cảm');
    assert.match(serverSource, /app\.post\('\/api\/devices\/:deviceId\/commands', requireApiAuth/,
        'Gửi lệnh thiết bị phải yêu cầu đăng nhập');
    assert.match(serverSource, /app\.delete\('\/api\/patients\/:id', requireApiAuth, requireMinimumRole\('HEAD_NURSE'\), requireDeleteOtp/,
        'Xóa bệnh nhân phải yêu cầu đăng nhập, đúng vai trò và OTP');
    assert.match(serverSource, /checkDeleteOtpRateLimit\(req, patientId\)/,
        'OTP xóa bệnh nhân phải có rate-limit');
    assert.match(serverSource, /ALLOW_DEMO_LOGIN/, 'Demo login phải có cờ cấu hình rõ ràng');
    assert.match(serverSource, /NODE_ENV === 'production' \? 'false' : 'true'/,
        'Demo login phải tự tắt mặc định trong production');
    assert.match(webSource, /DEMO_LOGIN_PASSWORD = 'demo123'/,
        'Dashboard giữ mật khẩu demo để test nhanh theo yêu cầu');
    assert.match(webSource, /api\/auth\/login/);
    assert.match(webSource, /Authorization: `Bearer \$\{state\.authToken\}`/);
}

function testDeviceCommandRetryContract() {
    assert.match(serverSource, /DEVICE_COMMAND_ACK_TIMEOUT_MS/,
        'Backend phải có timeout ACK riêng cho lệnh thiết bị');
    assert.match(serverSource, /function retryTimedOutDeviceCommands\(\)/,
        'Backend phải quét lại lệnh đã DISPATCHED\/DELIVERED nhưng không có ACK terminal');
    assert.match(serverSource, /DISPATCHED', 'DELIVERED/,
        'Retry phải áp dụng cho cả lệnh đã publish và lệnh trạm mới báo delivered');
    assert.match(serverSource, /rerouting to \$\{nextRoute\}/,
        'Khi route cũ không ACK, backend phải tự chuyển sang transport còn online');
    assert.match(serverSource, /dispatchAllPendingDeviceCommands\(\);\s*expireDeviceCommands\(\);/,
        'Interval nền phải vừa retry pending vừa expire lệnh quá hạn');
    assert.match(serverSource, /markBleGatewayCommandRouteUnready\(deviceId, liveData\.error\)/,
        'Lỗi BLE từ trạm phải hạ trạng thái GATT để không chọn lại route vừa thất bại');
    assert.match(serverSource, /command\.status = 'DISPATCHED';[\s\S]*mqttClient\.publish/,
        'Backend phải chuyển lệnh sang DISPATCHED ngay khi queue publish MQTT để ACK timeout có thể giám sát');
    assert.match(serverSource, /MQTT_PUBLISH_ERROR/,
        'Publish MQTT lỗi phải đưa lệnh về PENDING với lỗi rõ ràng');
    assert.match(serverSource, /if \(!mqttClient\.connected\) \{/,
        'MQTT.js dùng connected là boolean property, không được gọi connected() gây 500 khi gửi lệnh');
    assert.doesNotMatch(serverSource, /mqttClient\.connected\(\)/,
        'Không được gọi mqttClient.connected() như function');
}

function testProductionConfigContract() {
    assert.match(serverSource, /function validateProductionConfiguration\(\)/,
        'Backend phải kiểm tra cấu hình production');
    assert.match(serverSource, /DEFAULT_MQTT_BROKER_URL/,
        'Broker MQTT mặc định phải được đặt tên rõ và kiểm soát');
    assert.match(serverSource, /DB_SSL_REJECT_UNAUTHORIZED/,
        'DB SSL verification phải cấu hình được qua env');
    assert.match(serverSource, /ENFORCE_PRODUCTION_CONFIG/,
        'Production config fail-fast phải điều khiển được bằng env để staging không chết lúc deploy');
    assert.match(serverSource, /if \(ENFORCE_PRODUCTION_CONFIG\) \{[\s\S]*process\.exit\(1\)/,
        'Khi bật strict production config, backend vẫn phải fail fast');
    assert.equal(fs.existsSync(path.join(root, 'medpulse-backend/package.json')), false,
        'Không giữ package backend phụ gây nhầm entrypoint');
}

testSyntax();
testGattCommandVectors();
testBackendContracts();
testStationResilienceContract();
testRegistrationTimeoutContract();
testAuthContract();
testDeviceCommandRetryContract();
testProductionConfigContract();
console.log('MedPulse protocol/resilience tests: PASS');
