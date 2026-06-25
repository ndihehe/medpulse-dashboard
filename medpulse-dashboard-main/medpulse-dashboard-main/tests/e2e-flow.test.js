'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const serverSource = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const webSource = fs.readFileSync(path.join(root, 'web.html'), 'utf8');

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

const BLE_PROTOCOL_VERSION = 1;
const COMMAND_PROTOCOL_VERSION = 2;
const BLE_DEVICE_SEEN_STALE_MS = 45000;
const BLE_GATT_READY_STALE_MS = 45000;
const BLE_ALERT_TIMEOUT_MS = 180000;
const STATION_OFFLINE_TIMEOUT_MS = 90000;
const DIRECT_DEVICE_TIMEOUT_MS = 90000;
const MQTT_ROOT_TOPIC = 'medpulse_test';
const GATT_EVENT_TYPES = new Set(['HEARTBEAT', 'PREALERT', 'FALL_ALERT', 'RECOVERED']);
const bleDeviceStates = new Map();
const stationStates = new Map();
const directDeviceStates = new Map();
const deviceCommands = new Map();
const patientsState = {};
const registeredPatientIds = new Set();
const persistedDevices = [];
const deviceBroadcasts = [];
const transportBroadcasts = [];
const patientUpdates = [];
const commandBroadcasts = [];
const commandPersists = [];
const pendingDispatches = [];
const mqttPublishes = [];
const stationPersists = [];
const dashboardBroadcasts = [];

function isValidProtocolIdentifier(value, maxLength) {
    return typeof value === 'string' && value.length >= 1 && value.length <= maxLength
        && /^[A-Za-z0-9_-]+$/.test(value);
}
function toIsoOrNull(value) { return value ? new Date(value).toISOString() : null; }
function isBleDeviceSeenToday(device) { return Boolean(device.firstSeenToday); }
function createDevice(deviceId) {
    return {
        deviceId, patientId: null, stationId: null, status: 'OFFLINE',
        sessionActive: false, sessionBlocked: false, firstSeenToday: null,
        lastBleSeen: null, disconnectedAt: null, alertStartedAt: null,
        rssi: null, battery: null, lastGattEventSequence: null,
        lastGattEventUptimeSeconds: null, lastGattEventAt: null,
        gattReady: false, lastGattStatusAt: null, lastEvent: null,
    };
}
function getOrCreateBleDeviceState(deviceId) {
    if (!bleDeviceStates.has(deviceId)) bleDeviceStates.set(deviceId, createDevice(deviceId));
    return bleDeviceStates.get(deviceId);
}
function autoLinkBleDeviceToMatchingPatient(device) {
    if (!device || device.patientId || !registeredPatientIds.has(device.deviceId)) return false;
    device.patientId = device.deviceId;
    return true;
}
function markStationOnlineFromPayload(stationId, payload, now) {
    stationStates.set(stationId, { stationId, status: 'ONLINE', lastSeenAt: now, uptimeMs: payload.uptimeMs });
}
function persistBleDeviceState(device) { persistedDevices.push({ ...device }); }
function persistStationState(station) { stationPersists.push({ ...station }); }
function serializeStationState(station) { return { ...station }; }
function broadcastToDashboards(payload) { dashboardBroadcasts.push(payload); }
function resetBleDeviceForNewDay(device) {
    device.status = 'OFFLINE';
    device.sessionActive = false;
    device.sessionBlocked = false;
    device.firstSeenToday = null;
    device.lastBleSeen = null;
    device.disconnectedAt = null;
    device.alertStartedAt = null;
    device.lastEvent = 'DAILY_RESET';
    persistBleDeviceState(device);
    broadcastBleDeviceState(device);
}
function broadcastBleDeviceState(device, type = 'BLE_DEVICE_UPDATE') {
    deviceBroadcasts.push({ type, deviceId: device.deviceId, lastEvent: device.lastEvent });
}
function broadcastDeviceTransportState(deviceId) { transportBroadcasts.push(deviceId); }
function dispatchPendingCommandsForDevice(deviceId) { pendingDispatches.push(deviceId); }
function syncFallSafeState(patient, metric, status) {
    assert.equal(metric, 'status');
    patient.status = status;
    patient.current_status = status;
    patient.fall = status === 'ALERT';
    patient.safe = status === 'IDLE';
}
function publishPatientUpdate(patientId, options) { patientUpdates.push({ patientId, options, status: patientsState[patientId].status }); }
function persistDeviceCommand(command) { commandPersists.push({ ...command }); }
function broadcastDeviceCommand(command) { commandBroadcasts.push({ ...command }); }
function isDirectDeviceOnline(deviceId) {
    const direct = directDeviceStates.get(deviceId);
    const device = bleDeviceStates.get(deviceId);
    return Boolean(device?.sessionActive && !device.sessionBlocked && direct?.online
        && Date.now() - direct.lastSeenAt.getTime() < DIRECT_DEVICE_TIMEOUT_MS);
}

const mqttClient = {
    connected: () => true,
    publish(topic, payload, options, callback) {
        mqttPublishes.push({ topic, payload: JSON.parse(payload), options });
        callback(null);
    },
};

const backendFactory = new Function(
    'BLE_PROTOCOL_VERSION', 'COMMAND_PROTOCOL_VERSION', 'BLE_GATT_READY_STALE_MS',
    'BLE_DEVICE_SEEN_STALE_MS', 'BLE_ALERT_TIMEOUT_MS', 'STATION_OFFLINE_TIMEOUT_MS',
    'DIRECT_DEVICE_TIMEOUT_MS', 'MQTT_ROOT_TOPIC', 'GATT_EVENT_TYPES',
    'bleDeviceStates', 'stationStates', 'directDeviceStates', 'deviceCommands',
    'patientsState', 'registeredPatientIds', 'isValidProtocolIdentifier', 'toIsoOrNull',
    'isBleDeviceSeenToday', 'getOrCreateBleDeviceState', 'markStationOnlineFromPayload',
    'persistBleDeviceState', 'persistStationState', 'serializeStationState', 'broadcastToDashboards',
    'resetBleDeviceForNewDay', 'broadcastBleDeviceState', 'broadcastDeviceTransportState',
    'dispatchPendingCommandsForDevice', 'syncFallSafeState', 'publishPatientUpdate',
    'persistDeviceCommand', 'broadcastDeviceCommand', 'isDirectDeviceOnline',
    'autoLinkBleDeviceToMatchingPatient', 'mqttClient',
    `${extractFunction(serverSource, 'validateGatewayTransportPayload')}
     ${extractFunction(serverSource, 'validateGatewayEventPayload')}
     ${extractFunction(serverSource, 'isFreshGatewayEvent')}
     ${extractFunction(serverSource, 'firmwareStatusFromDeviceEvent')}
     ${extractFunction(serverSource, 'statusFromDeviceEvent')}
     ${extractFunction(serverSource, 'isActiveFallDeviceEventName')}
     ${extractFunction(serverSource, 'logUnmappedFallEvent')}
     ${extractFunction(serverSource, 'isBleGatewayReady')}
     ${extractFunction(serverSource, 'resolveDeviceTransportState')}
     ${extractFunction(serverSource, 'describeTransportBlocker')}
     ${extractFunction(serverSource, 'handleGatewayTransportPayload')}
     ${extractFunction(serverSource, 'handleGatewayEventPayload')}
     ${extractFunction(serverSource, 'evaluateBleStateMachine')}
     ${extractFunction(serverSource, 'dispatchDeviceCommand')}
     ${extractFunction(serverSource, 'handleDirectDeviceMessage')}
     return {
       validateGatewayTransportPayload, validateGatewayEventPayload,
       resolveDeviceTransportState, handleGatewayTransportPayload,
       handleGatewayEventPayload, evaluateBleStateMachine, dispatchDeviceCommand, handleDirectDeviceMessage
     };`
);

const backend = backendFactory(
    BLE_PROTOCOL_VERSION, COMMAND_PROTOCOL_VERSION, BLE_GATT_READY_STALE_MS,
    BLE_DEVICE_SEEN_STALE_MS, BLE_ALERT_TIMEOUT_MS, STATION_OFFLINE_TIMEOUT_MS,
    DIRECT_DEVICE_TIMEOUT_MS, MQTT_ROOT_TOPIC, GATT_EVENT_TYPES,
    bleDeviceStates, stationStates, directDeviceStates, deviceCommands,
    patientsState, registeredPatientIds, isValidProtocolIdentifier, toIsoOrNull,
    isBleDeviceSeenToday, getOrCreateBleDeviceState, markStationOnlineFromPayload,
    persistBleDeviceState, persistStationState, serializeStationState, broadcastToDashboards,
    resetBleDeviceForNewDay, broadcastBleDeviceState, broadcastDeviceTransportState,
    dispatchPendingCommandsForDevice, syncFallSafeState, publishPatientUpdate,
    persistDeviceCommand, broadcastDeviceCommand, isDirectDeviceOnline,
    autoLinkBleDeviceToMatchingPatient, mqttClient
);

function testStationToBackendTransport() {
    const payload = {
        protocolVersion: 2, stationId: 'STATION-01', deviceId: 'MINI-A1',
        transport: 'BLE_GATEWAY', online: true, gattReady: true,
        rssi: -61, stationUptimeMs: 10000,
    };
    assert.equal(backend.validateGatewayTransportPayload(payload, 'STATION-01', 'MINI-A1'), null);
    backend.handleGatewayTransportPayload(payload, 'STATION-01', 'MINI-A1');
    assert.equal(backend.resolveDeviceTransportState('MINI-A1').activeTransport, 'BLE_GATEWAY');
    assert.equal(bleDeviceStates.get('MINI-A1').status, 'ONLINE');
    assert.ok(deviceBroadcasts.some(entry => entry.deviceId === 'MINI-A1'));
    assert.ok(transportBroadcasts.includes('MINI-A1'));

    assert.equal(backend.handleDirectDeviceMessage('MINI-A1', 'status', {
        protocolVersion: 2, deviceId: 'MINI-A1', transport: 'DIRECT_MQTT', online: true, wifiRssi: -55,
    }), true);
    assert.equal(backend.resolveDeviceTransportState('MINI-A1').activeTransport, 'BLE_GATEWAY', 'BLE phải được ưu tiên');

    backend.handleGatewayTransportPayload({ ...payload, online: false, gattReady: false }, 'STATION-01', 'MINI-A1');
    assert.equal(backend.resolveDeviceTransportState('MINI-A1').activeTransport, 'DIRECT_MQTT');
}

function testFallEventToPatientAndWebsocket() {
    const device = bleDeviceStates.get('MINI-A1');
    device.patientId = 'RFID-P1';
    device.sessionActive = true;
    patientsState['RFID-P1'] = { id: 'RFID-P1', status: 'IDLE', safe: true, fall: false };
    registeredPatientIds.add('RFID-P1');

    const event = {
        protocolVersion: 2, stationId: 'STATION-01', deviceId: 'MINI-A1', sequence: 7,
        event: 'FALL_ALERT', battery: 88, fallDetected: true, charging: false,
        buzzerActive: true, deviceUptimeSeconds: 120, rssi: -63, stationUptimeMs: 20000,
    };
    assert.equal(backend.validateGatewayEventPayload(event, 'STATION-01', 'MINI-A1'), null);
    backend.handleGatewayEventPayload(event, 'STATION-01', 'MINI-A1');
    assert.equal(patientsState['RFID-P1'].status, 'ALERT');
    assert.equal(patientUpdates.at(-1).options.saveToDatabase, false, 'Sự kiện không được giả thành lần đo');
    const updateCount = patientUpdates.length;
    backend.handleGatewayEventPayload(event, 'STATION-01', 'MINI-A1');
    assert.equal(patientUpdates.length, updateCount, 'Sequence trùng phải bị loại');

    backend.handleGatewayEventPayload({
        ...event, sequence: 8, event: 'RECOVERED', fallDetected: false, buzzerActive: false,
        deviceUptimeSeconds: 130,
    }, 'STATION-01', 'MINI-A1');
    assert.equal(patientsState['RFID-P1'].status, 'IDLE');
}

function testDirectRecoveredClearsPatientEvenAfterBleInactive() {
    const deviceId = 'MINI-DIRECT';
    const patientId = 'RFID-DIRECT';
    const device = getOrCreateBleDeviceState(deviceId);
    device.patientId = patientId;
    device.sessionActive = false;
    device.status = 'ALERT';
    device.lastEvent = 'DIRECT_FALL_ALERT';
    patientsState[patientId] = { id: patientId, status: 'ALERT', safe: false, fall: true };
    registeredPatientIds.add(patientId);

    assert.equal(backend.handleDirectDeviceMessage(deviceId, 'events', {
        protocolVersion: 2,
        deviceId,
        sequence: 11,
        event: 'RECOVERED',
        battery: 80,
        fallDetected: false,
        charging: false,
        buzzerActive: false,
        deviceUptimeSeconds: 220,
    }), true);

    assert.equal(device.sessionActive, true, 'Direct MQTT event phải mở lại session logic nếu không bị block');
    assert.equal(device.status, 'ONLINE');
    assert.equal(patientsState[patientId].status, 'IDLE');
    assert.equal(patientsState[patientId].fall, false);
    assert.equal(patientUpdates.at(-1).patientId, patientId);
    assert.equal(patientUpdates.at(-1).status, 'IDLE');
}

function testDirectFallAlertUpdatesPatient() {
    const deviceId = 'MINI-DIRECT-FALL';
    const patientId = 'RFID-DIRECT-FALL';
    const device = getOrCreateBleDeviceState(deviceId);
    device.patientId = patientId;
    device.sessionActive = false;
    device.status = 'OFFLINE';
    patientsState[patientId] = { id: patientId, status: 'IDLE', safe: true, fall: false };
    registeredPatientIds.add(patientId);

    assert.equal(backend.handleDirectDeviceMessage(deviceId, 'events', {
        protocolVersion: 2,
        deviceId,
        sequence: 21,
        event: 'FALL_ALERT',
        battery: 76,
        fallDetected: true,
        charging: false,
        buzzerActive: true,
        deviceUptimeSeconds: 330,
    }), true);

    assert.equal(device.sessionActive, true);
    assert.equal(device.status, 'ALERT');
    assert.equal(device.lastEvent, 'DIRECT_FALL_ALERT');
    assert.equal(patientsState[patientId].status, 'ALERT');
    assert.equal(patientsState[patientId].fall, true);
    assert.equal(patientUpdates.at(-1).patientId, patientId);
    assert.equal(patientUpdates.at(-1).status, 'ALERT');
}

function testDirectTransportDoesNotBecomeStationOffline() {
    const deviceId = 'MINI-DIRECT-STATION';
    const patientId = 'RFID-DIRECT-STATION';
    const device = getOrCreateBleDeviceState(deviceId);
    device.patientId = patientId;
    device.stationId = 'STATION-STALE';
    device.sessionActive = true;
    device.firstSeenToday = new Date();
    device.status = 'ONLINE';
    patientsState[patientId] = { id: patientId, status: 'IDLE', safe: true, fall: false };
    registeredPatientIds.add(patientId);
    stationStates.set('STATION-STALE', {
        stationId: 'STATION-STALE',
        status: 'OFFLINE',
        lastSeenAt: new Date(Date.now() - 120000),
    });
    directDeviceStates.set(deviceId, {
        deviceId,
        online: true,
        lastSeenAt: new Date(),
        wifiRssi: -58,
    });

    backend.evaluateBleStateMachine();

    assert.equal(device.status, 'ONLINE', 'Direct MQTT online không được bị ép thành STATION_OFFLINE vì stationId cũ');
    assert.equal(backend.resolveDeviceTransportState(deviceId).activeTransport, 'DIRECT_MQTT');
}

function testDirectOnlyTimeoutDoesNotBecomeStationOffline() {
    const deviceId = 'MINI-DIRECT-ONLY';
    const patientId = 'RFID-DIRECT-ONLY';
    const device = getOrCreateBleDeviceState(deviceId);
    device.patientId = patientId;
    device.stationId = null;
    device.sessionActive = true;
    device.firstSeenToday = new Date();
    device.status = 'ONLINE';
    patientsState[patientId] = { id: patientId, status: 'IDLE', safe: true, fall: false };
    registeredPatientIds.add(patientId);
    directDeviceStates.set(deviceId, {
        deviceId,
        online: false,
        lastSeenAt: new Date(Date.now() - DIRECT_DEVICE_TIMEOUT_MS - 1000),
        wifiRssi: -70,
    });

    backend.evaluateBleStateMachine();

    assert.equal(device.status, 'OFFLINE', 'Thiết bị direct-only mất heartbeat phải offline, không phải STATION_OFFLINE');
    assert.equal(device.lastEvent, 'DIRECT_MQTT_TIMEOUT');
}

function testCommandRouteFallbackAndAck() {
    const device = bleDeviceStates.get('MINI-A1');
    device.gattReady = true;
    device.lastGattStatusAt = new Date();
    device.sessionBlocked = false;
    stationStates.set('STATION-01', { status: 'ONLINE' });

    const command = {
        commandId: 'CMD-E2E-0001', deviceId: 'MINI-A1', patientId: 'RFID-P1',
        stationId: null, action: 'BUZZER_ON', durationMs: 10000,
        route: 'PENDING', status: 'PENDING', error: null,
        expiresAt: new Date(Date.now() + 60000), dispatchedAt: null,
    };
    deviceCommands.set(command.commandId, command);
    assert.equal(backend.dispatchDeviceCommand(command), true);
    assert.equal(mqttPublishes.at(-1).topic, 'medpulse_test/stations/STATION-01/devices/MINI-A1/commands');
    assert.equal(command.route, 'BLE_GATEWAY');

    assert.equal(backend.handleDirectDeviceMessage('MINI-A1', 'acks', {
        protocolVersion: 2, commandId: command.commandId, deviceId: 'MINI-A1',
        status: 'EXECUTED', transport: 'BLE_GATEWAY', stationId: 'STATION-01',
    }), true);
    assert.equal(command.status, 'EXECUTED');
    assert.ok(commandBroadcasts.some(entry => entry.commandId === command.commandId && entry.status === 'EXECUTED'));

    device.gattReady = false;
    const directCommand = {
        ...command, commandId: 'CMD-E2E-0002', route: 'PENDING', status: 'PENDING', stationId: null,
        expiresAt: new Date(Date.now() + 60000),
    };
    deviceCommands.set(directCommand.commandId, directCommand);
    assert.equal(backend.dispatchDeviceCommand(directCommand), true);
    assert.equal(mqttPublishes.at(-1).topic, 'medpulse_test/devices/MINI-A1/commands');
    assert.equal(directCommand.route, 'DIRECT_MQTT');
}

function testFrontendReceivesBackendState() {
    const BLE_DEVICE_STATUSES = new Set(['OFFLINE', 'ONLINE', 'GRACE', 'ALERT', 'STATION_OFFLINE']);
    const STATION_HEARTBEAT_STALE_MS = 90000;
    const frontendFactory = new Function(
        'BLE_DEVICE_STATUSES', 'STATION_HEARTBEAT_STALE_MS',
        `${extractFunction(webSource, 'normalizeBleDeviceState')}
         ${extractFunction(webSource, 'normalizeStationState')}
         ${extractFunction(webSource, 'isWearableFallAlert')}
         ${extractFunction(webSource, 'isWearablePreAlert')}
         ${extractFunction(webSource, 'playAlertTone')}
         ${extractFunction(webSource, 'hasBleConnectionAlert')}
         ${extractFunction(webSource, 'bleStatusMeta')}
         return { normalizeBleDeviceState, normalizeStationState, bleStatusMeta, playAlertTone };`
    );
    const frontend = frontendFactory(BLE_DEVICE_STATUSES, STATION_HEARTBEAT_STALE_MS);
    const normalizedDevice = frontend.normalizeBleDeviceState({
        deviceId: 'MINI-A1', patientId: 'RFID-P1', stationId: 'STATION-01', status: 'ONLINE',
        gattReady: true, activeTransport: 'BLE_GATEWAY', transportOnline: true,
        lastGattStatusAt: new Date().toISOString(),
    });
    assert.equal(normalizedDevice.activeTransport, 'BLE_GATEWAY');
    assert.equal(normalizedDevice.gattReady, true);
    assert.equal(frontend.bleStatusMeta({ ...normalizedDevice, activeTransport: 'DIRECT_MQTT' }).label, 'MQTT trực tiếp');
    assert.equal(frontend.bleStatusMeta({ ...normalizedDevice, status: 'ALERT', lastEvent: 'DIRECT_FALL_ALERT', activeTransport: 'DIRECT_MQTT' }).label, 'MQTT trực tiếp');
    assert.equal(frontend.bleStatusMeta({ ...normalizedDevice, status: 'ALERT', lastEvent: 'GATT_FALL_ALERT', activeTransport: 'BLE_GATEWAY' }).label, 'BLE online');
    assert.equal(frontend.bleStatusMeta({ ...normalizedDevice, status: 'ALERT', lastEvent: 'DIRECT_FALL_ALERT', activeTransport: 'OFFLINE' }).label, 'Offline');
    assert.equal(frontend.bleStatusMeta({ ...normalizedDevice, status: 'ALERT', lastEvent: 'BLE_ALERT_TIMEOUT', connectionAlert: true, activeTransport: 'OFFLINE' }).label, 'Mất BLE');
    assert.equal(webSource.includes('function syncPatientStatusFromWearableDevice'), false,
        'Frontend không được tự mutate trạng thái bệnh nhân từ BLE device update');
    assert.equal(typeof frontend.playAlertTone, 'function');

    const normalizedStation = frontend.normalizeStationState({
        stationId: 'STATION-01', status: 'ONLINE', bleScanner: true,
        freeHeap: 120000, trackedBleDevices: 1, gattReadyConnections: 1,
        malformedGattFrames: 0, droppedGattFrames: 0,
        max30102Ready: true, mlx90614Ready: true, rc522Ready: true,
        lastSeenAt: new Date().toISOString(),
    });
    assert.equal(normalizedStation.freeHeap, 120000);
    assert.equal(normalizedStation.gattReadyConnections, 1);
    assert.equal(normalizedStation.status, 'ONLINE');
    assert.equal(frontend.normalizeStationState({
        stationId: 'STATION-STALE',
        status: 'ONLINE',
        lastSeenAt: new Date(Date.now() - 120000).toISOString(),
    }).status, 'OFFLINE');
}

testStationToBackendTransport();
testFallEventToPatientAndWebsocket();
testDirectRecoveredClearsPatientEvenAfterBleInactive();
testDirectFallAlertUpdatesPatient();
testDirectTransportDoesNotBecomeStationOffline();
testDirectOnlyTimeoutDoesNotBecomeStationOffline();
testCommandRouteFallbackAndAck();
testFrontendReceivesBackendState();
console.log('MedPulse simulated E2E flow: PASS');
