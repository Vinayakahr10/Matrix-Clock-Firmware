#include <Arduino.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLE2902.h>
#include <DHT.h>
#include <Update.h>

namespace {

constexpr char DEVICE_NAME[] = "DOTMATRIX-APP";
constexpr char FIRMWARE_VERSION[] = "v1.0.2";  // Keep this aligned with your GitHub release tag.
constexpr char OTA_SHARED_SECRET[] = "";       // Set this to a non-empty token before production use.

constexpr char SERVICE_UUID[] = "4fafc201-1fb5-459e-8fcc-c5c9c331914b";
constexpr char COMMAND_CHAR_UUID[] = "beb5483e-36e1-4688-b7f5-ea07361b26a8";
// Custom notify/read characteristic carrying text payloads like "T:27.4,H:61.0" and OTA status strings.
constexpr char SENSOR_CHAR_UUID[] = "9a52b7b0-5f9a-4f7c-9c2d-8b6e4a1d2001";

constexpr uint8_t DHT_PIN = 4;
constexpr uint8_t DHT_TYPE = DHT22;

constexpr uint32_t SENSOR_INTERVAL_MS = 5000;
constexpr uint32_t SENSOR_MIN_READ_INTERVAL_MS = 2200;
constexpr uint32_t RESTART_ADV_DELAY_MS = 250;
constexpr uint32_t OTA_TIMEOUT_MS = 15000;
constexpr uint32_t REBOOT_DELAY_MS = 1500;
constexpr uint32_t SERIAL_BAUDRATE = 115200;

constexpr int DEFAULT_BRIGHTNESS = 50;
constexpr size_t OTA_MAX_CHUNK_BYTES = 256;

DHT dht(DHT_PIN, DHT_TYPE);
BLEServer *g_bleServer = nullptr;
BLECharacteristic *g_commandCharacteristic = nullptr;
BLECharacteristic *g_sensorCharacteristic = nullptr;

bool g_isDeviceConnected = false;
bool g_restartAdvertisingPending = false;
uint32_t g_disconnectTimestampMs = 0;
uint32_t g_lastSensorPublishMs = 0;
uint32_t g_lastSensorReadAttemptMs = 0;
uint32_t g_otaLastActivityMs = 0;
uint32_t g_rebootScheduledAtMs = 0;
float g_lastTemperatureC = NAN;
float g_lastHumidity = NAN;
bool g_hasValidSensorSample = false;
bool g_otaInProgress = false;
bool g_rebootPending = false;
bool g_otaHasExpectedMd5 = false;
size_t g_otaExpectedSize = 0;
size_t g_otaReceivedSize = 0;
String g_otaExpectedMd5;

String g_pendingDisplayText;
String g_currentMode = "clock";
String g_currentAnimation = "default";
String g_lastSyncedTimestamp;
int g_brightness = DEFAULT_BRIGHTNESS;

bool readSensorValues(float &temperatureC, float &humidity) {
  const uint32_t now = millis();

  if ((now - g_lastSensorReadAttemptMs) < SENSOR_MIN_READ_INTERVAL_MS) {
    return false;
  }

  g_lastSensorReadAttemptMs = now;

  humidity = dht.readHumidity();
  temperatureC = dht.readTemperature();

  if (isnan(humidity) || isnan(temperatureC)) {
    Serial.println("DHT read failed");
    return false;
  }

  return true;
}

void notifyPayload(const String &payload);
void sendVersionData();

bool parseHexByte(char highNibble, char lowNibble, uint8_t &result) {
  auto hexValue = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') {
      return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
      return 10 + (ch - 'a');
    }
    if (ch >= 'A' && ch <= 'F') {
      return 10 + (ch - 'A');
    }
    return -1;
  };

  const int high = hexValue(highNibble);
  const int low = hexValue(lowNibble);
  if (high < 0 || low < 0) {
    return false;
  }

  result = static_cast<uint8_t>((high << 4) | low);
  return true;
}

void notifyPayload(const String &payload) {
  if (g_sensorCharacteristic == nullptr) {
    return;
  }

  g_sensorCharacteristic->setValue(payload.c_str());

  if (g_isDeviceConnected) {
    g_sensorCharacteristic->notify();
  }
}

void sendOtaStatus(const String &status) {
  Serial.print("OTA status: ");
  Serial.println(status);
  notifyPayload("OTA:" + status);
}

bool isValidMd5(const String &value) {
  if (value.length() != 32) {
    return false;
  }

  for (size_t i = 0; i < value.length(); ++i) {
    const char ch = value[i];
    const bool isDigit = ch >= '0' && ch <= '9';
    const bool isLowerHex = ch >= 'a' && ch <= 'f';
    const bool isUpperHex = ch >= 'A' && ch <= 'F';
    if (!isDigit && !isLowerHex && !isUpperHex) {
      return false;
    }
  }

  return true;
}

bool isAuthorizedOtaRequest(const String &token) {
  if (OTA_SHARED_SECRET[0] == '\0') {
    return true;
  }

  return token == OTA_SHARED_SECRET;
}

void abortOta(const String &reason) {
  if (g_otaInProgress) {
    Update.abort();
  }

  g_otaInProgress = false;
  g_otaHasExpectedMd5 = false;
  g_otaExpectedSize = 0;
  g_otaReceivedSize = 0;
  g_otaLastActivityMs = 0;
  g_otaExpectedMd5 = "";

  sendOtaStatus("ERROR," + reason);
}

bool beginOta(const String &commandValue) {
  if (g_otaInProgress) {
    sendOtaStatus("ERROR,busy");
    return false;
  }

  String sizeField = commandValue;
  String md5Field;
  String tokenField;

  const int firstComma = commandValue.indexOf(',');
  if (firstComma >= 0) {
    sizeField = commandValue.substring(0, firstComma);
    const String remainder = commandValue.substring(firstComma + 1);
    const int secondComma = remainder.indexOf(',');
    if (secondComma >= 0) {
      md5Field = remainder.substring(0, secondComma);
      tokenField = remainder.substring(secondComma + 1);
    } else {
      md5Field = remainder;
    }
  }

  sizeField.trim();
  md5Field.trim();
  tokenField.trim();

  if (!isAuthorizedOtaRequest(tokenField)) {
    sendOtaStatus("ERROR,unauthorized");
    return false;
  }

  if (!md5Field.isEmpty() && !isValidMd5(md5Field)) {
    sendOtaStatus("ERROR,invalid_md5");
    return false;
  }

  const size_t expectedSize = static_cast<size_t>(sizeField.toInt());
  if (expectedSize == 0) {
    sendOtaStatus("ERROR,invalid_size");
    return false;
  }

  if (!Update.begin(expectedSize)) {
    Serial.print("Update.begin failed: ");
    Serial.println(Update.errorString());
    sendOtaStatus("ERROR,begin_failed");
    return false;
  }

  g_otaExpectedSize = expectedSize;
  g_otaReceivedSize = 0;
  g_otaLastActivityMs = millis();
  g_otaInProgress = true;
  g_otaExpectedMd5 = md5Field;
  g_otaHasExpectedMd5 = !md5Field.isEmpty();

  if (g_otaHasExpectedMd5 && !Update.setMD5(g_otaExpectedMd5.c_str())) {
    Serial.println("Update.setMD5 failed");
    abortOta("md5_setup");
    return false;
  }

  sendOtaStatus("READY," + String(g_otaExpectedSize) + "," +
                String(g_otaHasExpectedMd5 ? "MD5" : "NO_MD5"));
  return true;
}

bool writeOtaChunk(const String &hexPayload) {
  if (!g_otaInProgress) {
    sendOtaStatus("ERROR,no_session");
    return false;
  }

  if (hexPayload.isEmpty() || (hexPayload.length() % 2) != 0) {
    abortOta("invalid_chunk");
    return false;
  }

  const size_t chunkSize = hexPayload.length() / 2;
  if (chunkSize == 0 || chunkSize > OTA_MAX_CHUNK_BYTES) {
    abortOta("chunk_size");
    return false;
  }

  uint8_t buffer[OTA_MAX_CHUNK_BYTES];
  for (size_t i = 0; i < chunkSize; ++i) {
    uint8_t value = 0;
    if (!parseHexByte(hexPayload[(i * 2)], hexPayload[(i * 2) + 1], value)) {
      abortOta("chunk_hex");
      return false;
    }
    buffer[i] = value;
  }

  const size_t remaining = g_otaExpectedSize - g_otaReceivedSize;
  if (chunkSize > remaining) {
    abortOta("overflow");
    return false;
  }

  const size_t written = Update.write(buffer, chunkSize);
  if (written != chunkSize) {
    Serial.print("Update.write failed: ");
    Serial.println(Update.errorString());
    abortOta("write_failed");
    return false;
  }

  g_otaReceivedSize += written;
  g_otaLastActivityMs = millis();

  sendOtaStatus("ACK," + String(g_otaReceivedSize));
  return true;
}

bool finishOta() {
  if (!g_otaInProgress) {
    sendOtaStatus("ERROR,no_session");
    return false;
  }

  if (g_otaReceivedSize != g_otaExpectedSize) {
    abortOta("size_mismatch");
    return false;
  }

  if (!Update.end(true)) {
    Serial.print("Update.end failed: ");
    Serial.println(Update.errorString());
    abortOta("end_failed");
    return false;
  }

  g_otaInProgress = false;
  g_otaHasExpectedMd5 = false;
  g_otaExpectedSize = 0;
  g_otaReceivedSize = 0;
  g_otaLastActivityMs = 0;
  g_otaExpectedMd5 = "";
  g_rebootPending = true;
  g_rebootScheduledAtMs = millis() + REBOOT_DELAY_MS;

  sendOtaStatus("DONE," + String(FIRMWARE_VERSION));
  return true;
}

void sendVersionData() {
  String payload;
  if (g_hasValidSensorSample) {
    payload.reserve(32);
    payload = "T:";
    payload += String(g_lastTemperatureC, 1);
    payload += ",H:";
    payload += String(g_lastHumidity, 1);
    payload += ",V:";
    payload += FIRMWARE_VERSION;
  } else {
    payload = "V:";
    payload += FIRMWARE_VERSION;
  }

  Serial.print("Sent version data: ");
  Serial.println(payload);

  notifyPayload(payload);
}

void setBrightness(int val) {
  g_brightness = constrain(val, 0, 255);
  Serial.print("Brightness updated: ");
  Serial.println(g_brightness);

  // TODO: Apply brightness to your LED matrix driver here.
}

void setAnimation(const String &mode) {
  g_currentAnimation = mode;
  Serial.print("Animation updated: ");
  Serial.println(g_currentAnimation);

  // TODO: Apply animation mode to your renderer here.
}

void syncTime(const String &data) {
  g_lastSyncedTimestamp = data;
  Serial.print("Time synced: ");
  Serial.println(g_lastSyncedTimestamp);

  // TODO: Parse and apply RTC/system time synchronization here.
}

void displayText(const String &text) {
  g_pendingDisplayText = text;
  Serial.print("Display text updated: ");
  Serial.println(g_pendingDisplayText);

  // TODO: Send text payload to your matrix display pipeline here.
}

void setMode(const String &mode) {
  g_currentMode = mode;
  Serial.print("Mode updated: ");
  Serial.println(g_currentMode);

  // TODO: Switch between clock/music/other runtime modes here.
}

void handleCommand(String cmd) {
  cmd.trim();
  if (cmd.isEmpty()) {
    Serial.println("Ignored empty command");
    return;
  }

  Serial.print("Received command: ");
  Serial.println(cmd);

  if (cmd.equalsIgnoreCase("GET_VERSION")) {
    sendVersionData();
    return;
  }

  if (cmd.equalsIgnoreCase("OTA_END")) {
    finishOta();
    return;
  }

  if (cmd.equalsIgnoreCase("OTA_ABORT")) {
    abortOta("aborted");
    return;
  }

  if (cmd.equalsIgnoreCase("OTA_STATUS")) {
    if (g_otaInProgress) {
      sendOtaStatus("IN_PROGRESS," + String(g_otaReceivedSize) + "/" + String(g_otaExpectedSize) +
                    "," + String(g_otaHasExpectedMd5 ? "MD5" : "NO_MD5"));
    } else {
      sendOtaStatus("IDLE");
    }
    return;
  }

  const int separatorIndex = cmd.indexOf(':');
  if (separatorIndex <= 0) {
    Serial.println("Invalid command format. Expected KEY:VALUE");
    return;
  }

  String commandKey = cmd.substring(0, separatorIndex);
  String commandValue = cmd.substring(separatorIndex + 1);
  commandKey.trim();
  commandValue.trim();
  commandKey.toUpperCase();

  if (commandKey == "OTA_BEGIN") {
    beginOta(commandValue);
    return;
  }

  if (commandKey == "OTA_CHUNK") {
    writeOtaChunk(commandValue);
    return;
  }

  if (commandKey == "BRIGHT") {
    const int brightness = commandValue.toInt();
    setBrightness(brightness);
    return;
  }

  if (commandKey == "SYNC") {
    syncTime(commandValue);
    return;
  }

  if (commandKey == "ANIM") {
    setAnimation(commandValue);
    return;
  }

  if (commandKey == "TEXT") {
    displayText(commandValue);
    return;
  }

  if (commandKey == "MODE") {
    setMode(commandValue);
    return;
  }

  Serial.print("Unknown command key: ");
  Serial.println(commandKey);
}

class ServerCallbacks : public BLEServerCallbacks {
 public:
  void onConnect(BLEServer *server) override {
    g_isDeviceConnected = true;
    g_restartAdvertisingPending = false;
    (void)server;
    Serial.println("Connected");
  }

  void onDisconnect(BLEServer *server) override {
    g_isDeviceConnected = false;
    g_disconnectTimestampMs = millis();
    g_restartAdvertisingPending = true;
    (void)server;
    Serial.println("Disconnected");

    if (g_otaInProgress) {
      abortOta("disconnected");
    }
  }
};

class CommandCallbacks : public BLECharacteristicCallbacks {
 public:
  void onWrite(BLECharacteristic *characteristic) override {
    const String rawValue = characteristic->getValue();
    if (rawValue.isEmpty()) {
      Serial.println("Write received with empty payload");
      return;
    }

    handleCommand(rawValue);
  }
};

void configureBlePowerAndAdvertising() {
  // Moderate TX power for a better battery/range balance.
  BLEDevice::setPower(ESP_PWR_LVL_P3);

  BLEAdvertising *advertising = BLEDevice::getAdvertising();
  advertising->addServiceUUID(SERVICE_UUID);
  advertising->setScanResponse(false);
  advertising->setMinPreferred(0x06);
  advertising->setMinPreferred(0x12);

  // Conservative intervals help reduce BLE power usage while remaining responsive.
  advertising->setMinInterval(160);  // 100 ms
  advertising->setMaxInterval(320);  // 200 ms
}

void initBle() {
  BLEDevice::init(DEVICE_NAME);

  g_bleServer = BLEDevice::createServer();
  g_bleServer->setCallbacks(new ServerCallbacks());

  BLEService *service = g_bleServer->createService(SERVICE_UUID);

  g_commandCharacteristic = service->createCharacteristic(
      COMMAND_CHAR_UUID,
      BLECharacteristic::PROPERTY_WRITE | BLECharacteristic::PROPERTY_WRITE_NR);
  g_commandCharacteristic->setCallbacks(new CommandCallbacks());

  g_sensorCharacteristic = service->createCharacteristic(
      SENSOR_CHAR_UUID,
      BLECharacteristic::PROPERTY_NOTIFY | BLECharacteristic::PROPERTY_READ);
  g_sensorCharacteristic->addDescriptor(new BLE2902());
  g_sensorCharacteristic->setValue("T:0.0,H:0.0");

  service->start();
  configureBlePowerAndAdvertising();
  BLEDevice::startAdvertising();

  Serial.println("BLE advertising started");
}

void restartAdvertisingIfNeeded() {
  if (!g_restartAdvertisingPending) {
    return;
  }

  if ((millis() - g_disconnectTimestampMs) < RESTART_ADV_DELAY_MS) {
    return;
  }

  BLEDevice::startAdvertising();
  g_restartAdvertisingPending = false;
  Serial.println("BLE advertising restarted");
}

void publishSensorDataIfDue() {
  if (g_otaInProgress) {
    return;
  }

  const uint32_t now = millis();
  if ((now - g_lastSensorPublishMs) < SENSOR_INTERVAL_MS) {
    return;
  }

  g_lastSensorPublishMs = now;

  float temperatureC = NAN;
  float humidity = NAN;
  if (!readSensorValues(temperatureC, humidity)) {
    return;
  }

  g_lastTemperatureC = temperatureC;
  g_lastHumidity = humidity;
  g_hasValidSensorSample = true;

  char payload[32];
  snprintf(payload, sizeof(payload), "T:%.1f,H:%.1f", temperatureC, humidity);

  Serial.print("Sent sensor data: ");
  Serial.println(payload);

  if (g_sensorCharacteristic != nullptr) {
    notifyPayload(payload);
  }
}

void serviceOtaState() {
  if (g_otaInProgress && (millis() - g_otaLastActivityMs) > OTA_TIMEOUT_MS) {
    abortOta("timeout");
  }

  if (g_rebootPending && millis() >= g_rebootScheduledAtMs) {
    Serial.println("Rebooting into updated firmware");
    ESP.restart();
  }
}

}  // namespace

void setup() {
  Serial.begin(SERIAL_BAUDRATE);
  delay(200);
  Serial.println();
  Serial.println("Booting DOTMATRIX BLE firmware");

  dht.begin();
  initBle();
}

void loop() {
  restartAdvertisingIfNeeded();
  serviceOtaState();
  publishSensorDataIfDue();

  // Keep loop cooperative and ready for future OTA/task expansion.
  yield();
}
