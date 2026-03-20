#include <Arduino.h>
#include <BLE2902.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLESecurity.h>
#include <DHT.h>
#include <arduinoFFT.h>
#include <MD_MAX72xx.h>
#include <MD_Parola.h>
#include <RTClib.h>
#include <Wire.h>
#include <esp_system.h>
#include <sys/time.h>
#include <time.h>
#include <Update.h>

namespace {

constexpr char DEVICE_NAME[] = "DotMatrix Clock";
constexpr uint16_t DEVICE_APPEARANCE = 0x0340;  // Clock

constexpr uint32_t RESTART_ADV_DELAY_MS = 250;
constexpr uint32_t BOOT_ADVERTISING_STABILIZE_MS = 2000;
constexpr uint32_t ADVERTISING_REFRESH_INTERVAL_MS = 10000;
constexpr uint32_t BATTERY_UPDATE_INTERVAL_MS = 10000;
constexpr uint32_t SENSOR_UPDATE_INTERVAL_MS = 5000;
constexpr uint32_t SENSOR_MIN_READ_INTERVAL_MS = 2200;
constexpr uint32_t OTA_TIMEOUT_MS = 20000;
constexpr uint32_t OTA_REBOOT_DELAY_MS = 1500;
constexpr uint32_t DISPLAY_UPDATE_INTERVAL_MS = 1000;
constexpr uint32_t RTC_WRITEBACK_INTERVAL_MS = 60000;
constexpr uint32_t CLOCK_TIME_VIEW_MS = 5000;
constexpr uint32_t CLOCK_DATE_VIEW_MS = 2000;
constexpr uint32_t CLOCK_TEMP_VIEW_MS = 2000;
constexpr uint32_t CLOCK_DAY_VIEW_MS = 2000;
constexpr uint32_t SERIAL_BAUDRATE = 115200;
constexpr uint32_t BLE_STATIC_PASSKEY = 123456;
constexpr int TIME_VALID_MIN_YEAR = 2024;

constexpr uint8_t DHT_PIN = 4;
constexpr uint8_t DHT_TYPE = DHT22;
constexpr uint8_t DISPLAY_HARDWARE_TYPE = MD_MAX72XX::FC16_HW;
constexpr uint8_t DISPLAY_MAX_DEVICES = 4;
constexpr uint8_t DISPLAY_PIN_DIN = 23;
constexpr uint8_t DISPLAY_PIN_CLK = 18;
constexpr uint8_t DISPLAY_PIN_CS = 5;
constexpr uint8_t DISPLAY_DEFAULT_INTENSITY = 2;
constexpr uint8_t BUTTON_MODE_PIN = 32;
constexpr uint8_t BUTTON_NEXT_PIN = 33;
constexpr uint8_t BUTTON_BACK_PIN = 25;
constexpr uint8_t BUTTON_SELECT_PIN = 26;
constexpr uint8_t BUZZER_PIN = 27;
constexpr bool BUZZER_ACTIVE_HIGH = true;
constexpr uint32_t BUTTON_DEBOUNCE_MS = 180;
constexpr uint32_t BUTTON_CLICK_MS = 35;
constexpr uint32_t VISUALIZER_FRAME_INTERVAL_MS = 180;
constexpr uint32_t BUZZER_BEEP_INTERVAL_MS = 250;
constexpr int MIC_INPUT_PIN = 34;
constexpr int MIC_SENSITIVITY_PIN = -1;
constexpr uint16_t FFT_SAMPLE_COUNT = 64;
constexpr double FFT_SAMPLING_FREQUENCY = 4000.0;

// Set this to an ADC-capable pin if you have a battery divider attached.
constexpr int BATTERY_SENSE_PIN = -1;
constexpr int BATTERY_ADC_MIN = 1800;
constexpr int BATTERY_ADC_MAX = 3200;

constexpr char CUSTOM_SERVICE_UUID[] = "6b5f9001-3d10-4f76-8e22-4b0d6e6a1001";
constexpr char CUSTOM_RX_UUID[] = "6b5f9002-3d10-4f76-8e22-4b0d6e6a1001";
constexpr char CUSTOM_TX_UUID[] = "6b5f9003-3d10-4f76-8e22-4b0d6e6a1001";

constexpr char DEVICE_INFO_SERVICE_UUID[] = "180A";
constexpr char BATTERY_SERVICE_UUID[] = "180F";
constexpr char BATTERY_LEVEL_UUID[] = "2A19";
constexpr char MANUFACTURER_NAME_UUID[] = "2A29";
constexpr char MODEL_NUMBER_UUID[] = "2A24";
constexpr char FIRMWARE_REVISION_UUID[] = "2A26";

constexpr char MANUFACTURER_NAME[] = "DotMatrix Labs";
constexpr char MODEL_NUMBER[] = "DM-CLOCK-01";

#ifndef FIRMWARE_VERSION
#define FIRMWARE_VERSION "v1.0.2"
#endif
constexpr char FIRMWARE_REVISION[] = FIRMWARE_VERSION;

#ifndef OTA_SHARED_SECRET
#define OTA_SHARED_SECRET ""
#endif
constexpr char OTA_SHARED_SECRET_STR[] = OTA_SHARED_SECRET;

constexpr size_t OTA_MAX_CHUNK_BYTES = 252;

DHT g_dht(DHT_PIN, DHT_TYPE);
RTC_DS1307 g_rtc;
MD_Parola g_matrix(static_cast<MD_MAX72XX::moduleType_t>(DISPLAY_HARDWARE_TYPE),
                   DISPLAY_PIN_CS,
                   DISPLAY_MAX_DEVICES);
ArduinoFFT<double> g_fft = ArduinoFFT<double>();
BLEServer *g_server = nullptr;
BLECharacteristic *g_batteryCharacteristic = nullptr;
BLECharacteristic *g_txCharacteristic = nullptr;

enum TimeSourceState {
  TIME_INVALID,
  TIME_SOURCE_RTC,
  TIME_SOURCE_PHONE_SYNC,
};

enum DisplayMode {
  DISPLAY_MODE_CLOCK,
  DISPLAY_MODE_MESSAGE,
  DISPLAY_MODE_VISUALIZER,
  DISPLAY_MODE_SETTINGS,
};

enum TimeDisplayFormat {
  TIME_FORMAT_24H,
  TIME_FORMAT_12H,
};

enum VisualizerSource {
  VISUALIZER_SOURCE_DEVICE,
  VISUALIZER_SOURCE_PHONE,
};

enum VisualizerStyle {
  VISUALIZER_STYLE_BARS,
  VISUALIZER_STYLE_WAVE,
  VISUALIZER_STYLE_RADIAL,
};

enum MessageAnimationStyle {
  MESSAGE_ANIM_NONE,
  MESSAGE_ANIM_WAVE,
  MESSAGE_ANIM_SCROLL,
  MESSAGE_ANIM_RAIN,
};

enum ClockTool {
  CLOCK_TOOL_TIME,
  CLOCK_TOOL_ALARM,
  CLOCK_TOOL_TIMER,
  CLOCK_TOOL_STOPWATCH,
};

enum AlarmEditField {
  ALARM_EDIT_HOUR,
  ALARM_EDIT_MINUTE,
};

enum TimerEditField {
  TIMER_EDIT_MINUTES,
  TIMER_EDIT_SECONDS,
};

bool g_isConnected = false;
bool g_restartAdvertisingPending = false;
uint32_t g_bootTimestampMs = 0;
uint32_t g_disconnectTimestampMs = 0;
uint32_t g_lastAdvertisingKickMs = 0;
uint32_t g_lastBatteryUpdateMs = 0;
uint32_t g_lastSensorUpdateMs = 0;
uint32_t g_lastSensorReadAttemptMs = 0;
uint32_t g_otaLastActivityMs = 0;
uint32_t g_otaRebootAtMs = 0;
uint32_t g_lastDisplayUpdateMs = 0;
uint32_t g_lastRtcWriteBackMs = 0;
uint32_t g_lastVisualizerFrameMs = 0;
uint32_t g_lastPhoneVisualizerUpdateMs = 0;
uint32_t g_clockModeCycleStartMs = 0;
float g_lastTemperatureC = NAN;
float g_lastHumidity = NAN;
bool g_hasSensorReading = false;
bool g_otaInProgress = false;
bool g_otaHasExpectedMd5 = false;
bool g_otaRebootPending = false;
size_t g_otaExpectedSize = 0;
size_t g_otaReceivedSize = 0;
String g_otaExpectedMd5;
bool g_batteryAvailable = false;
uint8_t g_batteryLevel = 0;
uint8_t g_displayBrightness = DISPLAY_DEFAULT_INTENSITY;
bool g_rtcAvailable = false;
bool g_timeNotSet = true;
bool g_lastDisplayedTimeValid = false;
bool g_lastDisplayPlaceholder = false;
TimeSourceState g_timeSourceState = TIME_INVALID;
TimeDisplayFormat g_timeDisplayFormat = TIME_FORMAT_24H;
VisualizerSource g_visualizerSource = VISUALIZER_SOURCE_DEVICE;
VisualizerStyle g_visualizerStyle = VISUALIZER_STYLE_BARS;
MessageAnimationStyle g_messageAnimationStyle = MESSAGE_ANIM_SCROLL;
String g_lastDisplayedTimeText;
String g_lastDisplayedStatusText;
DisplayMode g_currentMode = DISPLAY_MODE_CLOCK;
DisplayMode g_pendingMenuMode = DISPLAY_MODE_CLOCK;
String g_appMessage = "HELLO";
String g_activeScrollText = "HELLO";
bool g_menuActive = false;
bool g_clockToolsMenuActive = false;
bool g_messageModeDirty = true;
uint8_t g_visualizerPhase = 0;
double g_fftReal[FFT_SAMPLE_COUNT];
double g_fftImag[FFT_SAMPLE_COUNT];
uint8_t g_phoneVisualizerLevels[32] = {};
uint8_t g_lastVisualizerLevels[32] = {};
ClockTool g_activeClockTool = CLOCK_TOOL_TIME;
ClockTool g_pendingClockTool = CLOCK_TOOL_TIME;
int g_alarmHour = 7;
int g_alarmMinute = 0;
bool g_alarmEnabled = false;
bool g_alarmRinging = false;
bool g_alarmEditMode = false;
AlarmEditField g_alarmEditField = ALARM_EDIT_HOUR;
uint32_t g_lastAlarmTriggerMinuteEpoch = 0;
int32_t g_timerTotalSeconds = 0;
int32_t g_timerRemainingSeconds = 0;
bool g_timerRunning = false;
uint32_t g_timerLastTickMs = 0;
bool g_timerFinished = false;
bool g_timerEditMode = false;
TimerEditField g_timerEditField = TIMER_EDIT_MINUTES;
bool g_timerConfigured = false;
bool g_stopwatchRunning = false;
uint32_t g_stopwatchElapsedMs = 0;
uint32_t g_stopwatchLastTickMs = 0;
uint32_t g_stopwatchLapMs = 0;
bool g_stopwatchLapFrozen = false;
bool g_buzzerOutputOn = false;
uint32_t g_lastBuzzerToggleMs = 0;
uint32_t g_buttonClickUntilMs = 0;
const uint8_t kSpectralHeight[9] = {
    0b00000000,
    0b10000000,
    0b11000000,
    0b11100000,
    0b11110000,
    0b11111000,
    0b11111100,
    0b11111110,
    0b11111111,
};

struct ButtonState {
  uint8_t pin;
  uint32_t lastPressMs;
};

ButtonState g_modeButton{BUTTON_MODE_PIN, 0};
ButtonState g_nextButton{BUTTON_NEXT_PIN, 0};
ButtonState g_backButton{BUTTON_BACK_PIN, 0};
ButtonState g_selectButton{BUTTON_SELECT_PIN, 0};

String buildSystemInfoMessage();
void notifyTx(const String &message);
void renderDisplayText(const String &text);
void updateDisplayTime();
void updateClockToolDisplay();
void updateSettingsDisplay();
void handleVirtualButton(const String &buttonName);
void startAdvertisingNow(const char *reason, bool forceRestart);
String buildAlarmStateMessage();
String buildTimerStateMessage();
String buildStopwatchStateMessage();

void writeBuzzer(bool enabled) {
  g_buzzerOutputOn = enabled;
  digitalWrite(BUZZER_PIN, enabled == BUZZER_ACTIVE_HIGH ? HIGH : LOW);
}

void triggerButtonClick() {
  if (g_alarmRinging || g_timerFinished) {
    return;
  }
  g_buttonClickUntilMs = millis() + BUTTON_CLICK_MS;
}

void stopBuzzerAlerts() {
  const bool wasActive = g_alarmRinging || g_timerFinished || g_buzzerOutputOn;
  g_alarmRinging = false;
  g_timerFinished = false;
  writeBuzzer(false);
  if (wasActive) {
    updateClockToolDisplay();
    notifyTx("STATUS:ALERT_STOPPED");
  }
}

const char *clockToolToString(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:
      return "ALARM";
    case CLOCK_TOOL_TIMER:
      return "TIMER";
    case CLOCK_TOOL_STOPWATCH:
      return "STOPWATCH";
    case CLOCK_TOOL_TIME:
    default:
      return "TIME";
  }
}

const char *clockToolLabel(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:
      return "ALRM";
    case CLOCK_TOOL_TIMER:
      return "TIMR";
    case CLOCK_TOOL_STOPWATCH:
      return "STOP";
    case CLOCK_TOOL_TIME:
    default:
      return "TIME";
  }
}

const char *clockToolMenuText(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:
      return "ALARM SET";
    case CLOCK_TOOL_TIMER:
      return "TIMER SET";
    case CLOCK_TOOL_STOPWATCH:
      return "STOPWATCH";
    case CLOCK_TOOL_TIME:
    default:
      return "TIME";
  }
}

const char *displayModeToString(DisplayMode mode) {
  switch (mode) {
    case DISPLAY_MODE_SETTINGS:
      return "SETTINGS";
    case DISPLAY_MODE_MESSAGE:
      return "MESSAGE";
    case DISPLAY_MODE_VISUALIZER:
      return "VISUALIZER";
    case DISPLAY_MODE_CLOCK:
    default:
      return "CLOCK";
  }
}

const char *displayModeLabel(DisplayMode mode) {
  switch (mode) {
    case DISPLAY_MODE_SETTINGS:
      return "SET";
    case DISPLAY_MODE_MESSAGE:
      return "MSG";
    case DISPLAY_MODE_VISUALIZER:
      return "VIZ";
    case DISPLAY_MODE_CLOCK:
    default:
      return "CLK";
  }
}

const char *timeFormatToString(TimeDisplayFormat format) {
  switch (format) {
    case TIME_FORMAT_12H:
      return "12";
    case TIME_FORMAT_24H:
    default:
      return "24";
  }
}

const char *visualizerSourceToString(VisualizerSource source) {
  switch (source) {
    case VISUALIZER_SOURCE_PHONE:
      return "PHONE";
    case VISUALIZER_SOURCE_DEVICE:
    default:
      return "DEVICE";
  }
}

const char *visualizerStyleToString(VisualizerStyle style) {
  switch (style) {
    case VISUALIZER_STYLE_WAVE:
      return "WAVE";
    case VISUALIZER_STYLE_RADIAL:
      return "RADIAL";
    case VISUALIZER_STYLE_BARS:
    default:
      return "BARS";
  }
}

const char *messageAnimationToString(MessageAnimationStyle style) {
  switch (style) {
    case MESSAGE_ANIM_NONE:
      return "NONE";
    case MESSAGE_ANIM_WAVE:
      return "WAVE";
    case MESSAGE_ANIM_RAIN:
      return "RAIN";
    case MESSAGE_ANIM_SCROLL:
    default:
      return "SCROLL";
  }
}

const char *timeSourceToString(TimeSourceState state) {
  switch (state) {
    case TIME_SOURCE_RTC:
      return "RTC";
    case TIME_SOURCE_PHONE_SYNC:
      return "PHONE_SYNC";
    case TIME_INVALID:
    default:
      return "NONE";
  }
}

bool isDateTimeValid(const DateTime &value) {
  if (value.year() < TIME_VALID_MIN_YEAR) {
    return false;
  }
  if (value.month() < 1 || value.month() > 12) {
    return false;
  }
  if (value.day() < 1 || value.day() > 31) {
    return false;
  }
  if (value.hour() > 23 || value.minute() > 59 || value.second() > 59) {
    return false;
  }
  return true;
}

uint8_t clampDisplayBrightness(int value) {
  return static_cast<uint8_t>(constrain(value, 0, 15));
}

DateTime getSystemDateTime() {
  time_t now = time(nullptr);
  struct tm timeInfo = {};
  localtime_r(&now, &timeInfo);
  return DateTime(timeInfo.tm_year + 1900,
                  timeInfo.tm_mon + 1,
                  timeInfo.tm_mday,
                  timeInfo.tm_hour,
                  timeInfo.tm_min,
                  timeInfo.tm_sec);
}

void setSystemClock(const DateTime &value) {
  struct tm timeInfo = {};
  timeInfo.tm_year = value.year() - 1900;
  timeInfo.tm_mon = value.month() - 1;
  timeInfo.tm_mday = value.day();
  timeInfo.tm_hour = value.hour();
  timeInfo.tm_min = value.minute();
  timeInfo.tm_sec = value.second();
  timeInfo.tm_isdst = -1;

  const time_t epoch = mktime(&timeInfo);
  timeval now = {};
  now.tv_sec = epoch;
  settimeofday(&now, nullptr);
}

void showMenuSelection() {
  const char *label = displayModeLabel(g_pendingMenuMode);
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == label) {
    return;
  }

  g_lastDisplayPlaceholder = true;
  g_lastDisplayedStatusText = label;
  g_matrix.displayClear();
  g_matrix.displayText(label, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.printf("Menu selection: %s\n", label);
}

void showClockToolSelection() {
  const char *label = clockToolLabel(g_pendingClockTool);
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == label) {
    return;
  }

  g_lastDisplayPlaceholder = true;
  g_lastDisplayedStatusText = label;
  g_matrix.displayClear();
  g_matrix.displayText(label, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.printf("Clock tool selection: %s\n", label);
}

String buildAlarmDisplayText() {
  if (g_alarmRinging) {
    return String("ALRM");
  }

  char buffer[6];
  if (g_alarmEditMode) {
    if (g_alarmEditField == ALARM_EDIT_HOUR) {
      snprintf(buffer, sizeof(buffer), "H%02d", g_alarmHour);
    } else {
      snprintf(buffer, sizeof(buffer), "M%02d", g_alarmMinute);
    }
  } else {
    snprintf(buffer, sizeof(buffer), "%02d:%02d", g_alarmHour, g_alarmMinute);
  }
  return String(buffer);
}

String buildTimerDisplayText() {
  char buffer[6];
  if (g_timerEditMode) {
    const int32_t total = max<int32_t>(g_timerTotalSeconds, 0);
    const int32_t minutes = total / 60;
    const int32_t seconds = total % 60;
    if (g_timerEditField == TIMER_EDIT_MINUTES) {
      snprintf(buffer, sizeof(buffer), "M%02ld", static_cast<long>(minutes));
    } else {
      snprintf(buffer, sizeof(buffer), "S%02ld", static_cast<long>(seconds));
    }
  } else {
    const int32_t seconds = max<int32_t>(g_timerRemainingSeconds, 0);
    const int32_t minutes = seconds / 60;
    const int32_t remSeconds = seconds % 60;
    snprintf(buffer, sizeof(buffer), "%02ld:%02ld", static_cast<long>(minutes), static_cast<long>(remSeconds));
  }
  return String(buffer);
}

String buildStopwatchDisplayText() {
  const uint32_t baseMs = g_stopwatchLapFrozen ? g_stopwatchLapMs : g_stopwatchElapsedMs;
  const uint32_t totalSeconds = baseMs / 1000;
  const uint32_t minutes = (totalSeconds / 60) % 100;
  const uint32_t seconds = totalSeconds % 60;
  char buffer[6];
  snprintf(buffer, sizeof(buffer), "%02lu:%02lu", static_cast<unsigned long>(minutes),
           static_cast<unsigned long>(seconds));
  return String(buffer);
}

void updateClockToolDisplay() {
  if (g_currentMode != DISPLAY_MODE_CLOCK || g_menuActive || g_clockToolsMenuActive) {
    return;
  }

  switch (g_activeClockTool) {
    case CLOCK_TOOL_ALARM:
      renderDisplayText(buildAlarmDisplayText());
      break;
    case CLOCK_TOOL_TIMER:
      renderDisplayText(g_timerFinished ? String("DONE") : buildTimerDisplayText());
      break;
    case CLOCK_TOOL_STOPWATCH:
      renderDisplayText(buildStopwatchDisplayText());
      break;
    case CLOCK_TOOL_TIME:
    default:
      updateDisplayTime();
      break;
  }
}

void updateSettingsDisplay() {
  if (g_currentMode != DISPLAY_MODE_SETTINGS || g_menuActive) {
    return;
  }

  char buffer[6];
  snprintf(buffer, sizeof(buffer), "B%02u", static_cast<unsigned>(g_displayBrightness));
  renderDisplayText(String(buffer));
}

void activateClockTool(ClockTool tool) {
  g_currentMode = DISPLAY_MODE_CLOCK;
  g_activeClockTool = tool;
  g_menuActive = false;
  g_clockToolsMenuActive = false;
  g_alarmEditMode = false;
  g_timerEditMode = false;
  g_stopwatchLapFrozen = false;
  g_lastDisplayedStatusText = "";
  g_lastDisplayedTimeText = "";
  updateClockToolDisplay();
}

String buildAlarmStateMessage() {
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "ALARM:%02d:%02d,%s",
           g_alarmHour,
           g_alarmMinute,
           g_alarmEnabled ? "ON" : "OFF");
  return String(buffer);
}

String buildTimerStateMessage() {
  const int32_t seconds = max<int32_t>(g_timerRemainingSeconds, 0);
  const int32_t minutes = seconds / 60;
  const int32_t remSeconds = seconds % 60;
  const char *state = g_timerFinished ? "DONE" : (g_timerRunning ? "RUNNING" : "PAUSED");
  char buffer[40];
  snprintf(buffer, sizeof(buffer), "TIMER:%02ld:%02ld,%s",
           static_cast<long>(minutes),
           static_cast<long>(remSeconds),
           state);
  return String(buffer);
}

String buildStopwatchStateMessage() {
  const uint32_t baseMs = g_stopwatchLapFrozen ? g_stopwatchLapMs : g_stopwatchElapsedMs;
  const uint32_t totalSeconds = baseMs / 1000;
  const uint32_t minutes = (totalSeconds / 60) % 100;
  const uint32_t seconds = totalSeconds % 60;
  const char *state = g_stopwatchRunning ? "RUNNING" : "PAUSED";
  char buffer[40];
  snprintf(buffer, sizeof(buffer), "SW:%02lu:%02lu,%s",
           static_cast<unsigned long>(minutes),
           static_cast<unsigned long>(seconds),
           state);
  return String(buffer);
}

void startMessageDisplay() {
  g_activeScrollText = g_appMessage;
  g_activeScrollText.trim();
  if (g_activeScrollText.isEmpty()) {
    g_activeScrollText = "NO MSG";
  }

  g_matrix.displayClear();
  textEffect_t entryEffect = PA_SCROLL_LEFT;
  textEffect_t exitEffect = PA_SCROLL_LEFT;
  uint16_t speed = 50;

  switch (g_messageAnimationStyle) {
    case MESSAGE_ANIM_NONE:
      entryEffect = PA_PRINT;
      exitEffect = PA_NO_EFFECT;
      speed = 0;
      break;
    case MESSAGE_ANIM_WAVE:
      entryEffect = PA_SCROLL_UP;
      exitEffect = PA_SCROLL_DOWN;
      speed = 40;
      break;
    case MESSAGE_ANIM_RAIN:
      entryEffect = PA_SCROLL_DOWN;
      exitEffect = PA_SCROLL_DOWN;
      speed = 40;
      break;
    case MESSAGE_ANIM_SCROLL:
    default:
      entryEffect = PA_SCROLL_LEFT;
      exitEffect = PA_SCROLL_LEFT;
      speed = 50;
      break;
  }

  g_matrix.displayText(g_activeScrollText.c_str(), PA_LEFT, speed, 0, entryEffect, exitEffect);
  g_matrix.displayReset();
  g_messageModeDirty = false;
  Serial.print("Message mode text: ");
  Serial.println(g_activeScrollText);
}

void applyDisplayBrightness(uint8_t brightness) {
  g_displayBrightness = clampDisplayBrightness(brightness);
  g_matrix.setIntensity(g_displayBrightness);
  Serial.printf("Display brightness: %u\n", static_cast<unsigned>(g_displayBrightness));
}

void drawVisualizerBars(const uint8_t *levels) {
  MD_MAX72XX *graphics = g_matrix.getGraphicObject();
  if (graphics == nullptr) {
    return;
  }

  for (uint8_t i = 0; i < 32; ++i) {
    const uint8_t level = constrain(levels[i], 0, 8);
    graphics->setColumn(31 - i, kSpectralHeight[level]);
  }
  g_matrix.displayAnimate();
}

void drawVisualizerWave(const uint8_t *levels) {
  MD_MAX72XX *graphics = g_matrix.getGraphicObject();
  if (graphics == nullptr) {
    return;
  }

  graphics->clear();
  for (uint8_t i = 0; i < 32; ++i) {
    const uint8_t level = constrain(levels[i], 0, 8);
    const uint8_t row = level == 0 ? 7 : static_cast<uint8_t>(8 - level);
    graphics->setPoint(row, 31 - i, true);
  }
  g_matrix.displayAnimate();
}

void drawVisualizerRadial(const uint8_t *levels) {
  MD_MAX72XX *graphics = g_matrix.getGraphicObject();
  if (graphics == nullptr) {
    return;
  }

  graphics->clear();
  uint16_t total = 0;
  for (uint8_t i = 0; i < 32; ++i) {
    total += constrain(levels[i], 0, 8);
  }

  const uint8_t radius = static_cast<uint8_t>(map(total, 0, 32 * 8, 0, 16));
  for (uint8_t offset = 0; offset < radius && offset < 16; ++offset) {
    graphics->setColumn(15 - offset, kSpectralHeight[min<uint8_t>(offset / 2 + 1, 8)]);
    graphics->setColumn(16 + offset, kSpectralHeight[min<uint8_t>(offset / 2 + 1, 8)]);
  }
  g_matrix.displayAnimate();
}

void drawVisualizerFrame(const uint8_t *levels) {
  memcpy(g_lastVisualizerLevels, levels, sizeof(g_lastVisualizerLevels));

  switch (g_visualizerStyle) {
    case VISUALIZER_STYLE_WAVE:
      drawVisualizerWave(levels);
      break;
    case VISUALIZER_STYLE_RADIAL:
      drawVisualizerRadial(levels);
      break;
    case VISUALIZER_STYLE_BARS:
    default:
      drawVisualizerBars(levels);
      break;
  }
}

void renderDeviceVisualizerFrame() {
  int sensitivity = 80;
  if (MIC_SENSITIVITY_PIN >= 0) {
    sensitivity = map(analogRead(MIC_SENSITIVITY_PIN), 0, 4095, 50, 100);
  }

  for (uint16_t i = 0; i < FFT_SAMPLE_COUNT; ++i) {
    g_fftReal[i] = analogRead(MIC_INPUT_PIN) / static_cast<double>(sensitivity);
    g_fftImag[i] = 0;
    delayMicroseconds(static_cast<uint32_t>(1000000.0 / FFT_SAMPLING_FREQUENCY));
  }

  g_fft.windowing(g_fftReal, FFT_SAMPLE_COUNT, FFT_WIN_TYP_HAMMING, FFT_FORWARD);
  g_fft.compute(g_fftReal, g_fftImag, FFT_SAMPLE_COUNT, FFT_FORWARD);
  g_fft.complexToMagnitude(g_fftReal, g_fftImag, FFT_SAMPLE_COUNT);

  uint8_t levels[32];
  for (uint8_t i = 0; i < 32; ++i) {
    double magnitude = constrain(g_fftReal[i], 0.0, 80.0);
    levels[i] = static_cast<uint8_t>(map(static_cast<int>(magnitude), 0, 80, 0, 8));
  }

  drawVisualizerFrame(levels);
}

void renderPhoneVisualizerFrame() {
  drawVisualizerFrame(g_phoneVisualizerLevels);
}

bool parsePhoneVisualizerFrame(const String &payload) {
  uint8_t parsedLevels[32] = {};
  size_t count = 0;
  int start = 0;

  while (start <= payload.length() && count < 32) {
    int commaIndex = payload.indexOf(',', start);
    String token = commaIndex >= 0 ? payload.substring(start, commaIndex) : payload.substring(start);
    token.trim();
    if (token.isEmpty()) {
      return false;
    }

    const int level = token.toInt();
    if (level < 0 || level > 8) {
      return false;
    }
    parsedLevels[count++] = static_cast<uint8_t>(level);

    if (commaIndex < 0) {
      break;
    }
    start = commaIndex + 1;
  }

  if (count != 32) {
    return false;
  }

  memcpy(g_phoneVisualizerLevels, parsedLevels, sizeof(g_phoneVisualizerLevels));
  g_lastPhoneVisualizerUpdateMs = millis();
  return true;
}

void applyDisplayMode(DisplayMode mode) {
  g_currentMode = mode;
  g_menuActive = false;
  g_clockToolsMenuActive = false;
  g_lastDisplayPlaceholder = false;
  g_lastDisplayedStatusText = "";
  g_lastDisplayedTimeText = "";
  g_messageModeDirty = true;
  g_clockModeCycleStartMs = millis();

  switch (g_currentMode) {
    case DISPLAY_MODE_MESSAGE:
      startMessageDisplay();
      break;
    case DISPLAY_MODE_VISUALIZER:
      g_lastDisplayedTimeText = "";
      g_lastDisplayedStatusText = "";
      if (g_visualizerSource == VISUALIZER_SOURCE_DEVICE) {
        renderDeviceVisualizerFrame();
      } else {
        renderPhoneVisualizerFrame();
      }
      break;
    case DISPLAY_MODE_SETTINGS:
      updateSettingsDisplay();
      break;
    case DISPLAY_MODE_CLOCK:
    default:
      updateDisplayTime();
      break;
  }

  Serial.printf("Active mode: %s\n", displayModeToString(g_currentMode));
}

bool wasButtonPressed(ButtonState &button, uint32_t nowMs) {
  if (digitalRead(button.pin) != LOW) {
    return false;
  }

  if ((nowMs - button.lastPressMs) < BUTTON_DEBOUNCE_MS) {
    return false;
  }

  button.lastPressMs = nowMs;
  return true;
}

void serviceButtons() {
  const uint32_t now = millis();

  if (wasButtonPressed(g_modeButton, now)) {
    triggerButtonClick();
    handleVirtualButton("MODE");
  }

  if (wasButtonPressed(g_nextButton, now)) {
    triggerButtonClick();
    handleVirtualButton("NEXT");
  }

  if (wasButtonPressed(g_backButton, now)) {
    triggerButtonClick();
    handleVirtualButton("BACK");
  }

  if (wasButtonPressed(g_selectButton, now)) {
    triggerButtonClick();
    handleVirtualButton("SELECT");
  }
}

void handleVirtualButton(const String &buttonName) {
  if (g_alarmRinging || g_timerFinished) {
    stopBuzzerAlerts();
    notifyTx("BTN:ALERT_STOP");
    return;
  }

  if (buttonName.equalsIgnoreCase("MODE")) {
    if (g_currentMode == DISPLAY_MODE_CLOCK && g_activeClockTool == CLOCK_TOOL_ALARM && g_alarmEditMode) {
      g_alarmEditMode = false;
      updateClockToolDisplay();
      notifyTx("BTN:MODE");
      return;
    }

    if (g_currentMode == DISPLAY_MODE_CLOCK && g_activeClockTool == CLOCK_TOOL_TIMER && g_timerEditMode) {
      g_timerEditMode = false;
      g_timerRemainingSeconds = g_timerTotalSeconds;
      g_timerConfigured = g_timerTotalSeconds > 0;
      updateClockToolDisplay();
      notifyTx("BTN:MODE");
      return;
    }

    if (g_clockToolsMenuActive) {
      g_clockToolsMenuActive = false;
      updateClockToolDisplay();
      notifyTx("BTN:MODE");
      return;
    }

    g_menuActive = !g_menuActive;
    g_pendingMenuMode = g_currentMode;
    if (g_menuActive) {
      showMenuSelection();
    } else {
      applyDisplayMode(g_currentMode);
    }
    notifyTx("BTN:MODE");
    return;
  }

  if (g_clockToolsMenuActive) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      g_pendingClockTool = static_cast<ClockTool>((g_pendingClockTool + 1) % 4);
      showClockToolSelection();
      notifyTx("BTN:NEXT");
      return;
    }

    if (buttonName.equalsIgnoreCase("BACK")) {
      g_pendingClockTool = static_cast<ClockTool>((g_pendingClockTool + 3) % 4);
      showClockToolSelection();
      notifyTx("BTN:BACK");
      return;
    }

    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      activateClockTool(g_pendingClockTool);
      if (g_timerTotalSeconds <= 0) {
        g_timerConfigured = false;
      }
      notifyTx("BTN:SELECT");
      return;
    }
  }

  if (g_menuActive) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      g_pendingMenuMode = static_cast<DisplayMode>((g_pendingMenuMode + 1) % 4);
      showMenuSelection();
      notifyTx("BTN:NEXT");
      return;
    }

    if (buttonName.equalsIgnoreCase("BACK")) {
      g_pendingMenuMode = static_cast<DisplayMode>((g_pendingMenuMode + 3) % 4);
      showMenuSelection();
      notifyTx("BTN:BACK");
      return;
    }

    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      if (g_pendingMenuMode == DISPLAY_MODE_CLOCK) {
        g_menuActive = false;
        g_clockToolsMenuActive = true;
        g_pendingClockTool = g_activeClockTool;
        showClockToolSelection();
      } else {
        applyDisplayMode(g_pendingMenuMode);
      }
      notifyTx("BTN:SELECT");
      return;
    }
  }

  if (g_currentMode == DISPLAY_MODE_SETTINGS) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      applyDisplayBrightness(clampDisplayBrightness(g_displayBrightness + 1));
      updateSettingsDisplay();
      notifyTx("STATUS:BRIGHTNESS_UPDATED");
      return;
    }

    if (buttonName.equalsIgnoreCase("BACK")) {
      applyDisplayBrightness(clampDisplayBrightness(static_cast<int>(g_displayBrightness) - 1));
      updateSettingsDisplay();
      notifyTx("STATUS:BRIGHTNESS_UPDATED");
      return;
    }

    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      notifyTx("BRIGHTNESS:" + String(g_displayBrightness));
      return;
    }
  }

  if (g_currentMode == DISPLAY_MODE_CLOCK) {
    if (g_activeClockTool == CLOCK_TOOL_ALARM) {
      if (buttonName.equalsIgnoreCase("NEXT")) {
        if (g_alarmEditMode) {
          if (g_alarmEditField == ALARM_EDIT_HOUR) {
            g_alarmHour = (g_alarmHour + 1) % 24;
          } else {
            g_alarmMinute = (g_alarmMinute + 1) % 60;
          }
        } else {
          g_alarmMinute = (g_alarmMinute + 1) % 60;
        }
        updateClockToolDisplay();
        notifyTx("BTN:NEXT");
        return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        if (g_alarmEditMode) {
          if (g_alarmEditField == ALARM_EDIT_HOUR) {
            g_alarmHour = (g_alarmHour + 23) % 24;
          } else {
            g_alarmMinute = (g_alarmMinute + 59) % 60;
          }
        } else {
          g_alarmMinute = (g_alarmMinute + 59) % 60;
        }
        updateClockToolDisplay();
        notifyTx("BTN:BACK");
        return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        if (!g_alarmEditMode) {
          g_alarmEditMode = true;
          g_alarmEditField = ALARM_EDIT_HOUR;
        } else if (g_alarmEditField == ALARM_EDIT_HOUR) {
          g_alarmEditField = ALARM_EDIT_MINUTE;
        } else {
          g_alarmEditMode = false;
          g_alarmEnabled = !g_alarmEnabled;
          g_alarmRinging = false;
          activateClockTool(CLOCK_TOOL_TIME);
        }
        updateClockToolDisplay();
        notifyTx("BTN:SELECT");
        return;
      }
    }

    if (g_activeClockTool == CLOCK_TOOL_TIMER) {
      if (buttonName.equalsIgnoreCase("NEXT") && !g_timerRunning) {
        if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES) {
            g_timerTotalSeconds = min<int32_t>(g_timerTotalSeconds + 60, 99 * 60 + 59);
          } else {
            const int32_t minutes = g_timerTotalSeconds / 60;
            const int32_t seconds = (g_timerTotalSeconds + 1) % 60;
            g_timerTotalSeconds = minutes * 60 + seconds;
          }
        } else {
          g_timerTotalSeconds = min<int32_t>(g_timerTotalSeconds + 60, 99 * 60 + 59);
        }
        g_timerRemainingSeconds = g_timerTotalSeconds;
        g_timerFinished = false;
        g_timerConfigured = g_timerTotalSeconds > 0;
        updateClockToolDisplay();
        notifyTx("BTN:NEXT");
        return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        if (g_timerRunning) {
          g_timerRunning = false;
        } else if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES) {
            g_timerTotalSeconds = max<int32_t>(g_timerTotalSeconds - 60, 0);
          } else {
            const int32_t minutes = g_timerTotalSeconds / 60;
            int32_t seconds = g_timerTotalSeconds % 60;
            seconds = (seconds + 59) % 60;
            g_timerTotalSeconds = minutes * 60 + seconds;
          }
        } else {
          g_timerTotalSeconds = max<int32_t>(g_timerTotalSeconds - 60, 0);
        }
        g_timerRemainingSeconds = g_timerTotalSeconds;
        g_timerFinished = false;
        g_timerConfigured = g_timerTotalSeconds > 0;
        updateClockToolDisplay();
        notifyTx("BTN:BACK");
        return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        if (g_timerFinished) {
          g_timerFinished = false;
          g_timerRemainingSeconds = g_timerTotalSeconds;
          g_timerConfigured = g_timerTotalSeconds > 0;
        } else if (!g_timerRunning && !g_timerEditMode && !g_timerConfigured) {
          g_timerEditMode = true;
          g_timerEditField = TIMER_EDIT_MINUTES;
        } else if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES) {
            g_timerEditField = TIMER_EDIT_SECONDS;
          } else {
            g_timerEditMode = false;
            g_timerRemainingSeconds = g_timerTotalSeconds;
            g_timerConfigured = g_timerTotalSeconds > 0;
          }
        } else if (g_timerConfigured && g_timerRemainingSeconds > 0) {
          g_timerRunning = !g_timerRunning;
          g_timerLastTickMs = millis();
        }
        updateClockToolDisplay();
        notifyTx("BTN:SELECT");
        return;
      }
    }

    if (g_activeClockTool == CLOCK_TOOL_STOPWATCH) {
      if (buttonName.equalsIgnoreCase("NEXT")) {
        if (g_stopwatchRunning) {
          g_stopwatchLapMs = g_stopwatchElapsedMs;
          g_stopwatchLapFrozen = !g_stopwatchLapFrozen;
        } else {
          g_stopwatchElapsedMs = 0;
          g_stopwatchLapMs = 0;
          g_stopwatchLapFrozen = false;
        }
        updateClockToolDisplay();
        notifyTx("BTN:NEXT");
        return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        g_stopwatchElapsedMs = 0;
        g_stopwatchLapMs = 0;
        g_stopwatchLapFrozen = false;
        g_stopwatchRunning = false;
        updateClockToolDisplay();
        notifyTx("BTN:BACK");
        return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        g_stopwatchRunning = !g_stopwatchRunning;
        g_stopwatchLastTickMs = millis();
        updateClockToolDisplay();
        notifyTx("BTN:SELECT");
        return;
      }
    }
  }

  notifyTx("BTN:UNKNOWN");
}

void renderDisplayText(const String &text) {
  if (!g_lastDisplayPlaceholder && g_lastDisplayedTimeText == text) {
    return;
  }

  g_lastDisplayPlaceholder = false;
  g_lastDisplayedTimeText = text;
  g_matrix.displayClear();
  g_matrix.displayText(text.c_str(), PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.print("Display time: ");
  Serial.println(text);
}

String buildClockModeDisplayText(const DateTime &now, uint32_t nowMs) {
  static const char *kWeekdays[] = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
  const uint32_t cycleDuration =
      CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS + CLOCK_TEMP_VIEW_MS + CLOCK_DAY_VIEW_MS;
  const uint32_t cycleOffset = (nowMs - g_clockModeCycleStartMs) % cycleDuration;

  if (cycleOffset < CLOCK_TIME_VIEW_MS) {
    int displayHour = now.hour();
    if (g_timeDisplayFormat == TIME_FORMAT_12H) {
      displayHour = displayHour % 12;
      if (displayHour == 0) {
        displayHour = 12;
      }
    }

    char buffer[6];
    snprintf(buffer, sizeof(buffer), "%02d:%02d", displayHour, now.minute());
    return String(buffer);
  }

  if (cycleOffset < (CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS)) {
    char buffer[6];
    snprintf(buffer, sizeof(buffer), "%02d-%02d", now.day(), now.month());
    return String(buffer);
  }

  if (cycleOffset < (CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS + CLOCK_TEMP_VIEW_MS)) {
    if (!g_hasSensorReading || isnan(g_lastTemperatureC)) {
      return String("TEMP");
    }

    char buffer[6];
    snprintf(buffer, sizeof(buffer), "%2.0fC", g_lastTemperatureC);
    return String(buffer);
  }

  return String(kWeekdays[now.dayOfTheWeek()]);
}

void showSyncNeeded() {
  constexpr char kPlaceholder[] = "--:--";
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == kPlaceholder) {
    return;
  }

  g_lastDisplayPlaceholder = true;
  g_lastDisplayedStatusText = kPlaceholder;
  g_matrix.displayClear();
  g_matrix.displayText(kPlaceholder, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.println("Display status: --:-- (waiting for phone sync)");
}

void updateDisplayTime() {
  if (g_currentMode != DISPLAY_MODE_CLOCK || g_menuActive || g_clockToolsMenuActive ||
      g_activeClockTool != CLOCK_TOOL_TIME) {
    return;
  }

  if (g_timeNotSet) {
    showSyncNeeded();
    return;
  }

  const DateTime now = getSystemDateTime();
  renderDisplayText(buildClockModeDisplayText(now, millis()));
}

void initDisplay() {
  g_matrix.begin();
  applyDisplayBrightness(DISPLAY_DEFAULT_INTENSITY);
  g_matrix.displayClear();
  g_matrix.displayText("BOOT", PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.println("Matrix display initialized");
}

bool saveTimeToRtc(const DateTime &value) {
  if (!g_rtcAvailable) {
    return false;
  }

  g_rtc.adjust(value);
  Serial.printf("RTC updated: %04d-%02d-%02d %02d:%02d:%02d\n",
                value.year(),
                value.month(),
                value.day(),
                value.hour(),
                value.minute(),
                value.second());
  return true;
}

bool loadTimeFromRtc() {
  if (!g_rtcAvailable) {
    return false;
  }

  const DateTime rtcNow = g_rtc.now();
  if (!isDateTimeValid(rtcNow)) {
    Serial.println("RTC time invalid");
    return false;
  }

  setSystemClock(rtcNow);
  g_timeNotSet = false;
  g_timeSourceState = TIME_SOURCE_RTC;
  Serial.printf("Time loaded from RTC: %04d-%02d-%02d %02d:%02d:%02d\n",
                rtcNow.year(),
                rtcNow.month(),
                rtcNow.day(),
                rtcNow.hour(),
                rtcNow.minute(),
                rtcNow.second());
  return true;
}

bool initRtc() {
  Wire.begin();
  g_rtcAvailable = g_rtc.begin();
  if (!g_rtcAvailable) {
    Serial.println("RTC init failed");
    return false;
  }

  return true;
}

bool parsePhoneSyncValue(const String &value, DateTime &parsedValue) {
  int hour = 0;
  int minute = 0;
  int second = 0;
  int day = 0;
  int month = 0;
  int year = 0;

  if (sscanf(value.c_str(), "%d:%d:%d,%d-%d-%d", &hour, &minute, &second, &day, &month, &year) != 6) {
    return false;
  }

  parsedValue = DateTime(year, month, day, hour, minute, second);
  return isDateTimeValid(parsedValue);
}

bool applyPhoneSync(const String &syncValue) {
  DateTime parsedValue(2000, 1, 1, 0, 0, 0);
  if (!parsePhoneSyncValue(syncValue, parsedValue)) {
    notifyTx("STATUS:TIME_SYNC_INVALID");
    return false;
  }

  setSystemClock(parsedValue);
  saveTimeToRtc(parsedValue);
  g_timeNotSet = false;
  g_timeSourceState = TIME_SOURCE_PHONE_SYNC;
  g_lastRtcWriteBackMs = millis();
  updateDisplayTime();
  notifyTx("STATUS:TIME_SYNCED");
  notifyTx(buildSystemInfoMessage());
  return true;
}

void serviceBuzzer() {
  const bool alertActive = g_alarmRinging || g_timerFinished;
  if (!alertActive) {
    if (g_buttonClickUntilMs > millis()) {
      if (!g_buzzerOutputOn) {
        writeBuzzer(true);
      }
      return;
    }

    if (g_buzzerOutputOn) {
      writeBuzzer(false);
    }
    return;
  }

  const uint32_t nowMs = millis();
  if ((nowMs - g_lastBuzzerToggleMs) >= BUZZER_BEEP_INTERVAL_MS) {
    g_lastBuzzerToggleMs = nowMs;
    writeBuzzer(!g_buzzerOutputOn);
  }
}

void serviceClockTools() {
  const uint32_t nowMs = millis();

  if (g_activeClockTool == CLOCK_TOOL_TIMER && g_timerRunning && g_timerRemainingSeconds > 0) {
    if ((nowMs - g_timerLastTickMs) >= 1000) {
      const uint32_t elapsedSeconds = (nowMs - g_timerLastTickMs) / 1000;
      g_timerLastTickMs += elapsedSeconds * 1000;
      g_timerRemainingSeconds = max<int32_t>(g_timerRemainingSeconds - static_cast<int32_t>(elapsedSeconds), 0);
      if (g_timerRemainingSeconds == 0) {
        g_timerRunning = false;
        g_timerFinished = true;
      }
      updateClockToolDisplay();
    }
  }

  if (g_activeClockTool == CLOCK_TOOL_STOPWATCH && g_stopwatchRunning) {
    g_stopwatchElapsedMs += nowMs - g_stopwatchLastTickMs;
    g_stopwatchLastTickMs = nowMs;
    updateClockToolDisplay();
  }

  if (g_activeClockTool == CLOCK_TOOL_ALARM && g_alarmEnabled && !g_timeNotSet) {
    const DateTime now = getSystemDateTime();
    const uint32_t currentMinuteEpoch =
        static_cast<uint32_t>(now.unixtime() - static_cast<uint32_t>(now.second()));
    if (now.hour() == g_alarmHour && now.minute() == g_alarmMinute &&
        g_lastAlarmTriggerMinuteEpoch != currentMinuteEpoch) {
      g_alarmRinging = true;
      g_lastAlarmTriggerMinuteEpoch = currentMinuteEpoch;
      updateClockToolDisplay();
      notifyTx("STATUS:ALARM_TRIGGERED");
    }
  }
}

void serviceDisplayClock() {
  const uint32_t now = millis();
  if ((now - g_lastDisplayUpdateMs) < DISPLAY_UPDATE_INTERVAL_MS) {
    return;
  }

  g_lastDisplayUpdateMs = now;
  updateDisplayTime();
}

void serviceRtcWriteBack() {
  if (!g_rtcAvailable || g_timeNotSet || g_otaInProgress) {
    return;
  }

  const uint32_t now = millis();
  if ((now - g_lastRtcWriteBackMs) < RTC_WRITEBACK_INTERVAL_MS) {
    return;
  }

  g_lastRtcWriteBackMs = now;
  saveTimeToRtc(getSystemDateTime());
}
uint8_t clampBatteryPercent(int value) {
  return static_cast<uint8_t>(constrain(value, 0, 100));
}

bool readBatteryPercent(uint8_t &batteryPercent) {
  if (BATTERY_SENSE_PIN < 0) {
    return false;
  }

  const int raw = analogRead(BATTERY_SENSE_PIN);
  const int percent = map(raw, BATTERY_ADC_MIN, BATTERY_ADC_MAX, 0, 100);
  batteryPercent = clampBatteryPercent(percent);
  return true;
}

String buildSystemInfoMessage() {
  String message = "INFO:NAME:";
  message += DEVICE_NAME;
  message += ",MODEL:";
  message += MODEL_NUMBER;
  message += ",FW:";
  message += FIRMWARE_REVISION;
  message += ",MODE:";
  message += displayModeToString(g_currentMode);
  message += ",CTOOL:";
  message += clockToolToString(g_activeClockTool);
  message += ",MANIM:";
  message += messageAnimationToString(g_messageAnimationStyle);
  message += ",VIZ:";
  message += visualizerSourceToString(g_visualizerSource);
  message += ",VSTYLE:";
  message += visualizerStyleToString(g_visualizerStyle);
  message += ",BR:";
  message += String(g_displayBrightness);
  message += ",TF:";
  message += timeFormatToString(g_timeDisplayFormat);
  message += ",TIME:";
  message += g_timeNotSet ? "INVALID" : "VALID";
  message += ",SRC:";
  message += timeSourceToString(g_timeSourceState);
  message += ",BAT:";
  message += g_batteryAvailable ? String(g_batteryLevel) : "N/A";
  if (g_hasSensorReading) {
    message += ",T:";
    message += String(g_lastTemperatureC, 1);
    message += ",H:";
    message += String(g_lastHumidity, 1);
  }
  return message;
}

String buildVersionMessage() {
  String message = "VERSION:";
  message += FIRMWARE_REVISION;
  return message;
}

String buildSensorMessage() {
  String message = "SENSOR:";
  if (!g_hasSensorReading) {
    message += "UNAVAILABLE";
    return message;
  }

  message += "T:";
  message += String(g_lastTemperatureC, 1);
  message += ",H:";
  message += String(g_lastHumidity, 1);
  return message;
}

bool parseHexByte(char highNibble, char lowNibble, uint8_t &result) {
  auto hexValue = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
    if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
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
  if (OTA_SHARED_SECRET_STR[0] == '\0') {
    return true;
  }
  return token == OTA_SHARED_SECRET_STR;
}

void notifyTx(const String &message) {
  if (g_txCharacteristic == nullptr) {
    return;
  }

  g_txCharacteristic->setValue(message.c_str());
  if (g_isConnected) {
    g_txCharacteristic->notify();
  }
}

void sendOtaStatus(const String &status) {
  Serial.print("OTA status: ");
  Serial.println(status);
  notifyTx("OTA:" + status);
}

void abortOta(const String &reason) {
  if (g_otaInProgress) {
    Update.abort();
  }

  g_otaInProgress = false;
  g_otaHasExpectedMd5 = false;
  g_otaRebootPending = false;
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

  Serial.printf("OTA begin requested: size=%u, md5=%s, auth=%s\n",
                static_cast<unsigned>(expectedSize),
                md5Field.isEmpty() ? "none" : md5Field.c_str(),
                tokenField.isEmpty() ? "none" : "provided");

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
  Serial.printf("OTA chunk written: %u/%u bytes\n",
                static_cast<unsigned>(g_otaReceivedSize),
                static_cast<unsigned>(g_otaExpectedSize));
  sendOtaStatus("ACK," + String(g_otaReceivedSize));
  return true;
}

bool finishOta() {
  if (!g_otaInProgress) {
    sendOtaStatus("ERROR,no_session");
    return false;
  }

  Serial.printf("OTA finish requested: received=%u expected=%u\n",
                static_cast<unsigned>(g_otaReceivedSize),
                static_cast<unsigned>(g_otaExpectedSize));

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
  g_otaRebootPending = true;
  g_otaRebootAtMs = millis() + OTA_REBOOT_DELAY_MS;
  Serial.printf("OTA complete, reboot scheduled in %u ms\n",
                static_cast<unsigned>(OTA_REBOOT_DELAY_MS));
  sendOtaStatus("DONE," + String(FIRMWARE_REVISION));
  return true;
}

void updateBatteryCharacteristic(bool forceNotify) {
  if (g_batteryCharacteristic == nullptr) {
    return;
  }

  uint8_t newLevel = 0;
  g_batteryAvailable = readBatteryPercent(newLevel);
  if (!g_batteryAvailable) {
    return;
  }

  const bool changed = newLevel != g_batteryLevel;
  g_batteryLevel = newLevel;

  g_batteryCharacteristic->setValue(&g_batteryLevel, 1);
  if (g_isConnected && (forceNotify || changed)) {
    g_batteryCharacteristic->notify();
  }

  Serial.printf("Battery level: %u%%\n", static_cast<unsigned>(g_batteryLevel));
}

bool updateSensorReading() {
  if (g_otaInProgress) {
    return false;
  }

  const uint32_t now = millis();
  if ((now - g_lastSensorReadAttemptMs) < SENSOR_MIN_READ_INTERVAL_MS) {
    return false;
  }

  g_lastSensorReadAttemptMs = now;
  const float humidity = g_dht.readHumidity();
  const float temperatureC = g_dht.readTemperature();
  if (isnan(humidity) || isnan(temperatureC)) {
    Serial.println("DHT22 read failed");
    return false;
  }

  g_lastTemperatureC = temperatureC;
  g_lastHumidity = humidity;
  g_hasSensorReading = true;
  Serial.printf("Sensor reading: T=%.1fC H=%.1f%%\n", g_lastTemperatureC, g_lastHumidity);
  return true;
}

void serviceDisplayModes() {
  if (g_menuActive) {
    return;
  }

  if (g_clockToolsMenuActive) {
    g_matrix.displayAnimate();
    return;
  }

  switch (g_currentMode) {
    case DISPLAY_MODE_MESSAGE:
      if (g_messageModeDirty) {
        startMessageDisplay();
      }
      g_matrix.displayAnimate();
      break;
    case DISPLAY_MODE_VISUALIZER: {
      const uint32_t now = millis();
      if ((now - g_lastVisualizerFrameMs) >= VISUALIZER_FRAME_INTERVAL_MS) {
        g_lastVisualizerFrameMs = now;
        if (g_visualizerSource == VISUALIZER_SOURCE_DEVICE) {
          renderDeviceVisualizerFrame();
        } else if ((now - g_lastPhoneVisualizerUpdateMs) < 1000) {
          renderPhoneVisualizerFrame();
        }
      }
      break;
    }
    case DISPLAY_MODE_CLOCK:
    default:
      break;
  }
}

class SecurityCallbacks : public BLESecurityCallbacks {
 public:
  uint32_t onPassKeyRequest() override {
    return BLE_STATIC_PASSKEY;
  }

  void onPassKeyNotify(uint32_t passKey) override {
    Serial.printf("BLE passkey: %06lu\n", static_cast<unsigned long>(passKey));
  }

  bool onSecurityRequest() override {
    Serial.println("BLE security request received");
    return true;
  }

  bool onConfirmPIN(uint32_t passKey) override {
    Serial.printf("Confirming BLE passkey: %06lu\n", static_cast<unsigned long>(passKey));
    return true;
  }

#if defined(CONFIG_BLUEDROID_ENABLED)
  void onAuthenticationComplete(esp_ble_auth_cmpl_t authResult) override {
    if (authResult.success) {
      Serial.println("BLE pairing/bonding complete");
      notifyTx("STATUS:SECURE");
      notifyTx(buildSystemInfoMessage());
    } else {
      Serial.printf("BLE pairing failed, reason: %d\n", authResult.fail_reason);
      notifyTx("STATUS:PAIRING_FAILED");
    }
  }
#endif

#if defined(CONFIG_NIMBLE_ENABLED)
  void onAuthenticationComplete(ble_gap_conn_desc *desc) override {
    if (desc->sec_state.encrypted) {
      Serial.println("BLE pairing/bonding complete");
      notifyTx("STATUS:SECURE");
      notifyTx(buildSystemInfoMessage());
    } else {
      Serial.println("BLE pairing failed");
      notifyTx("STATUS:PAIRING_FAILED");
    }
  }
#endif
};

class ServerCallbacks : public BLEServerCallbacks {
 public:
  void onConnect(BLEServer *server) override {
    (void)server;
    g_isConnected = true;
    g_restartAdvertisingPending = false;
    g_lastAdvertisingKickMs = millis();
    Serial.println("BLE client connected");
    updateSensorReading();
    updateBatteryCharacteristic(true);
    notifyTx("STATUS:CONNECTED");
    notifyTx(buildSystemInfoMessage());
    notifyTx(buildSensorMessage());
  }

  void onDisconnect(BLEServer *server) override {
    (void)server;
    g_isConnected = false;
    g_restartAdvertisingPending = true;
    g_disconnectTimestampMs = millis();
    g_lastAdvertisingKickMs = g_disconnectTimestampMs;
    Serial.println("BLE client disconnected");
    if (!g_timeNotSet && g_rtcAvailable) {
      saveTimeToRtc(getSystemDateTime());
      g_timeSourceState = TIME_SOURCE_RTC;
      Serial.println("Offline mode active: RTC remains primary time source");
    }
    startAdvertisingNow("disconnect restart", true);
    if (g_otaInProgress) {
      abortOta("disconnected");
    }
  }
};

class RxCallbacks : public BLECharacteristicCallbacks {
 public:
  void onWrite(BLECharacteristic *characteristic) override {
    const String value = characteristic->getValue();
    if (value.isEmpty()) {
      Serial.println("RX write received with empty payload");
      return;
    }

    Serial.print("RX: ");
    Serial.println(value);

    if (value.startsWith("BAT:")) {
      const int requestedLevel = value.substring(4).toInt();
      if (!g_batteryAvailable) {
        notifyTx("BATTERY:UNAVAILABLE");
      } else {
        g_batteryLevel = clampBatteryPercent(requestedLevel);
        updateBatteryCharacteristic(true);
        notifyTx("BATTERY:UPDATED");
      }
      return;
    }

    if (value.startsWith("MSG:")) {
      g_appMessage = value.substring(4);
      g_messageModeDirty = true;
      notifyTx("STATUS:MESSAGE_UPDATED");
      if (g_currentMode == DISPLAY_MODE_MESSAGE && !g_menuActive) {
        startMessageDisplay();
      }
      return;
    }

    if (value.startsWith("CTOOL:")) {
      const String toolValue = value.substring(6);
      if (toolValue.equalsIgnoreCase("TIME") || toolValue.equalsIgnoreCase("CLOCK")) {
        activateClockTool(CLOCK_TOOL_TIME);
      } else if (toolValue.equalsIgnoreCase("ALARM")) {
        activateClockTool(CLOCK_TOOL_ALARM);
      } else if (toolValue.equalsIgnoreCase("TIMER")) {
        activateClockTool(CLOCK_TOOL_TIMER);
      } else if (toolValue.equalsIgnoreCase("STOPWATCH")) {
        activateClockTool(CLOCK_TOOL_STOPWATCH);
      } else {
        notifyTx("STATUS:CTOOL_INVALID");
        return;
      }
      notifyTx("CTOOL:" + String(clockToolToString(g_activeClockTool)));
      return;
    }

    if (value.startsWith("ALARM_SET:")) {
      int hour = -1;
      int minute = -1;
      if (sscanf(value.substring(10).c_str(), "%d:%d", &hour, &minute) != 2 ||
          hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        notifyTx("STATUS:ALARM_INVALID");
        return;
      }
      g_alarmHour = hour;
      g_alarmMinute = minute;
      g_alarmEditMode = false;
      if (g_currentMode == DISPLAY_MODE_CLOCK) {
        activateClockTool(CLOCK_TOOL_TIME);
      }
      notifyTx("STATUS:ALARM_UPDATED");
      notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_ON")) {
      g_alarmEnabled = true;
      if (g_currentMode == DISPLAY_MODE_CLOCK) {
        activateClockTool(CLOCK_TOOL_TIME);
      }
      notifyTx("STATUS:ALARM_UPDATED");
      notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_OFF")) {
      g_alarmEnabled = false;
      g_alarmRinging = false;
      if (g_currentMode == DISPLAY_MODE_CLOCK) {
        activateClockTool(CLOCK_TOOL_TIME);
      }
      notifyTx("STATUS:ALARM_UPDATED");
      notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_CLEAR")) {
      g_alarmEnabled = false;
      g_alarmRinging = false;
      g_alarmHour = 0;
      g_alarmMinute = 0;
      if (g_currentMode == DISPLAY_MODE_CLOCK) {
        activateClockTool(CLOCK_TOOL_TIME);
      }
      notifyTx("STATUS:ALARM_UPDATED");
      notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.startsWith("TIMER_SET:")) {
      int minutes = -1;
      int seconds = -1;
      if (sscanf(value.substring(10).c_str(), "%d:%d", &minutes, &seconds) != 2 ||
          minutes < 0 || minutes > 99 || seconds < 0 || seconds > 59) {
        notifyTx("STATUS:TIMER_INVALID");
        return;
      }
      g_timerTotalSeconds = minutes * 60 + seconds;
      g_timerRemainingSeconds = g_timerTotalSeconds;
      g_timerRunning = false;
      g_timerFinished = false;
      g_timerEditMode = false;
      g_timerConfigured = g_timerTotalSeconds > 0;
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED");
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.startsWith("TIMER_ADD:")) {
      const int addSeconds = value.substring(10).toInt();
      if (addSeconds <= 0) {
        notifyTx("STATUS:TIMER_INVALID");
        return;
      }
      g_timerTotalSeconds = min<int32_t>(g_timerTotalSeconds + addSeconds, 99 * 60 + 59);
      g_timerRemainingSeconds = min<int32_t>(g_timerRemainingSeconds + addSeconds, 99 * 60 + 59);
      g_timerConfigured = g_timerTotalSeconds > 0;
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED");
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_START")) {
      if (g_timerRemainingSeconds <= 0) {
        notifyTx("STATUS:TIMER_INVALID");
        return;
      }
      g_timerRunning = true;
      g_timerFinished = false;
      g_timerLastTickMs = millis();
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED");
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_PAUSE")) {
      g_timerRunning = false;
      notifyTx("STATUS:TIMER_UPDATED");
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_RESET")) {
      g_timerRunning = false;
      g_timerFinished = false;
      g_timerRemainingSeconds = g_timerTotalSeconds;
      notifyTx("STATUS:TIMER_UPDATED");
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_START")) {
      g_stopwatchRunning = true;
      g_stopwatchLastTickMs = millis();
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED");
      notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_PAUSE")) {
      g_stopwatchRunning = false;
      notifyTx("STATUS:SW_UPDATED");
      notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_RESET")) {
      g_stopwatchRunning = false;
      g_stopwatchElapsedMs = 0;
      g_stopwatchLapMs = 0;
      g_stopwatchLapFrozen = false;
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED");
      notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_LAP")) {
      if (!g_stopwatchRunning && g_stopwatchElapsedMs == 0) {
        notifyTx("STATUS:SW_INVALID");
        return;
      }
      g_stopwatchLapMs = g_stopwatchElapsedMs;
      g_stopwatchLapFrozen = !g_stopwatchLapFrozen;
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED");
      notifyTx("SW:LAP," + buildStopwatchStateMessage().substring(3));
      return;
    }

    if (value.startsWith("MSGANIM:")) {
      const String animationValue = value.substring(8);
      if (animationValue.equalsIgnoreCase("NONE")) {
        g_messageAnimationStyle = MESSAGE_ANIM_NONE;
      } else if (animationValue.equalsIgnoreCase("WAVE")) {
        g_messageAnimationStyle = MESSAGE_ANIM_WAVE;
      } else if (animationValue.equalsIgnoreCase("SCROLL")) {
        g_messageAnimationStyle = MESSAGE_ANIM_SCROLL;
      } else if (animationValue.equalsIgnoreCase("RAIN")) {
        g_messageAnimationStyle = MESSAGE_ANIM_RAIN;
      } else {
        notifyTx("STATUS:MSGANIM_INVALID");
        return;
      }

      g_messageModeDirty = true;
      if (g_currentMode == DISPLAY_MODE_MESSAGE && !g_menuActive) {
        startMessageDisplay();
      }
      notifyTx("STATUS:MSGANIM_UPDATED");
      notifyTx("MSGANIM:" + String(messageAnimationToString(g_messageAnimationStyle)));
      return;
    }

    if (value.startsWith("VIZSRC:")) {
      const String sourceValue = value.substring(7);
      if (sourceValue.equalsIgnoreCase("DEVICE")) {
        g_visualizerSource = VISUALIZER_SOURCE_DEVICE;
      } else if (sourceValue.equalsIgnoreCase("PHONE")) {
        g_visualizerSource = VISUALIZER_SOURCE_PHONE;
      } else {
        notifyTx("STATUS:VIZSRC_INVALID");
        return;
      }

      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive) {
        applyDisplayMode(DISPLAY_MODE_VISUALIZER);
      }
      notifyTx("STATUS:VIZSRC_UPDATED");
      notifyTx("VIZSRC:" + String(visualizerSourceToString(g_visualizerSource)));
      return;
    }

    if (value.startsWith("VIZSTYLE:")) {
      const String styleValue = value.substring(9);
      if (styleValue.equalsIgnoreCase("BARS")) {
        g_visualizerStyle = VISUALIZER_STYLE_BARS;
      } else if (styleValue.equalsIgnoreCase("WAVE")) {
        g_visualizerStyle = VISUALIZER_STYLE_WAVE;
      } else if (styleValue.equalsIgnoreCase("RADIAL")) {
        g_visualizerStyle = VISUALIZER_STYLE_RADIAL;
      } else {
        notifyTx("STATUS:VIZSTYLE_INVALID");
        return;
      }

      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive) {
        if (g_visualizerSource == VISUALIZER_SOURCE_PHONE) {
          renderPhoneVisualizerFrame();
        } else {
          renderDeviceVisualizerFrame();
        }
      }
      notifyTx("STATUS:VIZSTYLE_UPDATED");
      notifyTx("VIZSTYLE:" + String(visualizerStyleToString(g_visualizerStyle)));
      return;
    }

    if (value.startsWith("VIZFRAME:")) {
      if (!parsePhoneVisualizerFrame(value.substring(9))) {
        notifyTx("STATUS:VIZFRAME_INVALID");
        return;
      }

      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive &&
          g_visualizerSource == VISUALIZER_SOURCE_PHONE) {
        renderPhoneVisualizerFrame();
      }
      notifyTx("STATUS:VIZFRAME_UPDATED");
      return;
    }

    if (value.startsWith("BRIGHTNESS:")) {
      const int requestedBrightness = value.substring(11).toInt();
      const bool inRange = requestedBrightness >= 0 && requestedBrightness <= 15;
      if (!inRange) {
        notifyTx("STATUS:BRIGHTNESS_INVALID");
        return;
      }

      applyDisplayBrightness(static_cast<uint8_t>(requestedBrightness));
      notifyTx("STATUS:BRIGHTNESS_UPDATED");
      return;
    }

    if (value.startsWith("TIMEFMT:")) {
      const String formatValue = value.substring(8);
      if (formatValue == "12") {
        g_timeDisplayFormat = TIME_FORMAT_12H;
      } else if (formatValue == "24") {
        g_timeDisplayFormat = TIME_FORMAT_24H;
      } else {
        notifyTx("STATUS:TIME_FORMAT_INVALID");
        return;
      }

      if (g_currentMode == DISPLAY_MODE_CLOCK && !g_menuActive) {
        g_lastDisplayedTimeText = "";
        updateDisplayTime();
      }
      notifyTx("STATUS:TIME_FORMAT_UPDATED");
      notifyTx("TIMEFMT:" + String(timeFormatToString(g_timeDisplayFormat)));
      return;
    }

    if (value.startsWith("BTN:")) {
      handleVirtualButton(value.substring(4));
      return;
    }

    if (value.equalsIgnoreCase("OTA_END")) {
      Serial.println("RX command: OTA_END");
      finishOta();
      return;
    }

    if (value.equalsIgnoreCase("OTA_ABORT")) {
      Serial.println("RX command: OTA_ABORT");
      abortOta("aborted");
      return;
    }

    if (value.equalsIgnoreCase("OTA_STATUS")) {
      if (g_otaInProgress) {
        sendOtaStatus("IN_PROGRESS," + String(g_otaReceivedSize) + "/" + String(g_otaExpectedSize));
      } else {
        sendOtaStatus("IDLE");
      }
      return;
    }

    if (value.equalsIgnoreCase("INFO")) {
      notifyTx(buildSystemInfoMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIME?")) {
      if (g_timeNotSet) {
        notifyTx("TIME:INVALID");
      } else {
        const DateTime now = getSystemDateTime();
        char buffer[32];
        snprintf(buffer,
                 sizeof(buffer),
                 "TIME:%02d:%02d:%02d,%02d-%02d-%04d,SRC:%s",
                 now.hour(),
                 now.minute(),
                 now.second(),
                 now.day(),
                 now.month(),
                 now.year(),
                 timeSourceToString(g_timeSourceState));
        notifyTx(String(buffer));
      }
      return;
    }

    if (value.equalsIgnoreCase("MODE?")) {
      notifyTx("MODE:" + String(displayModeToString(g_currentMode)));
      return;
    }

    if (value.equalsIgnoreCase("CTOOL?")) {
      notifyTx("CTOOL:" + String(clockToolToString(g_activeClockTool)));
      return;
    }

    if (value.equalsIgnoreCase("ALARM?")) {
      notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER?")) {
      notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW?")) {
      notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("MSGANIM?")) {
      notifyTx("MSGANIM:" + String(messageAnimationToString(g_messageAnimationStyle)));
      return;
    }

    if (value.equalsIgnoreCase("VIZSRC?")) {
      notifyTx("VIZSRC:" + String(visualizerSourceToString(g_visualizerSource)));
      return;
    }

    if (value.equalsIgnoreCase("VIZSTYLE?")) {
      notifyTx("VIZSTYLE:" + String(visualizerStyleToString(g_visualizerStyle)));
      return;
    }

    if (value.equalsIgnoreCase("BRIGHTNESS?")) {
      notifyTx("BRIGHTNESS:" + String(g_displayBrightness));
      return;
    }

    if (value.equalsIgnoreCase("TIMEFMT?")) {
      notifyTx("TIMEFMT:" + String(timeFormatToString(g_timeDisplayFormat)));
      return;
    }

    if (value.equalsIgnoreCase("VERSION?")) {
      notifyTx(buildVersionMessage());
      return;
    }

    if (value.equalsIgnoreCase("BAT?")) {
      notifyTx(g_batteryAvailable ? "BATTERY:" + String(g_batteryLevel) : "BATTERY:UNAVAILABLE");
      return;
    }

    if (value.equalsIgnoreCase("SENSOR?")) {
      updateSensorReading();
      notifyTx(buildSensorMessage());
      return;
    }

    if (value.startsWith("OTA_BEGIN:")) {
      Serial.println("RX command: OTA_BEGIN");
      beginOta(value.substring(10));
      return;
    }

    if (value.startsWith("SYNC:")) {
      Serial.println("RX command: SYNC");
      applyPhoneSync(value.substring(5));
      return;
    }

    if (value.startsWith("OTA_CHUNK:")) {
      Serial.printf("RX command: OTA_CHUNK len=%u\n",
                    static_cast<unsigned>(value.length() - 10));
      writeOtaChunk(value.substring(10));
      return;
    }

    notifyTx("ECHO:" + value);
  }
};

void createBatteryService() {
  BLEService *batteryService = g_server->createService(BATTERY_SERVICE_UUID);
  g_batteryCharacteristic = batteryService->createCharacteristic(
      BATTERY_LEVEL_UUID,
      BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);
  g_batteryCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ_ENCRYPTED);
  g_batteryCharacteristic->addDescriptor(new BLE2902());
  g_batteryCharacteristic->setValue(&g_batteryLevel, 1);
  batteryService->start();
}

void createDeviceInformationService() {
  BLEService *deviceInfoService = g_server->createService(DEVICE_INFO_SERVICE_UUID);

  BLECharacteristic *manufacturerCharacteristic =
      deviceInfoService->createCharacteristic(MANUFACTURER_NAME_UUID, BLECharacteristic::PROPERTY_READ);
  manufacturerCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ);
  manufacturerCharacteristic->setValue(MANUFACTURER_NAME);

  BLECharacteristic *modelCharacteristic =
      deviceInfoService->createCharacteristic(MODEL_NUMBER_UUID, BLECharacteristic::PROPERTY_READ);
  modelCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ);
  modelCharacteristic->setValue(MODEL_NUMBER);

  BLECharacteristic *firmwareCharacteristic =
      deviceInfoService->createCharacteristic(FIRMWARE_REVISION_UUID, BLECharacteristic::PROPERTY_READ);
  firmwareCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ);
  firmwareCharacteristic->setValue(FIRMWARE_REVISION);

  deviceInfoService->start();
}

void createCustomService() {
  BLEService *customService = g_server->createService(CUSTOM_SERVICE_UUID);

  BLECharacteristic *rxCharacteristic = customService->createCharacteristic(
      CUSTOM_RX_UUID,
      BLECharacteristic::PROPERTY_WRITE | BLECharacteristic::PROPERTY_WRITE_NR);
  rxCharacteristic->setAccessPermissions(ESP_GATT_PERM_WRITE_ENCRYPTED);
  rxCharacteristic->setCallbacks(new RxCallbacks());

  g_txCharacteristic = customService->createCharacteristic(
      CUSTOM_TX_UUID,
      BLECharacteristic::PROPERTY_NOTIFY | BLECharacteristic::PROPERTY_READ);
  g_txCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ_ENCRYPTED);
  g_txCharacteristic->addDescriptor(new BLE2902());
  g_txCharacteristic->setValue("STATUS:READY");

  customService->start();
}

void startAdvertisingNow(const char *reason, bool forceRestart) {
  if (forceRestart) {
    BLEDevice::stopAdvertising();
    delay(40);
  }

  BLEDevice::startAdvertising();
  g_lastAdvertisingKickMs = millis();
  Serial.print("BLE advertising ");
  Serial.println(reason);
}

void configureAdvertising() {
  BLEAdvertising *advertising = BLEDevice::getAdvertising();
  BLEDevice::setPower(ESP_PWR_LVL_P9);
  advertising->setAppearance(DEVICE_APPEARANCE);
  advertising->setScanResponse(true);
  advertising->setMinPreferred(0x06);
  advertising->setMaxPreferred(0x12);
  advertising->setMinInterval(160);
  advertising->setMaxInterval(320);
  advertising->addServiceUUID(BATTERY_SERVICE_UUID);
  advertising->addServiceUUID(CUSTOM_SERVICE_UUID);
  advertising->addServiceUUID(DEVICE_INFO_SERVICE_UUID);
  startAdvertisingNow("started", false);
}

void initBle() {
  BLEDevice::init(DEVICE_NAME);
  BLEDevice::setMTU(517);
  BLEDevice::setSecurityCallbacks(new SecurityCallbacks());

  BLESecurity *security = new BLESecurity();
  security->setAuthenticationMode(true, true, true);
  security->setCapability(ESP_IO_CAP_OUT);
  security->setPassKey(true, BLE_STATIC_PASSKEY);
  security->setInitEncryptionKey(ESP_BLE_ENC_KEY_MASK | ESP_BLE_ID_KEY_MASK);
  security->setRespEncryptionKey(ESP_BLE_ENC_KEY_MASK | ESP_BLE_ID_KEY_MASK);

  g_server = BLEDevice::createServer();
  g_server->setCallbacks(new ServerCallbacks());
  g_server->advertiseOnDisconnect(false);

  createBatteryService();
  createDeviceInformationService();
  createCustomService();
  configureAdvertising();
}

void restartAdvertisingIfNeeded() {
  if (!g_restartAdvertisingPending) {
    return;
  }

  if ((millis() - g_disconnectTimestampMs) < RESTART_ADV_DELAY_MS) {
    return;
  }

  startAdvertisingNow("restarted", true);
  g_restartAdvertisingPending = false;
}

void serviceAdvertisingHealth() {
  if (g_isConnected) {
    return;
  }

  const uint32_t now = millis();
  if ((now - g_bootTimestampMs) < BOOT_ADVERTISING_STABILIZE_MS) {
    return;
  }

  if ((now - g_lastAdvertisingKickMs) < ADVERTISING_REFRESH_INTERVAL_MS) {
    return;
  }

  startAdvertisingNow("refreshed", true);
}

void serviceBatteryUpdates() {
  if (g_otaInProgress) {
    return;
  }

  const uint32_t now = millis();
  if ((now - g_lastBatteryUpdateMs) < BATTERY_UPDATE_INTERVAL_MS) {
    return;
  }

  g_lastBatteryUpdateMs = now;
  updateBatteryCharacteristic(false);
}

void serviceSensorUpdates() {
  if (g_otaInProgress) {
    return;
  }

  const uint32_t now = millis();
  if ((now - g_lastSensorUpdateMs) < SENSOR_UPDATE_INTERVAL_MS) {
    return;
  }

  g_lastSensorUpdateMs = now;
  if (updateSensorReading()) {
    notifyTx(buildSensorMessage());
  }
}

void serviceOtaState() {
  if (g_otaInProgress && (millis() - g_otaLastActivityMs) > OTA_TIMEOUT_MS) {
    abortOta("timeout");
  }

  if (g_otaRebootPending && millis() >= g_otaRebootAtMs) {
    Serial.println("Rebooting into updated firmware");
    ESP.restart();
  }
}

}  // namespace

void setup() {
  Serial.begin(SERIAL_BAUDRATE);
  delay(200);
  Serial.println();
  Serial.println("Booting DotMatrix Clock BLE firmware");
  Serial.printf("Reset reason: %d\n", static_cast<int>(esp_reset_reason()));
  g_bootTimestampMs = millis();

  if (BATTERY_SENSE_PIN >= 0) {
    analogReadResolution(12);
    pinMode(BATTERY_SENSE_PIN, INPUT);
  }

  analogReadResolution(12);
  pinMode(MIC_INPUT_PIN, INPUT);
  if (MIC_SENSITIVITY_PIN >= 0) {
    pinMode(MIC_SENSITIVITY_PIN, INPUT);
  }

  pinMode(BUZZER_PIN, OUTPUT);
  writeBuzzer(false);

  pinMode(BUTTON_MODE_PIN, INPUT_PULLUP);
  pinMode(BUTTON_NEXT_PIN, INPUT_PULLUP);
  pinMode(BUTTON_BACK_PIN, INPUT_PULLUP);
  pinMode(BUTTON_SELECT_PIN, INPUT_PULLUP);

  initDisplay();
  const bool rtcInitialized = initRtc();
  if (rtcInitialized && loadTimeFromRtc()) {
    Serial.println("Using RTC as primary time source");
  } else {
    g_timeNotSet = true;
    g_timeSourceState = TIME_INVALID;
    Serial.println("Valid RTC time unavailable; waiting for app sync");
    showSyncNeeded();
  }

  g_dht.begin();
  initBle();
  updateSensorReading();
  updateBatteryCharacteristic(false);
  applyDisplayMode(DISPLAY_MODE_CLOCK);
}

void loop() {
  serviceButtons();
  restartAdvertisingIfNeeded();
  serviceAdvertisingHealth();
  serviceOtaState();
  serviceBatteryUpdates();
  serviceSensorUpdates();
  serviceDisplayClock();
  serviceClockTools();
  serviceBuzzer();
  serviceDisplayModes();
  serviceRtcWriteBack();
  yield();
}
