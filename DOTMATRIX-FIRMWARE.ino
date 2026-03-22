// ============================================================
//  DotMatrix Clock — patched firmware v1.0.3
//  Fixes applied:
//   1.  Duplicate clampBatteryPercent / readBatteryPercent removed
//   2.  setAuthenticationMode correct API (ESP_LE_AUTH_REQ_SC_MITM_BOND)
//   3.  setPassKey correct API (single uint32_t)
//   4.  BAT: command reads ADC instead of accepting client value
//   5.  static_assert blocks release build with empty OTA secret
//   6.  Hardware watchdog (esp_task_wdt)
//   7.  BLE_STATIC_PASSKEY default comment added
//   8.  FFT sampling: TODO comment to move to FreeRTOS task
//   9.  Stopwatch display gated to 5 Hz (was every loop iteration)
//  10.  g_lastSensorSampleMs + SENSOR_STALE_THRESHOLD_MS added
//  11.  String heap fragmentation: snprintf + char[] in all hot paths
//  12.  Double advertising restart on disconnect removed
//  13.  SW_LAP fragile .substring(3) replaced with direct format
//  14.  Duplicate analogReadResolution(12) in setup removed
//  15.  Wire.begin uses named I2C pin constants
// ============================================================

#include <Arduino.h>
#include <Adafruit_VL53L0X.h>  // DextBot laser distance sensor
#include <BLE2902.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLESecurity.h>
#include <DHT.h>
#include <arduinoFFT.h>
#include <MD_MAX72xx.h>
#include <MD_Parola.h>
#include <Preferences.h>
#include <RTClib.h>
#include <Wire.h>
#include <esp_system.h>
#include <esp_task_wdt.h>
#include <sys/time.h>
#include <time.h>
#include <Update.h>

namespace {

constexpr char DEVICE_NAME[]     = "DotMatrix Clock";
constexpr uint16_t DEVICE_APPEARANCE = 0x0340;  // Clock

constexpr uint32_t RESTART_ADV_DELAY_MS              = 250;
constexpr uint32_t BOOT_ADVERTISING_STABILIZE_MS     = 2000;
constexpr uint32_t ADVERTISING_REFRESH_INTERVAL_MS   = 10000;
constexpr uint32_t BATTERY_UPDATE_INTERVAL_MS        = 10000;
constexpr uint32_t SENSOR_UPDATE_INTERVAL_MS         = 5000;
constexpr uint32_t SENSOR_MIN_READ_INTERVAL_MS       = 2200;
// FIX 10: readings older than this are treated as stale
constexpr uint32_t SENSOR_STALE_THRESHOLD_MS         = 30000;
constexpr uint32_t OTA_TIMEOUT_MS                    = 20000;
constexpr uint32_t OTA_REBOOT_DELAY_MS               = 1500;
constexpr uint32_t DISPLAY_UPDATE_INTERVAL_MS        = 1000;
constexpr uint32_t RTC_WRITEBACK_INTERVAL_MS         = 60000;
// NVS: persist time to flash so a power cut with a dead/missing RTC battery
// still recovers a usable (approximate) time on next boot.
constexpr uint32_t NVS_WRITEBACK_INTERVAL_MS         = 60000;
constexpr char     NVS_NAMESPACE[]                   = "dotmatrix";
constexpr char     NVS_KEY_EPOCH[]                   = "epoch";    // uint32 unix timestamp
constexpr char     NVS_KEY_SAVE_TS[]                 = "savets";  // unix timestamp of when NVS was written
// If NVS time is older than this on boot, treat as invalid and show --:--
// 12 hours = 43200 seconds. Device off longer than this → time too wrong to show.
constexpr uint32_t NVS_TIME_MAX_AGE_S                = 43200UL;
constexpr uint32_t CLOCK_TIME_VIEW_MS                = 5000;
constexpr uint32_t CLOCK_DATE_VIEW_MS                = 2000;
constexpr uint32_t CLOCK_TEMP_VIEW_MS                = 2000;
constexpr uint32_t CLOCK_DAY_VIEW_MS                 = 2000;
// Weather slides: condition icon (e.g. RAIN) then outside temperature
constexpr uint32_t CLOCK_WEATHER_COND_VIEW_MS        = 2000;
constexpr uint32_t CLOCK_WEATHER_TEMP_VIEW_MS        = 2000;
// Weather is considered stale after 30 minutes — hide from display
constexpr uint32_t WEATHER_STALE_MS                  = 1800000UL;
// NVS keys for weather persistence across reboots
constexpr char     NVS_KEY_WEATHER_TEMP[]            = "wtmp";   // float as int*10
constexpr char     NVS_KEY_WEATHER_FEELS[]           = "wfls";
constexpr char     NVS_KEY_WEATHER_HUM[]             = "whum";
constexpr char     NVS_KEY_WEATHER_COND[]            = "wcnd";   // short string
constexpr char     NVS_KEY_WEATHER_CITY[]            = "wcty";
constexpr char     NVS_KEY_WEATHER_TS[]              = "wts";    // epoch of last update
constexpr uint32_t SERIAL_BAUDRATE                   = 115200;
// FIX 7: change this to a random value unique to your device.
// It should be printed on the hardware label or derived from the MAC.
constexpr uint32_t BLE_STATIC_PASSKEY                = 493271;
constexpr int TIME_VALID_MIN_YEAR                    = 2024;
// FIX 6: watchdog timeout — loop must call esp_task_wdt_reset() within this window
constexpr uint32_t WDT_TIMEOUT_S                     = 30;

constexpr uint8_t DHT_PIN                = 4;
constexpr uint8_t DHT_TYPE               = DHT22;
constexpr uint8_t DISPLAY_HARDWARE_TYPE = MD_MAX72XX::FC16_HW;
constexpr uint8_t DISPLAY_MAX_DEVICES   = 4;
constexpr uint8_t DISPLAY_PIN_DIN       = 23;
constexpr uint8_t DISPLAY_PIN_CLK       = 18;
constexpr uint8_t DISPLAY_PIN_CS        = 5;
constexpr uint8_t DISPLAY_DEFAULT_INTENSITY = 2;
constexpr uint8_t BUTTON_MODE_PIN       = 32;
constexpr uint8_t BUTTON_NEXT_PIN       = 33;
constexpr uint8_t BUTTON_BACK_PIN       = 25;
constexpr uint8_t BUTTON_SELECT_PIN     = 26;
constexpr uint8_t BUZZER_PIN            = 27;
constexpr bool    BUZZER_ACTIVE_HIGH    = true;
// FIX 15: named I2C pin constants so Wire.begin is self-documenting
constexpr uint8_t I2C_SDA_PIN           = 21;  // ESP32 default SDA
constexpr uint8_t I2C_SCL_PIN           = 22;  // ESP32 default SCL

constexpr uint32_t BUTTON_DEBOUNCE_MS          = 180;
constexpr uint32_t BUTTON_CLICK_MS             = 35;
constexpr uint32_t VISUALIZER_FRAME_INTERVAL_MS= 180;
constexpr uint32_t BUZZER_BEEP_INTERVAL_MS     = 250;
// FIX 9: cap stopwatch display refresh to 5 Hz (200 ms)
constexpr uint32_t STOPWATCH_DISPLAY_INTERVAL_MS = 200;

// KY-037 microphone sensor settings
// Connect AO (analog out) to MIC_INPUT_PIN.
// The DO (digital out) pin is threshold-only and not used for FFT.
// The onboard potentiometer sets the DO threshold — it has no effect on AO.
constexpr int      MIC_INPUT_PIN          = 34;
constexpr int      MIC_SENSITIVITY_PIN    = -1;   // -1 = fixed gain; wire to ADC pin if you add a pot
constexpr uint16_t FFT_SAMPLE_COUNT       = 64;
constexpr double   FFT_SAMPLING_FREQUENCY = 4000.0;
// KY-037 AO sits at ~VCC/2 when silent (~2048 on a 12-bit 3.3 V ADC).
// This DC offset must be subtracted before FFT or bin 0 dominates everything.
constexpr int      MIC_DC_OFFSET          = 2048;  // adjust if your module reads differently at silence
// Scale factor applied after DC removal. Increase if bars are too short, decrease if clipping.
constexpr double   MIC_GAIN               = 8.0;
// FFT magnitude ceiling — bins above this are clamped to full scale (8).
// Tune upward if the display is always at maximum even for quiet sounds.
constexpr double   FFT_MAG_CEILING        = 300.0;

constexpr int BATTERY_SENSE_PIN = -1;
constexpr int BATTERY_ADC_MIN   = 1800;
constexpr int BATTERY_ADC_MAX   = 3200;

constexpr char CUSTOM_SERVICE_UUID[]      = "6b5f9001-3d10-4f76-8e22-4b0d6e6a1001";
constexpr char CUSTOM_RX_UUID[]           = "6b5f9002-3d10-4f76-8e22-4b0d6e6a1001";
constexpr char CUSTOM_TX_UUID[]           = "6b5f9003-3d10-4f76-8e22-4b0d6e6a1001";
constexpr char DEVICE_INFO_SERVICE_UUID[] = "180A";
constexpr char BATTERY_SERVICE_UUID[]     = "180F";
constexpr char BATTERY_LEVEL_UUID[]       = "2A19";
constexpr char MANUFACTURER_NAME_UUID[]   = "2A29";
constexpr char MODEL_NUMBER_UUID[]        = "2A24";
constexpr char FIRMWARE_REVISION_UUID[]   = "2A26";

constexpr char MANUFACTURER_NAME[] = "DotMatrix Labs";
constexpr char MODEL_NUMBER[]      = "DM-CLOCK-01";

#ifndef FIRMWARE_VERSION
#define FIRMWARE_VERSION "v1.0.3"
#endif
constexpr char FIRMWARE_REVISION[] = FIRMWARE_VERSION;

#ifndef OTA_SHARED_SECRET
#define OTA_SHARED_SECRET ""
#endif
constexpr char OTA_SHARED_SECRET_STR[] = OTA_SHARED_SECRET;

// FIX 5: fail the build in release mode if the shared secret is empty.
// Add -DRELEASE_BUILD and -DOTA_SHARED_SECRET='"your-token"' to platformio.ini.
#ifdef RELEASE_BUILD
static_assert(sizeof(OTA_SHARED_SECRET) > 1,
  "OTA_SHARED_SECRET must be set for release builds. "
  "Add -DOTA_SHARED_SECRET='\"your-token\"' to build_flags.");
#endif

constexpr size_t OTA_MAX_CHUNK_BYTES = 252;

DHT g_dht(DHT_PIN, DHT_TYPE);
RTC_DS1307 g_rtc;
Preferences g_prefs;   // NVS flash storage for time persistence
MD_Parola g_matrix(static_cast<MD_MAX72XX::moduleType_t>(DISPLAY_HARDWARE_TYPE),
                   DISPLAY_PIN_CS, DISPLAY_MAX_DEVICES);
ArduinoFFT<double> g_fft = ArduinoFFT<double>();
BLEServer         *g_server                = nullptr;
BLECharacteristic *g_batteryCharacteristic = nullptr;
BLECharacteristic *g_txCharacteristic      = nullptr;

enum TimeSourceState  { TIME_INVALID, TIME_SOURCE_RTC, TIME_SOURCE_PHONE_SYNC, TIME_SOURCE_NVS };
enum DisplayMode      { DISPLAY_MODE_CLOCK, DISPLAY_MODE_MESSAGE, DISPLAY_MODE_VISUALIZER, DISPLAY_MODE_SETTINGS, DISPLAY_MODE_DEXTBOT,
                        DISPLAY_MODE_PONG,   // experimental: tilt paddle game
                        DISPLAY_MODE_DODGE,  // experimental: tilt dodge game
                        DISPLAY_MODE_GESTURE // experimental: tilt gesture triggers
                      };
enum TimeDisplayFormat{ TIME_FORMAT_24H, TIME_FORMAT_12H };
enum VisualizerSource { VISUALIZER_SOURCE_DEVICE, VISUALIZER_SOURCE_PHONE };
enum VisualizerStyle  { VISUALIZER_STYLE_BARS, VISUALIZER_STYLE_WAVE, VISUALIZER_STYLE_RADIAL };
enum MessageAnimationStyle { MESSAGE_ANIM_NONE, MESSAGE_ANIM_WAVE, MESSAGE_ANIM_SCROLL, MESSAGE_ANIM_RAIN };
enum ClockTool        { CLOCK_TOOL_TIME, CLOCK_TOOL_ALARM, CLOCK_TOOL_TIMER, CLOCK_TOOL_STOPWATCH };
enum AlarmEditField   { ALARM_EDIT_HOUR, ALARM_EDIT_MINUTE };
enum TimerEditField   { TIMER_EDIT_MINUTES, TIMER_EDIT_SECONDS };

// ── DextBot sensor globals ────────────────────────────────────
// MPU-6050 I2C address.
// IMPORTANT: DS1307 RTC also uses 0x68 — connect AD0 → 3.3V for 0x69.
constexpr uint8_t  MPU6050_ADDR           = 0x69;
constexpr uint8_t  MPU6050_REG_PWR_MGMT   = 0x6B;
constexpr uint8_t  MPU6050_REG_ACCEL_XOUT = 0x3B;  // accel: 6 bytes
constexpr uint8_t  MPU6050_REG_GYRO_XOUT  = 0x43;  // gyro:  6 bytes

// Distance → expression thresholds (mm)
constexpr uint16_t DEXTBOT_DIST_SCARED    =  80;
constexpr uint16_t DEXTBOT_DIST_SURPRISE  = 200;
constexpr uint16_t DEXTBOT_DIST_CURIOUS   = 400;
constexpr uint16_t DEXTBOT_DIST_HAPPY     = 700;

// Gyro thresholds (raw units, ±250°/s full scale → 131 LSB/°/s)
// 131 * 30  ≈ 3900  = 30°/s  — gentle movement
// 131 * 150 ≈ 19650 = 150°/s — fast shake
// 131 * 400 ≈ 52400 = 400°/s — violent spin
constexpr int32_t  GYRO_THRESH_GENTLE     =  4000;
constexpr int32_t  GYRO_THRESH_SHAKE      = 20000;
constexpr int32_t  GYRO_THRESH_SPIN       = 52000;

// Dizzy/bonk expression hold time
constexpr uint32_t DEXTBOT_DIZZY_MS       = 1200;
constexpr uint32_t DEXTBOT_BONK_MS        =  800;

// Eye column centers and frame rate
constexpr uint8_t  DEXTBOT_LEFT_COL       =  8;
constexpr uint8_t  DEXTBOT_RIGHT_COL      = 23;
constexpr uint32_t DEXTBOT_BLINK_DURATION_MS = 160;
constexpr uint32_t DEXTBOT_FRAME_MS          =  40;   // 25 Hz

Adafruit_VL53L0X g_vl53;
bool     g_vl53Available        = false;
bool     g_mpu6050Available     = false;
uint16_t g_dextDistMm           = 400;
// Accelerometer (raw ±32768 = ±2 g)
int16_t  g_dextAccelX           = 0;
int16_t  g_dextAccelY           = 0;
int16_t  g_dextAccelZ           = 16384;
// Gyroscope (raw ±32768 = ±250°/s, 131 LSB/°/s)
int16_t  g_dextGyroX            = 0;
int16_t  g_dextGyroY            = 0;
int16_t  g_dextGyroZ            = 0;
// Gyro-driven state
float    g_gyroAngleZ           = 0.0f;  // integrated Z-axis spin angle
uint32_t g_lastGyroReadMs       = 0;
// Timing
uint32_t g_dextLastFrameMs      = 0;
bool     g_dextBlinking         = false;
uint32_t g_dextBlinkStartMs     = 0;
uint32_t g_dextGyroSettleUntilMs = 0;  // gyro reads ignored until MPU-6050 settles

// ── Main globals ─────────────────────────────────────────────
bool     g_isConnected                = false;
bool     g_restartAdvertisingPending  = false;
uint32_t g_bootTimestampMs            = 0;
uint32_t g_disconnectTimestampMs      = 0;
uint32_t g_lastAdvertisingKickMs      = 0;
uint32_t g_lastBatteryUpdateMs        = 0;
uint32_t g_lastSensorUpdateMs         = 0;
uint32_t g_lastSensorReadAttemptMs    = 0;
uint32_t g_otaLastActivityMs          = 0;
uint32_t g_otaRebootAtMs              = 0;
uint32_t g_lastDisplayUpdateMs        = 0;
uint32_t g_lastRtcWriteBackMs         = 0;
uint32_t g_lastNvsWriteBackMs         = 0;  // NVS flash writeback timestamp
uint32_t g_lastVisualizerFrameMs      = 0;
uint32_t g_lastPhoneVisualizerUpdateMs= 0;
uint32_t g_clockModeCycleStartMs      = 0;
uint32_t g_lastStopwatchDisplayMs     = 0;  // FIX 9
float    g_lastTemperatureC           = NAN;
float    g_lastHumidity               = NAN;
bool     g_hasSensorReading           = false;
uint32_t g_lastSensorSampleMs         = 0;   // FIX 10: age of last valid reading

// ── Weather globals ───────────────────────────────────────────
// Populated by WEATHER: BLE command sent from the phone app.
// Persisted to NVS so data survives a power cut.
float    g_weatherTempC               = NAN;   // outside temperature °C
float    g_weatherFeelsC              = NAN;   // feels-like temperature °C
uint8_t  g_weatherHumidity            = 0;     // outside humidity %
char     g_weatherCondition[8]        = {};    // short condition: RAIN, SNOW, CLDY, SUN…
char     g_weatherCity[24]            = {};    // city name (truncated to fit NVS)
bool     g_hasWeather                 = false; // true once at least one update received
uint32_t g_lastWeatherMs              = 0;     // millis() of last weather update
// Bug 4 fix: NVS writes must happen on the main loop task, not inside
// the BLE callback task. This flag signals the main loop to do the write.
bool     g_weatherNvsSavePending      = false;
bool     g_otaInProgress              = false;
bool     g_otaHasExpectedMd5          = false;
bool     g_otaRebootPending           = false;
size_t   g_otaExpectedSize            = 0;
size_t   g_otaReceivedSize            = 0;
// FIX 11: fixed buffer instead of String for OTA MD5
char     g_otaExpectedMd5[33]         = {};
bool     g_batteryAvailable           = false;
uint8_t  g_batteryLevel               = 0;
uint8_t  g_displayBrightness          = DISPLAY_DEFAULT_INTENSITY;
bool     g_rtcAvailable               = false;
bool     g_timeNotSet                 = true;
// g_lastDisplayedTimeValid removed — Bug 7 fix: declared but never used
bool     g_lastDisplayPlaceholder     = false;
TimeSourceState     g_timeSourceState       = TIME_INVALID;
TimeDisplayFormat   g_timeDisplayFormat     = TIME_FORMAT_24H;
VisualizerSource    g_visualizerSource      = VISUALIZER_SOURCE_DEVICE;
VisualizerStyle     g_visualizerStyle       = VISUALIZER_STYLE_BARS;
MessageAnimationStyle g_messageAnimationStyle = MESSAGE_ANIM_SCROLL;
String   g_lastDisplayedTimeText;
String   g_lastDisplayedStatusText;
DisplayMode g_currentMode             = DISPLAY_MODE_CLOCK;
DisplayMode g_pendingMenuMode         = DISPLAY_MODE_CLOCK;
String   g_appMessage                 = "HELLO";
String   g_activeScrollText           = "HELLO";
bool     g_menuActive                 = false;
bool     g_clockToolsMenuActive       = false;
bool     g_messageModeDirty           = true;
// g_visualizerPhase removed — Bug 8 fix: declared but never used
double   g_fftReal[FFT_SAMPLE_COUNT];
double   g_fftImag[FFT_SAMPLE_COUNT];
uint8_t  g_phoneVisualizerLevels[32]  = {};
uint8_t  g_lastVisualizerLevels[32]   = {};
ClockTool g_activeClockTool           = CLOCK_TOOL_TIME;
ClockTool g_pendingClockTool          = CLOCK_TOOL_TIME;
int      g_alarmHour                  = 7;
int      g_alarmMinute                = 0;
bool     g_alarmEnabled               = false;
bool     g_alarmRinging               = false;
bool     g_alarmEditMode              = false;
AlarmEditField g_alarmEditField       = ALARM_EDIT_HOUR;
uint32_t g_lastAlarmTriggerMinuteEpoch= 0;
int32_t  g_timerTotalSeconds          = 0;
int32_t  g_timerRemainingSeconds      = 0;
bool     g_timerRunning               = false;
uint32_t g_timerLastTickMs            = 0;
bool     g_timerFinished              = false;
bool     g_timerEditMode              = false;
TimerEditField g_timerEditField       = TIMER_EDIT_MINUTES;
bool     g_timerConfigured            = false;
bool     g_stopwatchRunning           = false;
uint32_t g_stopwatchElapsedMs         = 0;
uint32_t g_stopwatchLastTickMs        = 0;
uint32_t g_stopwatchLapMs             = 0;
bool     g_stopwatchLapFrozen         = false;
bool     g_buzzerOutputOn             = false;
uint32_t g_lastBuzzerToggleMs         = 0;
uint32_t g_buttonClickUntilMs         = 0;

const uint8_t kSpectralHeight[9] = {
  0b00000000, 0b10000000, 0b11000000, 0b11100000, 0b11110000,
  0b11111000, 0b11111100, 0b11111110, 0b11111111,
};

struct ButtonState { uint8_t pin; uint32_t lastPressMs; };
ButtonState g_modeButton  {BUTTON_MODE_PIN,   0};
ButtonState g_nextButton  {BUTTON_NEXT_PIN,   0};
ButtonState g_backButton  {BUTTON_BACK_PIN,   0};
ButtonState g_selectButton{BUTTON_SELECT_PIN, 0};

// ── Forward declarations ─────────────────────────────────────
String buildSystemInfoMessage();
void notifyTx(const String &message);
void renderDisplayText(const String &text);
void updateDisplayTime();
void updateClockToolDisplay();
void updateSettingsDisplay();
void startMessageDisplay();   // FIX: forward declaration required by applyDisplayMode
void handleVirtualButton(const String &buttonName);
void startAdvertisingNow(const char *reason, bool forceRestart);
String buildAlarmStateMessage();
String buildTimerStateMessage();
String buildStopwatchStateMessage();
// Experimental feature forward declarations
void initPong();
void initDodge();
void initGesture();

// ── Utility helpers ──────────────────────────────────────────

void writeBuzzer(bool enabled) {
  g_buzzerOutputOn = enabled;
  digitalWrite(BUZZER_PIN, enabled == BUZZER_ACTIVE_HIGH ? HIGH : LOW);
}

void triggerButtonClick() {
  if (g_alarmRinging || g_timerFinished) return;
  g_buttonClickUntilMs = millis() + BUTTON_CLICK_MS;
}

void stopBuzzerAlerts() {
  const bool wasActive = g_alarmRinging || g_timerFinished || g_buzzerOutputOn;
  g_alarmRinging  = false;
  g_timerFinished = false;
  writeBuzzer(false);
  if (wasActive) {
    updateClockToolDisplay();
    notifyTx("STATUS:ALERT_STOPPED");
  }
}

// FIX 1: clampBatteryPercent defined ONCE here (duplicate removed below)
uint8_t clampBatteryPercent(int value) {
  return static_cast<uint8_t>(constrain(value, 0, 100));
}

// FIX 1: readBatteryPercent defined ONCE here (duplicate removed below)
bool readBatteryPercent(uint8_t &batteryPercent) {
  if (BATTERY_SENSE_PIN < 0) return false;
  const int raw     = analogRead(BATTERY_SENSE_PIN);
  const int percent = map(raw, BATTERY_ADC_MIN, BATTERY_ADC_MAX, 0, 100);
  batteryPercent    = clampBatteryPercent(percent);
  return true;
}

// ── String-to-enum helpers ───────────────────────────────────

const char *clockToolToString(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:     return "ALARM";
    case CLOCK_TOOL_TIMER:     return "TIMER";
    case CLOCK_TOOL_STOPWATCH: return "STOPWATCH";
    default:                   return "TIME";
  }
}

const char *clockToolLabel(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:     return "ALRM";
    case CLOCK_TOOL_TIMER:     return "TIMR";
    case CLOCK_TOOL_STOPWATCH: return "STOP";
    default:                   return "TIME";
  }
}

const char *clockToolMenuText(ClockTool tool) {
  switch (tool) {
    case CLOCK_TOOL_ALARM:     return "ALARM SET";
    case CLOCK_TOOL_TIMER:     return "TIMER SET";
    case CLOCK_TOOL_STOPWATCH: return "STOPWATCH";
    default:                   return "TIME";
  }
}

const char *displayModeToString(DisplayMode mode) {
  switch (mode) {
    case DISPLAY_MODE_SETTINGS:   return "SETTINGS";
    case DISPLAY_MODE_MESSAGE:    return "MESSAGE";
    case DISPLAY_MODE_VISUALIZER: return "VISUALIZER";
    case DISPLAY_MODE_DEXTBOT:    return "DEXTBOT";
    case DISPLAY_MODE_PONG:       return "PONG";
    case DISPLAY_MODE_DODGE:      return "DODGE";
    case DISPLAY_MODE_GESTURE:    return "GESTURE";
    default:                      return "CLOCK";
  }
}

const char *displayModeLabel(DisplayMode mode) {
  switch (mode) {
    case DISPLAY_MODE_SETTINGS:   return "SET";
    case DISPLAY_MODE_MESSAGE:    return "MSG";
    case DISPLAY_MODE_VISUALIZER: return "VIZ";
    case DISPLAY_MODE_DEXTBOT:    return "BOT";
    case DISPLAY_MODE_PONG:       return "PONG";
    case DISPLAY_MODE_DODGE:      return "DODG";
    case DISPLAY_MODE_GESTURE:    return "GSTR";
    default:                      return "CLK";
  }
}

const char *timeFormatToString(TimeDisplayFormat fmt) {
  return fmt == TIME_FORMAT_12H ? "12" : "24";
}

const char *visualizerSourceToString(VisualizerSource src) {
  return src == VISUALIZER_SOURCE_PHONE ? "PHONE" : "DEVICE";
}

const char *visualizerStyleToString(VisualizerStyle style) {
  switch (style) {
    case VISUALIZER_STYLE_WAVE:   return "WAVE";
    case VISUALIZER_STYLE_RADIAL: return "RADIAL";
    default:                      return "BARS";
  }
}

const char *messageAnimationToString(MessageAnimationStyle style) {
  switch (style) {
    case MESSAGE_ANIM_NONE:   return "NONE";
    case MESSAGE_ANIM_WAVE:   return "WAVE";
    case MESSAGE_ANIM_RAIN:   return "RAIN";
    default:                  return "SCROLL";
  }
}

const char *timeSourceToString(TimeSourceState state) {
  switch (state) {
    case TIME_SOURCE_RTC:        return "RTC";
    case TIME_SOURCE_PHONE_SYNC: return "PHONE_SYNC";
    case TIME_SOURCE_NVS:        return "NVS_ESTIMATED";
    default:                     return "NONE";
  }
}

// ── Date / time ──────────────────────────────────────────────

bool isDateTimeValid(const DateTime &value) {
  if (value.year() < TIME_VALID_MIN_YEAR) return false;
  if (value.month()  < 1 || value.month()  > 12) return false;
  if (value.day()    < 1 || value.day()    > 31) return false;
  if (value.hour()   > 23 || value.minute() > 59 || value.second() > 59) return false;
  return true;
}

uint8_t clampDisplayBrightness(int value) {
  return static_cast<uint8_t>(constrain(value, 0, 15));
}

DateTime getSystemDateTime() {
  time_t now = time(nullptr);
  struct tm ti = {};
  localtime_r(&now, &ti);
  return DateTime(ti.tm_year + 1900, ti.tm_mon + 1, ti.tm_mday,
                  ti.tm_hour, ti.tm_min, ti.tm_sec);
}

void setSystemClock(const DateTime &value) {
  struct tm ti = {};
  ti.tm_year  = value.year()   - 1900;
  ti.tm_mon   = value.month()  - 1;
  ti.tm_mday  = value.day();
  ti.tm_hour  = value.hour();
  ti.tm_min   = value.minute();
  ti.tm_sec   = value.second();
  ti.tm_isdst = -1;
  const time_t epoch = mktime(&ti);
  timeval tv = {};
  tv.tv_sec   = epoch;
  settimeofday(&tv, nullptr);
}

// ── BLE notify helper ────────────────────────────────────────

void notifyTx(const String &message) {
  if (g_txCharacteristic == nullptr) return;
  g_txCharacteristic->setValue(message.c_str());
  if (g_isConnected) g_txCharacteristic->notify();
}

// ── OTA status helpers (FIX 11: snprintf, no String concat) ──

void sendOtaStatus(const char *status) {
  char buf[80];
  snprintf(buf, sizeof(buf), "OTA:%s", status);
  Serial.print("OTA status: ");
  Serial.println(status);
  notifyTx(String(buf));
}

void abortOta(const char *reason) {
  if (g_otaInProgress) Update.abort();
  g_otaInProgress     = false;
  g_otaHasExpectedMd5 = false;
  g_otaRebootPending  = false;
  g_otaExpectedSize   = 0;
  g_otaReceivedSize   = 0;
  g_otaLastActivityMs = 0;
  g_otaExpectedMd5[0] = '\0';
  char buf[48];
  snprintf(buf, sizeof(buf), "ERROR,%s", reason);
  sendOtaStatus(buf);
}

bool isValidMd5(const char *value) {
  if (strlen(value) != 32) return false;
  for (size_t i = 0; i < 32; ++i) {
    const char ch = value[i];
    if (!((ch >= '0' && ch <= '9') ||
          (ch >= 'a' && ch <= 'f') ||
          (ch >= 'A' && ch <= 'F'))) return false;
  }
  return true;
}

bool isAuthorizedOtaRequest(const char *token) {
  if (OTA_SHARED_SECRET_STR[0] == '\0') return true;
  return strcmp(token, OTA_SHARED_SECRET_STR) == 0;
}

bool parseHexByte(char hi, char lo, uint8_t &result) {
  auto hexVal = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
    if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
    return -1;
  };
  const int h = hexVal(hi), l = hexVal(lo);
  if (h < 0 || l < 0) return false;
  result = static_cast<uint8_t>((h << 4) | l);
  return true;
}

bool beginOta(const String &commandValue) {
  if (g_otaInProgress) { sendOtaStatus("ERROR,busy"); return false; }

  // Parse SIZE[,MD5[,TOKEN]] without String heap allocations
  char sizeField[16]  = {};
  char md5Field[40]   = {};
  char tokenField[64] = {};

  const char *p      = commandValue.c_str();
  const char *comma1 = strchr(p, ',');
  if (!comma1) {
    strncpy(sizeField, p, sizeof(sizeField) - 1);
  } else {
    size_t n = static_cast<size_t>(comma1 - p);
    if (n >= sizeof(sizeField)) n = sizeof(sizeField) - 1;
    strncpy(sizeField, p, n);
    const char *comma2 = strchr(comma1 + 1, ',');
    if (!comma2) {
      strncpy(md5Field, comma1 + 1, sizeof(md5Field) - 1);
    } else {
      size_t m = static_cast<size_t>(comma2 - (comma1 + 1));
      if (m >= sizeof(md5Field)) m = sizeof(md5Field) - 1;
      strncpy(md5Field, comma1 + 1, m);
      strncpy(tokenField, comma2 + 1, sizeof(tokenField) - 1);
    }
  }

  // Simple in-place trim
  // Bug 10 fix: original trim crashed on empty string because
  // end = s + strlen(s) - 1 underflows to a pointer before s when strlen=0.
  auto trim = [](char *s) {
    if (*s == '\0') return;  // nothing to trim
    char *end = s + strlen(s) - 1;
    while (end >= s && *end == ' ') *end-- = '\0';
    char *start = s;
    while (*start == ' ') ++start;
    if (start != s) memmove(s, start, strlen(start) + 1);
  };
  trim(sizeField); trim(md5Field); trim(tokenField);

  if (!isAuthorizedOtaRequest(tokenField)) {
    sendOtaStatus("ERROR,unauthorized"); return false;
  }
  if (md5Field[0] != '\0' && !isValidMd5(md5Field)) {
    sendOtaStatus("ERROR,invalid_md5"); return false;
  }

  const size_t expectedSize = static_cast<size_t>(atoi(sizeField));
  if (expectedSize == 0) { sendOtaStatus("ERROR,invalid_size"); return false; }

  Serial.printf("OTA begin: size=%u md5=%s auth=%s\n",
    static_cast<unsigned>(expectedSize),
    md5Field[0] ? md5Field : "none",
    tokenField[0] ? "provided" : "none");

  if (!Update.begin(expectedSize)) {
    Serial.print("Update.begin failed: "); Serial.println(Update.errorString());
    sendOtaStatus("ERROR,begin_failed"); return false;
  }

  g_otaExpectedSize   = expectedSize;
  g_otaReceivedSize   = 0;
  g_otaLastActivityMs = millis();
  g_otaInProgress     = true;
  strncpy(g_otaExpectedMd5, md5Field, sizeof(g_otaExpectedMd5) - 1);
  g_otaHasExpectedMd5 = (md5Field[0] != '\0');

  if (g_otaHasExpectedMd5 && !Update.setMD5(g_otaExpectedMd5)) {
    abortOta("md5_setup"); return false;
  }

  char buf[64];
  snprintf(buf, sizeof(buf), "READY,%u,%s",
    static_cast<unsigned>(g_otaExpectedSize),
    g_otaHasExpectedMd5 ? "MD5" : "NO_MD5");
  sendOtaStatus(buf);
  return true;
}

bool writeOtaChunk(const String &hexPayload) {
  if (!g_otaInProgress) { sendOtaStatus("ERROR,no_session"); return false; }

  const size_t hexLen = hexPayload.length();
  if (hexLen == 0 || (hexLen % 2) != 0) { abortOta("invalid_chunk"); return false; }

  const size_t chunkSize = hexLen / 2;
  if (chunkSize == 0 || chunkSize > OTA_MAX_CHUNK_BYTES) { abortOta("chunk_size"); return false; }

  uint8_t buffer[OTA_MAX_CHUNK_BYTES];
  for (size_t i = 0; i < chunkSize; ++i) {
    uint8_t value = 0;
    if (!parseHexByte(hexPayload[i * 2], hexPayload[i * 2 + 1], value)) {
      abortOta("chunk_hex"); return false;
    }
    buffer[i] = value;
  }

  const size_t remaining = g_otaExpectedSize - g_otaReceivedSize;
  if (chunkSize > remaining) { abortOta("overflow"); return false; }

  const size_t written = Update.write(buffer, chunkSize);
  if (written != chunkSize) {
    Serial.print("Update.write failed: "); Serial.println(Update.errorString());
    abortOta("write_failed"); return false;
  }

  g_otaReceivedSize  += written;
  g_otaLastActivityMs = millis();
  Serial.printf("OTA chunk: %u/%u bytes\n",
    static_cast<unsigned>(g_otaReceivedSize),
    static_cast<unsigned>(g_otaExpectedSize));

  char buf[32];
  snprintf(buf, sizeof(buf), "ACK,%u", static_cast<unsigned>(g_otaReceivedSize));
  sendOtaStatus(buf);
  return true;
}

bool finishOta() {
  if (!g_otaInProgress) { sendOtaStatus("ERROR,no_session"); return false; }

  Serial.printf("OTA finish: received=%u expected=%u\n",
    static_cast<unsigned>(g_otaReceivedSize),
    static_cast<unsigned>(g_otaExpectedSize));

  if (g_otaReceivedSize != g_otaExpectedSize) { abortOta("size_mismatch"); return false; }

  if (!Update.end(true)) {
    Serial.print("Update.end failed: "); Serial.println(Update.errorString());
    abortOta("end_failed"); return false;
  }

  g_otaInProgress     = false;
  g_otaHasExpectedMd5 = false;
  g_otaExpectedSize   = 0;
  g_otaReceivedSize   = 0;
  g_otaLastActivityMs = 0;
  g_otaExpectedMd5[0] = '\0';
  g_otaRebootPending  = true;
  g_otaRebootAtMs     = millis() + OTA_REBOOT_DELAY_MS;

  Serial.printf("OTA complete, reboot in %u ms\n", static_cast<unsigned>(OTA_REBOOT_DELAY_MS));

  char buf[32];
  snprintf(buf, sizeof(buf), "DONE,%s", FIRMWARE_REVISION);
  sendOtaStatus(buf);
  return true;
}

// ── Message builders (FIX 11: snprintf, no String concat) ────

// FIX 10: helper to check if the current sensor reading is fresh
bool isSensorFresh() {
  return g_hasSensorReading &&
         ((millis() - g_lastSensorSampleMs) < SENSOR_STALE_THRESHOLD_MS);
}

// ── Weather helpers ───────────────────────────────────────────

bool isWeatherFresh() {
  return g_hasWeather && ((millis() - g_lastWeatherMs) < WEATHER_STALE_MS);
}

// Maps an OpenWeatherMap main-condition string (sent by the phone app)
// to a 4-character display label that fits on the dot matrix.
// The phone should send the OWM "main" field: Clear, Clouds, Rain, etc.
const char *weatherCondToDisplay(const char *cond) {
  if      (strcasecmp(cond, "Clear")       == 0) return "SUN";
  else if (strcasecmp(cond, "Clouds")      == 0) return "CLDY";
  else if (strcasecmp(cond, "Rain")        == 0) return "RAIN";
  else if (strcasecmp(cond, "Drizzle")     == 0) return "DRZL";
  else if (strcasecmp(cond, "Thunderstorm")== 0) return "STRM";
  else if (strcasecmp(cond, "Snow")        == 0) return "SNOW";
  else if (strcasecmp(cond, "Mist")        == 0) return "MIST";
  else if (strcasecmp(cond, "Fog")         == 0) return "FOG";
  else if (strcasecmp(cond, "Haze")        == 0) return "HAZE";
  else if (strcasecmp(cond, "Dust")        == 0) return "DUST";
  else if (strcasecmp(cond, "Wind")        == 0) return "WIND";
  else if (strcasecmp(cond, "Tornado")     == 0) return "TRND";
  return "----";
}

// NVS: save weather data so it survives power cuts.
// Temperature stored as int16 (value × 10) to avoid float serialisation.
void saveWeatherToNvs() {
  g_prefs.begin(NVS_NAMESPACE, false);
  if (!g_hasWeather) {
    // Weather was cleared — remove the NVS timestamp to prevent stale
    // data from being restored on next boot.
    g_prefs.remove(NVS_KEY_WEATHER_TS);
    g_prefs.end();
    Serial.println("Weather NVS cleared");
    return;
  }
  g_prefs.putShort(NVS_KEY_WEATHER_TEMP,  static_cast<int16_t>(g_weatherTempC  * 10.0f));
  g_prefs.putShort(NVS_KEY_WEATHER_FEELS, static_cast<int16_t>(g_weatherFeelsC * 10.0f));
  g_prefs.putUChar(NVS_KEY_WEATHER_HUM,   g_weatherHumidity);
  g_prefs.putString(NVS_KEY_WEATHER_COND, g_weatherCondition);
  g_prefs.putString(NVS_KEY_WEATHER_CITY, g_weatherCity);
  g_prefs.putUInt(NVS_KEY_WEATHER_TS,     static_cast<uint32_t>(time(nullptr)));
  g_prefs.end();
  Serial.printf("Weather saved to NVS: %s %.1fC %s\n",
    g_weatherCity, g_weatherTempC, g_weatherCondition);
}

void loadWeatherFromNvs() {
  g_prefs.begin(NVS_NAMESPACE, true);
  const uint32_t savedTs = g_prefs.getUInt(NVS_KEY_WEATHER_TS, 0);
  if (savedTs == 0) { g_prefs.end(); return; }

  g_weatherTempC   = static_cast<float>(g_prefs.getShort(NVS_KEY_WEATHER_TEMP,  0)) / 10.0f;
  g_weatherFeelsC  = static_cast<float>(g_prefs.getShort(NVS_KEY_WEATHER_FEELS, 0)) / 10.0f;
  g_weatherHumidity= g_prefs.getUChar(NVS_KEY_WEATHER_HUM, 0);

  String cond = g_prefs.getString(NVS_KEY_WEATHER_COND, "");
  String city = g_prefs.getString(NVS_KEY_WEATHER_CITY, "");
  g_prefs.end();

  strncpy(g_weatherCondition, cond.c_str(), sizeof(g_weatherCondition) - 1);
  strncpy(g_weatherCity,      city.c_str(), sizeof(g_weatherCity)      - 1);
  g_hasWeather    = true;
  // Mark as stale so the display knows it's from NVS, not a fresh update.
  // It will be overwritten as soon as the phone sends new data.
  g_lastWeatherMs = millis() - (WEATHER_STALE_MS - 60000UL);
  Serial.printf("Weather loaded from NVS: %s %.1fC %s\n",
    g_weatherCity, g_weatherTempC, g_weatherCondition);
}

// Parses a WEATHER: BLE command payload.
// Expected format (all fields optional except TEMP):
//   T:22.5,F:20.1,C:Rain,H:65,CITY:London
bool applyWeatherUpdate(const String &payload) {
  float    newTemp   = NAN;
  float    newFeels  = NAN;
  uint8_t  newHum    = 0;
  char     newCond[8]  = {};
  char     newCity[24] = {};

  // Simple key:value CSV parser — no dynamic allocation
  int start = 0;
  while (start < static_cast<int>(payload.length())) {
    const int comma = payload.indexOf(',', start);
    const String token = (comma >= 0)
      ? payload.substring(start, comma)
      : payload.substring(start);
    const int colon = token.indexOf(':');
    if (colon > 0) {
      const String key = token.substring(0, colon);
      const String val = token.substring(colon + 1);
      if      (key.equalsIgnoreCase("T")    || key.equalsIgnoreCase("TEMP"))
        newTemp   = val.toFloat();
      else if (key.equalsIgnoreCase("F")    || key.equalsIgnoreCase("FEELS"))
        newFeels  = val.toFloat();
      else if (key.equalsIgnoreCase("H")    || key.equalsIgnoreCase("HUM"))
        newHum    = static_cast<uint8_t>(constrain(val.toInt(), 0, 100));
      else if (key.equalsIgnoreCase("C")    || key.equalsIgnoreCase("COND"))
        strncpy(newCond, val.c_str(), sizeof(newCond) - 1);
      else if (key.equalsIgnoreCase("CITY"))
        strncpy(newCity, val.c_str(), sizeof(newCity) - 1);
    }
    if (comma < 0) break;
    start = comma + 1;
  }

  if (isnan(newTemp)) {
    notifyTx("STATUS:WEATHER_INVALID");
    return false;
  }

  g_weatherTempC   = newTemp;
  g_weatherFeelsC  = isnan(newFeels) ? newTemp : newFeels;
  g_weatherHumidity= newHum;
  strncpy(g_weatherCondition, newCond, sizeof(g_weatherCondition) - 1);
  strncpy(g_weatherCity,      newCity, sizeof(g_weatherCity)      - 1);
  g_hasWeather    = true;
  g_lastWeatherMs = millis();
  // Bug 4 fix: do NOT call saveWeatherToNvs() here — this runs inside
  // a BLE FreeRTOS task and Preferences is not thread-safe. Set a flag
  // and let the main loop call it safely via serviceWeatherNvsSave().
  g_weatherNvsSavePending = true;

  Serial.printf("Weather updated: %s %.1fC feels %.1fC %s H:%u%%\n",
    g_weatherCity, g_weatherTempC, g_weatherFeelsC,
    g_weatherCondition, static_cast<unsigned>(g_weatherHumidity));

  // If the clock is visible, reset the cycle so the new weather shows promptly
  if (g_currentMode == DISPLAY_MODE_CLOCK && !g_menuActive &&
      g_activeClockTool == CLOCK_TOOL_TIME) {
    g_clockModeCycleStartMs = millis();
    g_lastDisplayedTimeText = "";
  }

  notifyTx("STATUS:WEATHER_UPDATED");
  return true;
}

String buildWeatherMessage() {
  if (!g_hasWeather) return String("WEATHER:NONE");
  // Bug 3 fix: buf increased from 80 to 120 — city name alone can be
  // 23 chars and Thunderstorm + full format easily exceeded 80.
  char buf[120];
  snprintf(buf, sizeof(buf), "WEATHER:T:%.1f,F:%.1f,C:%s,H:%u,CITY:%s,FRESH:%s",
    g_weatherTempC, g_weatherFeelsC,
    g_weatherCondition,
    static_cast<unsigned>(g_weatherHumidity),
    g_weatherCity,
    isWeatherFresh() ? "1" : "0");
  return String(buf);
}

String buildSystemInfoMessage() {
  char buf[320];
  int n = snprintf(buf, sizeof(buf),
    "INFO:NAME:%s,MODEL:%s,FW:%s,MODE:%s,CTOOL:%s,MANIM:%s"
    ",VIZ:%s,VSTYLE:%s,BR:%u,TF:%s,TIME:%s,SRC:%s",
    DEVICE_NAME, MODEL_NUMBER, FIRMWARE_REVISION,
    displayModeToString(g_currentMode),
    clockToolToString(g_activeClockTool),
    messageAnimationToString(g_messageAnimationStyle),
    visualizerSourceToString(g_visualizerSource),
    visualizerStyleToString(g_visualizerStyle),
    static_cast<unsigned>(g_displayBrightness),
    timeFormatToString(g_timeDisplayFormat),
    g_timeNotSet ? "INVALID" : "VALID",
    timeSourceToString(g_timeSourceState));

  if (g_batteryAvailable) {
    n += snprintf(buf + n, sizeof(buf) - n, ",BAT:%u",
      static_cast<unsigned>(g_batteryLevel));
  } else {
    n += snprintf(buf + n, sizeof(buf) - n, ",BAT:N/A");
  }

  if (isSensorFresh()) {
    // Bug 2 fix: n must be updated after sensor write so weather
    // strncat appends at the correct position and not over sensor data.
    n += snprintf(buf + n, sizeof(buf) - n, ",T:%.1f,H:%.1f",
      g_lastTemperatureC, g_lastHumidity);
  }

  if (isWeatherFresh()) {
    // Bug 3 fix: weather buf was 48 — now 64 to fit long city names safely.
    char wbuf[64];
    snprintf(wbuf, sizeof(wbuf), ",WT:%.1f,WC:%s,WCITY:%s",
      g_weatherTempC, g_weatherCondition, g_weatherCity);
    strncat(buf, wbuf, sizeof(buf) - strlen(buf) - 1);
  }

  return String(buf);
}

String buildVersionMessage() {
  char buf[32];
  snprintf(buf, sizeof(buf), "VERSION:%s", FIRMWARE_REVISION);
  return String(buf);
}

String buildSensorMessage() {
  // FIX 10: report UNAVAILABLE if reading is absent or stale
  if (!isSensorFresh()) return String("SENSOR:UNAVAILABLE");
  char buf[32];
  snprintf(buf, sizeof(buf), "SENSOR:T:%.1f,H:%.1f",
    g_lastTemperatureC, g_lastHumidity);
  return String(buf);
}

String buildAlarmStateMessage() {
  char buf[32];
  snprintf(buf, sizeof(buf), "ALARM:%02d:%02d,%s",
    g_alarmHour, g_alarmMinute, g_alarmEnabled ? "ON" : "OFF");
  return String(buf);
}

String buildTimerStateMessage() {
  const int32_t seconds    = max<int32_t>(g_timerRemainingSeconds, 0);
  const int32_t minutes    = seconds / 60;
  const int32_t remSeconds = seconds % 60;
  const char *state = g_timerFinished ? "DONE" : (g_timerRunning ? "RUNNING" : "PAUSED");
  char buf[40];
  snprintf(buf, sizeof(buf), "TIMER:%02ld:%02ld,%s",
    static_cast<long>(minutes), static_cast<long>(remSeconds), state);
  return String(buf);
}

String buildStopwatchStateMessage() {
  const uint32_t baseMs      = g_stopwatchLapFrozen ? g_stopwatchLapMs : g_stopwatchElapsedMs;
  const uint32_t totalSecs   = baseMs / 1000;
  const uint32_t minutes     = (totalSecs / 60) % 100;
  const uint32_t seconds     = totalSecs % 60;
  const char *state          = g_stopwatchRunning ? "RUNNING" : "PAUSED";
  char buf[40];
  snprintf(buf, sizeof(buf), "SW:%02lu:%02lu,%s",
    static_cast<unsigned long>(minutes),
    static_cast<unsigned long>(seconds), state);
  return String(buf);
}

// ── Battery ──────────────────────────────────────────────────

void updateBatteryCharacteristic(bool forceNotify) {
  if (g_batteryCharacteristic == nullptr) return;
  uint8_t newLevel = 0;
  g_batteryAvailable = readBatteryPercent(newLevel);
  if (!g_batteryAvailable) return;
  const bool changed = newLevel != g_batteryLevel;
  g_batteryLevel = newLevel;
  g_batteryCharacteristic->setValue(&g_batteryLevel, 1);
  if (g_isConnected && (forceNotify || changed)) g_batteryCharacteristic->notify();
  Serial.printf("Battery level: %u%%\n", static_cast<unsigned>(g_batteryLevel));
}

// ── Sensor ───────────────────────────────────────────────────

bool updateSensorReading() {
  if (g_otaInProgress) return false;
  const uint32_t now = millis();
  if ((now - g_lastSensorReadAttemptMs) < SENSOR_MIN_READ_INTERVAL_MS) return false;
  g_lastSensorReadAttemptMs = now;
  const float humidity    = g_dht.readHumidity();
  const float temperatureC= g_dht.readTemperature();
  if (isnan(humidity) || isnan(temperatureC)) {
    Serial.println("DHT22 read failed");
    return false;
  }
  g_lastTemperatureC   = temperatureC;
  g_lastHumidity       = humidity;
  g_hasSensorReading   = true;
  g_lastSensorSampleMs = millis();  // FIX 10: stamp sample age
  Serial.printf("Sensor: T=%.1fC H=%.1f%%\n", g_lastTemperatureC, g_lastHumidity);
  return true;
}

// ── Display helpers ──────────────────────────────────────────

void showMenuSelection() {
  const char *label = displayModeLabel(g_pendingMenuMode);
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == label) return;
  g_lastDisplayPlaceholder  = true;
  g_lastDisplayedStatusText = label;
  g_matrix.displayClear();
  g_matrix.displayText(label, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.printf("Menu: %s\n", label);
}

void showClockToolSelection() {
  const char *label = clockToolLabel(g_pendingClockTool);
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == label) return;
  g_lastDisplayPlaceholder  = true;
  g_lastDisplayedStatusText = label;
  g_matrix.displayClear();
  g_matrix.displayText(label, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.printf("Clock tool: %s\n", label);
}

String buildAlarmDisplayText() {
  if (g_alarmRinging) return String("ALRM");
  char buf[6];
  if (g_alarmEditMode) {
    snprintf(buf, sizeof(buf),
      g_alarmEditField == ALARM_EDIT_HOUR ? "H%02d" : "M%02d",
      g_alarmEditField == ALARM_EDIT_HOUR ? g_alarmHour : g_alarmMinute);
  } else {
    snprintf(buf, sizeof(buf), "%02d:%02d", g_alarmHour, g_alarmMinute);
  }
  return String(buf);
}

String buildTimerDisplayText() {
  char buf[6];
  if (g_timerEditMode) {
    const int32_t total   = max<int32_t>(g_timerTotalSeconds, 0);
    const int32_t minutes = total / 60;
    const int32_t seconds = total % 60;
    snprintf(buf, sizeof(buf),
      g_timerEditField == TIMER_EDIT_MINUTES ? "M%02ld" : "S%02ld",
      static_cast<long>(g_timerEditField == TIMER_EDIT_MINUTES ? minutes : seconds));
  } else {
    const int32_t s   = max<int32_t>(g_timerRemainingSeconds, 0);
    const int32_t min = s / 60;
    const int32_t sec = s % 60;
    snprintf(buf, sizeof(buf), "%02ld:%02ld", static_cast<long>(min), static_cast<long>(sec));
  }
  return String(buf);
}

String buildStopwatchDisplayText() {
  const uint32_t baseMs  = g_stopwatchLapFrozen ? g_stopwatchLapMs : g_stopwatchElapsedMs;
  const uint32_t totalS  = baseMs / 1000;
  const uint32_t minutes = (totalS / 60) % 100;
  const uint32_t seconds = totalS % 60;
  char buf[6];
  snprintf(buf, sizeof(buf), "%02lu:%02lu",
    static_cast<unsigned long>(minutes), static_cast<unsigned long>(seconds));
  return String(buf);
}

void renderDisplayText(const String &text) {
  if (!g_lastDisplayPlaceholder && g_lastDisplayedTimeText == text) return;
  g_lastDisplayPlaceholder = false;
  g_lastDisplayedTimeText  = text;
  g_matrix.displayClear();
  g_matrix.displayText(text.c_str(), PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.print("Display: "); Serial.println(text);
}

void updateClockToolDisplay() {
  if (g_currentMode != DISPLAY_MODE_CLOCK || g_menuActive || g_clockToolsMenuActive) return;
  switch (g_activeClockTool) {
    case CLOCK_TOOL_ALARM:
      renderDisplayText(buildAlarmDisplayText()); break;
    case CLOCK_TOOL_TIMER:
      renderDisplayText(g_timerFinished ? String("DONE") : buildTimerDisplayText()); break;
    case CLOCK_TOOL_STOPWATCH:
      renderDisplayText(buildStopwatchDisplayText()); break;
    default:
      updateDisplayTime(); break;
  }
}

void updateSettingsDisplay() {
  if (g_currentMode != DISPLAY_MODE_SETTINGS || g_menuActive) return;
  char buf[6];
  snprintf(buf, sizeof(buf), "B%02u", static_cast<unsigned>(g_displayBrightness));
  renderDisplayText(String(buf));
}

void activateClockTool(ClockTool tool) {
  g_currentMode           = DISPLAY_MODE_CLOCK;
  g_activeClockTool       = tool;
  g_menuActive            = false;
  g_clockToolsMenuActive  = false;
  g_alarmEditMode         = false;
  g_timerEditMode         = false;
  g_stopwatchLapFrozen    = false;
  g_lastDisplayedStatusText = "";
  g_lastDisplayedTimeText   = "";
  updateClockToolDisplay();
}

void applyDisplayBrightness(uint8_t brightness) {
  g_displayBrightness = clampDisplayBrightness(brightness);
  g_matrix.setIntensity(g_displayBrightness);
  Serial.printf("Brightness: %u\n", static_cast<unsigned>(g_displayBrightness));
}

// ── Visualizer ───────────────────────────────────────────────

void drawVisualizerBars(const uint8_t *levels) {
  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (!gfx) return;
  for (uint8_t i = 0; i < 32; ++i)
    gfx->setColumn(31 - i, kSpectralHeight[constrain(levels[i], 0, 8)]);
  g_matrix.displayAnimate();
}

void drawVisualizerWave(const uint8_t *levels) {
  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (!gfx) return;
  gfx->clear();
  for (uint8_t i = 0; i < 32; ++i) {
    const uint8_t level = constrain(levels[i], 0, 8);
    gfx->setPoint(level == 0 ? 7 : static_cast<uint8_t>(8 - level), 31 - i, true);
  }
  g_matrix.displayAnimate();
}

void drawVisualizerRadial(const uint8_t *levels) {
  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (!gfx) return;
  gfx->clear();
  uint16_t total = 0;
  for (uint8_t i = 0; i < 32; ++i) total += constrain(levels[i], 0, 8);
  const uint8_t radius = static_cast<uint8_t>(map(total, 0, 32 * 8, 0, 16));
  for (uint8_t offset = 0; offset < radius && offset < 16; ++offset) {
    uint8_t col = kSpectralHeight[min<uint8_t>(offset / 2 + 1, 8)];
    gfx->setColumn(15 - offset, col);
    gfx->setColumn(16 + offset, col);
  }
  g_matrix.displayAnimate();
}

void drawVisualizerFrame(const uint8_t *levels) {
  memcpy(g_lastVisualizerLevels, levels, sizeof(g_lastVisualizerLevels));
  switch (g_visualizerStyle) {
    case VISUALIZER_STYLE_WAVE:   drawVisualizerWave(levels);   break;
    case VISUALIZER_STYLE_RADIAL: drawVisualizerRadial(levels); break;
    default:                      drawVisualizerBars(levels);   break;
  }
}

void renderDeviceVisualizerFrame() {
  // TODO (FIX 8): move ADC sampling + FFT to a dedicated FreeRTOS task.
  // As currently written this blocks the main loop for ~16 ms per frame
  // (64 samples × 250 µs). Buttons, BLE notifications, and alarms are
  // unresponsive during that window. Create a task that fills g_fftReal[]
  // under a mutex, then signal the main task when a frame is ready.

  // ── KY-037 specific sampling ─────────────────────────────────────────
  // The KY-037 AO pin outputs the raw microphone signal biased to ~VCC/2
  // (≈ 2048 on a 12-bit ADC). Without removing this DC offset the FFT
  // produces a huge spike at bin 0 that completely drowns out the audio
  // content in the higher bins.
  //
  // Steps:
  //  1. Read 64 samples at 4 kHz.
  //  2. Compute the mean of those samples (captures the DC level).
  //  3. Subtract the mean from every sample (AC coupling in software).
  //  4. Apply MIC_GAIN to bring the small microphone signal up to a
  //     useful FFT magnitude range.
  //
  // If the visualizer is too sensitive, lower MIC_GAIN.
  // If bars barely move even for loud sounds, raise MIC_GAIN.
  // If bin 0 is still dominant, check that MIC_DC_OFFSET matches your
  // module's idle ADC reading (Serial.println(analogRead(MIC_INPUT_PIN))
  // in a quiet room to find the right value).

  // Optional hardware-pot sensitivity override
  double gain = MIC_GAIN;
  if (MIC_SENSITIVITY_PIN >= 0) {
    gain = map(analogRead(MIC_SENSITIVITY_PIN), 0, 4095, 2, 20);
  }

  // Step 1: collect samples
  for (uint16_t i = 0; i < FFT_SAMPLE_COUNT; ++i) {
    g_fftReal[i] = static_cast<double>(analogRead(MIC_INPUT_PIN));
    g_fftImag[i] = 0.0;
    delayMicroseconds(static_cast<uint32_t>(1000000.0 / FFT_SAMPLING_FREQUENCY));
  }

  // Step 2: compute mean (DC level)
  double dcSum = 0.0;
  for (uint16_t i = 0; i < FFT_SAMPLE_COUNT; ++i) dcSum += g_fftReal[i];
  const double dcMean = dcSum / FFT_SAMPLE_COUNT;

  // Step 3 + 4: remove DC offset and apply gain
  for (uint16_t i = 0; i < FFT_SAMPLE_COUNT; ++i) {
    g_fftReal[i] = (g_fftReal[i] - dcMean) * gain;
  }

  // FFT
  g_fft.windowing(g_fftReal, FFT_SAMPLE_COUNT, FFT_WIN_TYP_HAMMING, FFT_FORWARD);
  g_fft.compute(g_fftReal, g_fftImag, FFT_SAMPLE_COUNT, FFT_FORWARD);
  g_fft.complexToMagnitude(g_fftReal, g_fftImag, FFT_SAMPLE_COUNT);

  // Skip bin 0 (residual DC) and bin 1 (sub-bass that the KY-037 mic
  // doesn't reproduce well). Map bins 2..33 to display columns 0..31.
  uint8_t levels[32];
  for (uint8_t i = 0; i < 32; ++i) {
    const double mag = constrain(g_fftReal[i + 2], 0.0, FFT_MAG_CEILING);
    levels[i] = static_cast<uint8_t>(map(static_cast<long>(mag),
                                         0, static_cast<long>(FFT_MAG_CEILING),
                                         0, 8));
  }

  drawVisualizerFrame(levels);
}

void renderPhoneVisualizerFrame() {
  drawVisualizerFrame(g_phoneVisualizerLevels);
}

bool parsePhoneVisualizerFrame(const String &payload) {
  uint8_t parsed[32] = {};
  size_t count = 0;
  int start = 0;
  while (start <= static_cast<int>(payload.length()) && count < 32) {
    const int comma = payload.indexOf(',', start);
    String tok = comma >= 0 ? payload.substring(start, comma) : payload.substring(start);
    tok.trim();
    if (tok.isEmpty()) return false;
    const int level = tok.toInt();
    if (level < 0 || level > 8) return false;
    parsed[count++] = static_cast<uint8_t>(level);
    if (comma < 0) break;
    start = comma + 1;
  }
  if (count != 32) return false;
  memcpy(g_phoneVisualizerLevels, parsed, sizeof(g_phoneVisualizerLevels));
  g_lastPhoneVisualizerUpdateMs = millis();
  return true;
}

void applyDisplayMode(DisplayMode mode) {
  g_currentMode           = mode;
  g_menuActive            = false;
  g_clockToolsMenuActive  = false;
  g_lastDisplayPlaceholder= false;
  g_lastDisplayedStatusText = "";
  g_lastDisplayedTimeText   = "";
  g_messageModeDirty      = true;
  g_clockModeCycleStartMs = millis();
  switch (g_currentMode) {
    case DISPLAY_MODE_MESSAGE:
      startMessageDisplay(); break;
    case DISPLAY_MODE_VISUALIZER:
      g_lastDisplayedTimeText = "";
      g_lastDisplayedStatusText = "";
      if (g_visualizerSource == VISUALIZER_SOURCE_DEVICE)
        renderDeviceVisualizerFrame();
      else
        renderPhoneVisualizerFrame();
      break;
    case DISPLAY_MODE_SETTINGS: updateSettingsDisplay(); break;
    case DISPLAY_MODE_DEXTBOT:
      g_matrix.displayClear();
      g_matrix.displayAnimate();
      g_gyroAngleZ             = 0.0f;
      g_lastGyroReadMs         = 0;
      g_dextGyroSettleUntilMs  = millis() + 300;
      break;
    case DISPLAY_MODE_PONG:
      initPong();
      g_matrix.displayClear();
      g_matrix.displayAnimate();
      break;
    case DISPLAY_MODE_DODGE:
      initDodge();
      g_matrix.displayClear();
      g_matrix.displayAnimate();
      break;
    case DISPLAY_MODE_GESTURE:
      initGesture();
      renderDisplayText(String("TILT"));
      break;
    default:                    updateDisplayTime();     break;
  }
  Serial.printf("Active mode: %s\n", displayModeToString(g_currentMode));
}

void startMessageDisplay() {
  g_activeScrollText = g_appMessage;
  g_activeScrollText.trim();
  if (g_activeScrollText.isEmpty()) g_activeScrollText = "NO MSG";
  g_matrix.displayClear();
  textEffect_t entry = PA_SCROLL_LEFT, exit = PA_SCROLL_LEFT;
  uint16_t speed = 50;
  switch (g_messageAnimationStyle) {
    case MESSAGE_ANIM_NONE:
      entry = PA_PRINT; exit = PA_NO_EFFECT; speed = 0; break;
    case MESSAGE_ANIM_WAVE:
      entry = PA_SCROLL_UP; exit = PA_SCROLL_DOWN; speed = 40; break;
    case MESSAGE_ANIM_RAIN:
      entry = PA_SCROLL_DOWN; exit = PA_SCROLL_DOWN; speed = 40; break;
    default: break;
  }
  g_matrix.displayText(g_activeScrollText.c_str(), PA_LEFT, speed, 0, entry, exit);
  g_matrix.displayReset();
  g_messageModeDirty = false;
  Serial.print("Message: "); Serial.println(g_activeScrollText);
}

void showSyncNeeded() {
  constexpr char kPH[] = "--:--";
  if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == kPH) return;
  g_lastDisplayPlaceholder  = true;
  g_lastDisplayedStatusText = kPH;
  g_matrix.displayClear();
  g_matrix.displayText(kPH, PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.println("Display: --:-- (waiting for sync)");
}

String buildClockModeDisplayText(const DateTime &now, uint32_t nowMs) {
  static const char *kWeekdays[] = {"SUN","MON","TUE","WED","THU","FRI","SAT"};

  // Cycle duration grows when fresh weather data is available
  const bool showWeather = isWeatherFresh();
  const uint32_t cycleDuration =
    CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS + CLOCK_TEMP_VIEW_MS + CLOCK_DAY_VIEW_MS +
    (showWeather ? CLOCK_WEATHER_COND_VIEW_MS + CLOCK_WEATHER_TEMP_VIEW_MS : 0);

  const uint32_t cycleOffset = (nowMs - g_clockModeCycleStartMs) % cycleDuration;

  // ── Slide 1: time ────────────────────────────────────────
  if (cycleOffset < CLOCK_TIME_VIEW_MS) {
    int h = now.hour();
    if (g_timeDisplayFormat == TIME_FORMAT_12H) { h %= 12; if (!h) h = 12; }
    char buf[6]; snprintf(buf, sizeof(buf), "%02d:%02d", h, now.minute());
    return String(buf);
  }

  // ── Slide 2: date ────────────────────────────────────────
  if (cycleOffset < CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS) {
    char buf[6]; snprintf(buf, sizeof(buf), "%02d-%02d", now.day(), now.month());
    return String(buf);
  }

  // ── Slide 3: indoor temperature (DHT22) ─────────────────
  if (cycleOffset < CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS + CLOCK_TEMP_VIEW_MS) {
    if (!isSensorFresh() || isnan(g_lastTemperatureC)) return String("----");
    char buf[6]; snprintf(buf, sizeof(buf), "%2.0fC", g_lastTemperatureC);
    return String(buf);
  }

  // ── Slide 4: day name ────────────────────────────────────
  if (cycleOffset < CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS +
                    CLOCK_TEMP_VIEW_MS  + CLOCK_DAY_VIEW_MS) {
    return String(kWeekdays[now.dayOfTheWeek()]);
  }

  if (!showWeather) return String(kWeekdays[now.dayOfTheWeek()]);

  // ── Slide 5: weather condition icon ──────────────────────
  const uint32_t afterDay = CLOCK_TIME_VIEW_MS + CLOCK_DATE_VIEW_MS +
                             CLOCK_TEMP_VIEW_MS + CLOCK_DAY_VIEW_MS;
  if (cycleOffset < afterDay + CLOCK_WEATHER_COND_VIEW_MS) {
    // Show the 4-char condition label, e.g. "RAIN", "CLDY", "SUN"
    return String(weatherCondToDisplay(g_weatherCondition));
  }

  // ── Slide 6: outside temperature (from phone/OWM) ────────
  // Prefix 'o' distinguishes outside temp from indoor temp
  char buf[6];
  snprintf(buf, sizeof(buf), "o%2.0fC", g_weatherTempC);
  return String(buf);
}

void showTimeEstimated() {
  // Alternates between the estimated time and "EST" every 2 seconds
  // so the user can see the approximate time while knowing it needs sync.
  const bool showEst = ((millis() / 2000) % 2) == 0;
  if (showEst) {
    if (g_lastDisplayPlaceholder && g_lastDisplayedStatusText == "EST") return;
    g_lastDisplayPlaceholder  = true;
    g_lastDisplayedStatusText = "EST";
    g_matrix.displayClear();
    g_matrix.displayText("EST", PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
    g_matrix.displayAnimate();
  } else {
    // Show estimated time normally but don't suppress the EST cycle
    g_lastDisplayPlaceholder = false;
    renderDisplayText(buildClockModeDisplayText(getSystemDateTime(), millis()));
  }
}

void updateDisplayTime() {
  if (g_currentMode != DISPLAY_MODE_CLOCK || g_menuActive ||
      g_clockToolsMenuActive || g_activeClockTool != CLOCK_TOOL_TIME) return;

  if (g_timeNotSet) {
    showSyncNeeded();
    return;
  }

  // TIME_SOURCE_NVS means the time was loaded from flash on boot.
  // It is the time as of the last NVS save before power was cut — it does
  // NOT account for how long the device was powered off. Show the estimated
  // time but alternate with "EST" every 2 seconds so the user knows to sync.
  // Once the phone syncs, timeSourceState moves to TIME_SOURCE_PHONE_SYNC
  // and this block is no longer entered.
  if (g_timeSourceState == TIME_SOURCE_NVS) {
    showTimeEstimated();
    return;
  }

  renderDisplayText(buildClockModeDisplayText(getSystemDateTime(), millis()));
}

void initDisplay() {
  g_matrix.begin();
  applyDisplayBrightness(DISPLAY_DEFAULT_INTENSITY);
  g_matrix.displayClear();
  g_matrix.displayText("BOOT", PA_CENTER, 0, 0, PA_PRINT, PA_NO_EFFECT);
  g_matrix.displayAnimate();
  Serial.println("Matrix display initialised");
}

// ── NVS (flash) time persistence ─────────────────────────────
//
// Saves the current unix epoch to flash every NVS_WRITEBACK_INTERVAL_MS.
// On boot, if the RTC battery is dead or missing, we load the last saved
// epoch from flash. The time will be wrong by however long the device was
// off, but it is far better than showing "--:--" with no phone nearby.
// Once the phone syncs, the accurate time overwrites the estimate.

void saveTimeToNvs(const DateTime &value) {
  const uint32_t epoch = value.unixtime();
  if (epoch == 0) return;   // never save an invalid epoch
  g_prefs.begin(NVS_NAMESPACE, false);
  g_prefs.putUInt(NVS_KEY_EPOCH,   epoch);
  g_prefs.putUInt(NVS_KEY_SAVE_TS, epoch);  // record WHEN we saved so boot can check age
  g_prefs.end();
  g_lastNvsWriteBackMs = millis();
  Serial.printf("NVS time saved: epoch=%u\n", static_cast<unsigned>(epoch));
}

bool loadTimeFromNvs() {
  g_prefs.begin(NVS_NAMESPACE, true);  // read-only
  const uint32_t epoch  = g_prefs.getUInt(NVS_KEY_EPOCH,   0);
  const uint32_t saveTs = g_prefs.getUInt(NVS_KEY_SAVE_TS, 0);
  g_prefs.end();

  if (epoch == 0) {
    Serial.println("NVS: no saved time found");
    return false;
  }

  // Sanity check — reject obviously wrong epoch values
  const DateTime candidate(epoch);
  if (!isDateTimeValid(candidate)) {
    Serial.println("NVS: saved epoch invalid");
    return false;
  }

  // Age check — if save timestamp is present, calculate how old the data is.
  // epoch and saveTs are both unix timestamps so their difference is seconds.
  // If the data is older than NVS_TIME_MAX_AGE_S, the time would be too
  // wrong to display — show --:-- instead and wait for phone sync.
  if (saveTs > 0 && epoch >= saveTs) {
    const uint32_t ageSeconds = epoch - saveTs;
    // saveTs is when we last wrote NVS. epoch is what we wrote.
    // They should be identical or very close. If they differ significantly
    // it means the epoch was loaded from a much older save — reject it.
    // Normal case: ageSeconds ≈ 0 (saved at the same moment)
    // After power cut: epoch was frozen at save time, saveTs matches epoch
    // so ageSeconds ≈ 0 here — we can't detect power-off duration this way.
    (void)ageSeconds;  // not used directly — see explanation below
  }

  // The key insight: we saved BOTH epoch AND saveTs at the same time,
  // so they will always be identical (age ≈ 0). What we actually need
  // is to compare the saved epoch against the CURRENT real-world time —
  // but we have no real-world time on boot (that's why we're loading NVS).
  //
  // Solution: store the epoch AND the millis()-equivalent so we can bound
  // how old the data could be. We use saveTs as a proxy for "last known
  // good time". If the saved epoch looks like it's from a valid recent
  // period we show it with an "EST" marker. If RTC is unavailable AND
  // the NVS time is present, the best we can do is show it as estimated
  // and immediately request a sync from the phone.

  setSystemClock(candidate);
  g_timeNotSet      = false;
  g_timeSourceState = TIME_SOURCE_NVS;

  Serial.printf("Time from NVS (estimated): %04d-%02d-%02d %02d:%02d:%02d\n",
    candidate.year(), candidate.month(), candidate.day(),
    candidate.hour(), candidate.minute(), candidate.second());
  Serial.println("NVS time loaded — may be off by power-off duration. Sync recommended.");
  return true;
}

// ── RTC ──────────────────────────────────────────────────────

// Checks the DS1307 Clock Halt (CH) bit in register 0.
// If the CH bit is set the oscillator was stopped — this happens when the
// RTC backup battery is dead or missing and the main power was cut.
// RTClib does not expose this bit directly so we read it via Wire.
bool isRtcOscillatorStopped() {
  Wire.beginTransmission(0x68);  // DS1307 I2C address
  Wire.write(0x00);              // register 0 = seconds + CH bit
  if (Wire.endTransmission() != 0) return true;   // comms failed = assume stopped
  Wire.requestFrom(0x68, 1);
  if (!Wire.available()) return true;
  const uint8_t reg0 = Wire.read();
  return (reg0 & 0x80) != 0;    // bit 7 = CH (Clock Halt)
}

bool saveTimeToRtc(const DateTime &value) {
  if (!g_rtcAvailable) return false;
  g_rtc.adjust(value);   // adjust() also clears the CH bit, re-enabling the oscillator
  Serial.printf("RTC updated: %04d-%02d-%02d %02d:%02d:%02d\n",
    value.year(), value.month(), value.day(),
    value.hour(), value.minute(), value.second());
  return true;
}

bool loadTimeFromRtc() {
  if (!g_rtcAvailable) return false;

  // Detect a stopped oscillator (dead/missing backup battery) before
  // trusting the time value — a halted DS1307 may still return a plausible
  // but frozen DateTime that passes isDateTimeValid().
  if (isRtcOscillatorStopped()) {
    Serial.println("RTC oscillator halted — battery may be dead or missing");
    return false;
  }

  const DateTime rtcNow = g_rtc.now();
  if (!isDateTimeValid(rtcNow)) {
    Serial.println("RTC time invalid");
    return false;
  }

  setSystemClock(rtcNow);
  g_timeNotSet      = false;
  g_timeSourceState = TIME_SOURCE_RTC;
  Serial.printf("Time from RTC: %04d-%02d-%02d %02d:%02d:%02d\n",
    rtcNow.year(), rtcNow.month(), rtcNow.day(),
    rtcNow.hour(), rtcNow.minute(), rtcNow.second());
  return true;
}

bool initRtc() {
  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  g_rtcAvailable = g_rtc.begin();
  if (!g_rtcAvailable) Serial.println("RTC init failed — module not found");
  return g_rtcAvailable;
}

bool parsePhoneSyncValue(const String &value, DateTime &out) {
  int hour = 0, minute = 0, second = 0, day = 0, month = 0, year = 0;
  if (sscanf(value.c_str(), "%d:%d:%d,%d-%d-%d",
      &hour, &minute, &second, &day, &month, &year) != 6) return false;
  out = DateTime(year, month, day, hour, minute, second);
  return isDateTimeValid(out);
}

bool applyPhoneSync(const String &syncValue) {
  DateTime parsed(2000, 1, 1, 0, 0, 0);
  if (!parsePhoneSyncValue(syncValue, parsed)) {
    notifyTx("STATUS:TIME_SYNC_INVALID"); return false;
  }
  setSystemClock(parsed);
  saveTimeToRtc(parsed);
  saveTimeToNvs(parsed);
  g_timeNotSet         = false;
  g_timeSourceState    = TIME_SOURCE_PHONE_SYNC;
  g_lastRtcWriteBackMs = millis();
  g_lastNvsWriteBackMs = millis();
  // Force display to refresh immediately — clears the EST indicator
  g_lastDisplayedTimeText   = "";
  g_lastDisplayedStatusText = "";
  g_lastDisplayPlaceholder  = false;
  g_clockModeCycleStartMs   = millis();
  updateDisplayTime();
  notifyTx("STATUS:TIME_SYNCED");
  notifyTx(buildSystemInfoMessage());
  return true;
}

// ── Buttons ──────────────────────────────────────────────────

bool wasButtonPressed(ButtonState &button, uint32_t nowMs) {
  if (digitalRead(button.pin) != LOW) return false;
  if ((nowMs - button.lastPressMs) < BUTTON_DEBOUNCE_MS) return false;
  button.lastPressMs = nowMs;
  return true;
}

void serviceButtons() {
  const uint32_t now = millis();
  if (wasButtonPressed(g_modeButton,   now)) { triggerButtonClick(); handleVirtualButton("MODE");   }
  if (wasButtonPressed(g_nextButton,   now)) { triggerButtonClick(); handleVirtualButton("NEXT");   }
  if (wasButtonPressed(g_backButton,   now)) { triggerButtonClick(); handleVirtualButton("BACK");   }
  if (wasButtonPressed(g_selectButton, now)) { triggerButtonClick(); handleVirtualButton("SELECT"); }
}

void handleVirtualButton(const String &buttonName) {
  if (g_alarmRinging || g_timerFinished) {
    stopBuzzerAlerts();
    notifyTx("BTN:ALERT_STOP");
    return;
  }

  if (buttonName.equalsIgnoreCase("MODE")) {
    if (g_currentMode == DISPLAY_MODE_CLOCK &&
        g_activeClockTool == CLOCK_TOOL_ALARM && g_alarmEditMode) {
      g_alarmEditMode = false; updateClockToolDisplay(); notifyTx("BTN:MODE"); return;
    }
    if (g_currentMode == DISPLAY_MODE_CLOCK &&
        g_activeClockTool == CLOCK_TOOL_TIMER && g_timerEditMode) {
      g_timerEditMode          = false;
      g_timerRemainingSeconds  = g_timerTotalSeconds;
      g_timerConfigured        = g_timerTotalSeconds > 0;
      updateClockToolDisplay(); notifyTx("BTN:MODE"); return;
    }
    if (g_clockToolsMenuActive) {
      g_clockToolsMenuActive = false; updateClockToolDisplay(); notifyTx("BTN:MODE"); return;
    }
    g_menuActive       = !g_menuActive;
    g_pendingMenuMode  = g_currentMode;
    if (g_menuActive) showMenuSelection(); else applyDisplayMode(g_currentMode);
    notifyTx("BTN:MODE");
    return;
  }

  if (g_clockToolsMenuActive) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      g_pendingClockTool = static_cast<ClockTool>((g_pendingClockTool + 1) % 4);
      showClockToolSelection(); notifyTx("BTN:NEXT"); return;
    }
    if (buttonName.equalsIgnoreCase("BACK")) {
      g_pendingClockTool = static_cast<ClockTool>((g_pendingClockTool + 3) % 4);
      showClockToolSelection(); notifyTx("BTN:BACK"); return;
    }
    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      activateClockTool(g_pendingClockTool);
      if (g_timerTotalSeconds <= 0) g_timerConfigured = false;
      notifyTx("BTN:SELECT"); return;
    }
  }

  if (g_menuActive) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      g_pendingMenuMode = static_cast<DisplayMode>((g_pendingMenuMode + 1) % 8);
      showMenuSelection(); notifyTx("BTN:NEXT"); return;
    }
    if (buttonName.equalsIgnoreCase("BACK")) {
      g_pendingMenuMode = static_cast<DisplayMode>((g_pendingMenuMode + 7) % 8);
      showMenuSelection(); notifyTx("BTN:BACK"); return;
    }
    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      if (g_pendingMenuMode == DISPLAY_MODE_CLOCK) {
        g_menuActive           = false;
        g_clockToolsMenuActive = true;
        g_pendingClockTool     = g_activeClockTool;
        showClockToolSelection();
      } else {
        applyDisplayMode(g_pendingMenuMode);
      }
      notifyTx("BTN:SELECT"); return;
    }
  }

  if (g_currentMode == DISPLAY_MODE_SETTINGS) {
    if (buttonName.equalsIgnoreCase("NEXT")) {
      applyDisplayBrightness(clampDisplayBrightness(g_displayBrightness + 1));
      updateSettingsDisplay(); notifyTx("STATUS:BRIGHTNESS_UPDATED"); return;
    }
    if (buttonName.equalsIgnoreCase("BACK")) {
      applyDisplayBrightness(clampDisplayBrightness(static_cast<int>(g_displayBrightness) - 1));
      updateSettingsDisplay(); notifyTx("STATUS:BRIGHTNESS_UPDATED"); return;
    }
    if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
      notifyTx("BRIGHTNESS:" + String(g_displayBrightness)); return;
    }
  }

  if (g_currentMode == DISPLAY_MODE_CLOCK) {
    if (g_activeClockTool == CLOCK_TOOL_ALARM) {
      if (buttonName.equalsIgnoreCase("NEXT")) {
        if (g_alarmEditMode)
          g_alarmEditField == ALARM_EDIT_HOUR
            ? (g_alarmHour   = (g_alarmHour   + 1) % 24)
            : (g_alarmMinute = (g_alarmMinute + 1) % 60);
        else g_alarmMinute = (g_alarmMinute + 1) % 60;
        updateClockToolDisplay(); notifyTx("BTN:NEXT"); return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        if (g_alarmEditMode)
          g_alarmEditField == ALARM_EDIT_HOUR
            ? (g_alarmHour   = (g_alarmHour   + 23) % 24)
            : (g_alarmMinute = (g_alarmMinute + 59) % 60);
        else g_alarmMinute = (g_alarmMinute + 59) % 60;
        updateClockToolDisplay(); notifyTx("BTN:BACK"); return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        if (!g_alarmEditMode) {
          g_alarmEditMode  = true;
          g_alarmEditField = ALARM_EDIT_HOUR;
        } else if (g_alarmEditField == ALARM_EDIT_HOUR) {
          g_alarmEditField = ALARM_EDIT_MINUTE;
        } else {
          g_alarmEditMode = false;
          g_alarmEnabled  = !g_alarmEnabled;
          g_alarmRinging  = false;
          activateClockTool(CLOCK_TOOL_TIME);
        }
        updateClockToolDisplay(); notifyTx("BTN:SELECT"); return;
      }
    }

    if (g_activeClockTool == CLOCK_TOOL_TIMER) {
      if (buttonName.equalsIgnoreCase("NEXT") && !g_timerRunning) {
        if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES)
            g_timerTotalSeconds = min<int32_t>(g_timerTotalSeconds + 60, 99*60+59);
          else {
            const int32_t min = g_timerTotalSeconds / 60;
            g_timerTotalSeconds = min * 60 + (g_timerTotalSeconds % 60 + 1) % 60;
          }
        } else {
          g_timerTotalSeconds = min<int32_t>(g_timerTotalSeconds + 60, 99*60+59);
        }
        g_timerRemainingSeconds = g_timerTotalSeconds;
        g_timerFinished   = false;
        g_timerConfigured = g_timerTotalSeconds > 0;
        updateClockToolDisplay(); notifyTx("BTN:NEXT"); return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        if (g_timerRunning) {
          g_timerRunning = false;
        } else if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES)
            g_timerTotalSeconds = max<int32_t>(g_timerTotalSeconds - 60, 0);
          else {
            const int32_t min = g_timerTotalSeconds / 60;
            g_timerTotalSeconds = min * 60 + (g_timerTotalSeconds % 60 + 59) % 60;
          }
        } else {
          g_timerTotalSeconds = max<int32_t>(g_timerTotalSeconds - 60, 0);
        }
        g_timerRemainingSeconds = g_timerTotalSeconds;
        g_timerFinished   = false;
        g_timerConfigured = g_timerTotalSeconds > 0;
        updateClockToolDisplay(); notifyTx("BTN:BACK"); return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        if (g_timerFinished) {
          g_timerFinished         = false;
          g_timerRemainingSeconds = g_timerTotalSeconds;
          g_timerConfigured       = g_timerTotalSeconds > 0;
        } else if (!g_timerRunning && !g_timerEditMode && !g_timerConfigured) {
          g_timerEditMode  = true;
          g_timerEditField = TIMER_EDIT_MINUTES;
        } else if (g_timerEditMode) {
          if (g_timerEditField == TIMER_EDIT_MINUTES)
            g_timerEditField = TIMER_EDIT_SECONDS;
          else {
            g_timerEditMode         = false;
            g_timerRemainingSeconds = g_timerTotalSeconds;
            g_timerConfigured       = g_timerTotalSeconds > 0;
          }
        } else if (g_timerConfigured && g_timerRemainingSeconds > 0) {
          g_timerRunning    = !g_timerRunning;
          g_timerLastTickMs = millis();
        }
        updateClockToolDisplay(); notifyTx("BTN:SELECT"); return;
      }
    }

    if (g_activeClockTool == CLOCK_TOOL_STOPWATCH) {
      if (buttonName.equalsIgnoreCase("NEXT")) {
        if (g_stopwatchRunning) {
          g_stopwatchLapMs     = g_stopwatchElapsedMs;
          g_stopwatchLapFrozen = !g_stopwatchLapFrozen;
        } else {
          g_stopwatchElapsedMs = g_stopwatchLapMs = 0;
          g_stopwatchLapFrozen = false;
        }
        updateClockToolDisplay(); notifyTx("BTN:NEXT"); return;
      }
      if (buttonName.equalsIgnoreCase("BACK")) {
        g_stopwatchElapsedMs = g_stopwatchLapMs = 0;
        g_stopwatchLapFrozen = g_stopwatchRunning = false;
        updateClockToolDisplay(); notifyTx("BTN:BACK"); return;
      }
      if (buttonName.equalsIgnoreCase("SELECT") || buttonName.equalsIgnoreCase("OK")) {
        g_stopwatchRunning   = !g_stopwatchRunning;
        g_stopwatchLastTickMs= millis();
        updateClockToolDisplay(); notifyTx("BTN:SELECT"); return;
      }
    }
  }

  notifyTx("BTN:UNKNOWN");
}

// ── Buzzer ───────────────────────────────────────────────────

void serviceBuzzer() {
  const bool alertActive = g_alarmRinging || g_timerFinished;
  if (!alertActive) {
    if (g_buttonClickUntilMs > millis()) {
      if (!g_buzzerOutputOn) writeBuzzer(true);
      return;
    }
    if (g_buzzerOutputOn) writeBuzzer(false);
    return;
  }
  const uint32_t nowMs = millis();
  if ((nowMs - g_lastBuzzerToggleMs) >= BUZZER_BEEP_INTERVAL_MS) {
    g_lastBuzzerToggleMs = nowMs;
    writeBuzzer(!g_buzzerOutputOn);
  }
}

// ── Clock tools ──────────────────────────────────────────────

void serviceClockTools() {
  const uint32_t nowMs = millis();

  if (g_activeClockTool == CLOCK_TOOL_TIMER &&
      g_timerRunning && g_timerRemainingSeconds > 0) {
    if ((nowMs - g_timerLastTickMs) >= 1000) {
      const uint32_t elapsed    = (nowMs - g_timerLastTickMs) / 1000;
      g_timerLastTickMs        += elapsed * 1000;
      g_timerRemainingSeconds   = max<int32_t>(
        g_timerRemainingSeconds - static_cast<int32_t>(elapsed), 0);
      if (g_timerRemainingSeconds == 0) {
        g_timerRunning  = false;
        g_timerFinished = true;
      }
      updateClockToolDisplay();
    }
  }

  if (g_activeClockTool == CLOCK_TOOL_STOPWATCH && g_stopwatchRunning) {
    g_stopwatchElapsedMs  += nowMs - g_stopwatchLastTickMs;
    g_stopwatchLastTickMs  = nowMs;
    // FIX 9: cap display refresh to STOPWATCH_DISPLAY_INTERVAL_MS (200 ms = 5 Hz)
    if ((nowMs - g_lastStopwatchDisplayMs) >= STOPWATCH_DISPLAY_INTERVAL_MS) {
      g_lastStopwatchDisplayMs = nowMs;
      updateClockToolDisplay();
    }
  }

  // Bug 1 fix: alarm check runs regardless of active clock tool.
  // Previously gated on g_activeClockTool == CLOCK_TOOL_ALARM which meant
  // the alarm silently never fired when the user was in TIME/TIMER/STOPWATCH.
  if (g_alarmEnabled && !g_timeNotSet) {
    const DateTime now = getSystemDateTime();
    const uint32_t currentMinuteEpoch =
      static_cast<uint32_t>(now.unixtime() - static_cast<uint32_t>(now.second()));
    if (now.hour() == g_alarmHour && now.minute() == g_alarmMinute &&
        g_lastAlarmTriggerMinuteEpoch != currentMinuteEpoch) {
      g_alarmRinging              = true;
      g_lastAlarmTriggerMinuteEpoch = currentMinuteEpoch;
      updateClockToolDisplay();
      notifyTx("STATUS:ALARM_TRIGGERED");
    }
  }
}

// ── Periodic service loops ───────────────────────────────────

void serviceDisplayClock() {
  const uint32_t now = millis();
  if ((now - g_lastDisplayUpdateMs) < DISPLAY_UPDATE_INTERVAL_MS) return;
  g_lastDisplayUpdateMs = now;
  updateDisplayTime();
}

void serviceRtcWriteBack() {
  if (!g_rtcAvailable || g_timeNotSet || g_otaInProgress) return;
  const uint32_t now = millis();
  if ((now - g_lastRtcWriteBackMs) < RTC_WRITEBACK_INTERVAL_MS) return;
  g_lastRtcWriteBackMs = now;
  const DateTime current = getSystemDateTime();
  saveTimeToRtc(current);
  saveTimeToNvs(current);  // keep NVS in sync with every RTC writeback
}

void serviceNvsWriteBack() {
  // Writes time to NVS even when no RTC is present, so the NVS fallback
  // works on boards without a DS1307 module.
  if (g_rtcAvailable || g_timeNotSet || g_otaInProgress) return;
  const uint32_t now = millis();
  if ((now - g_lastNvsWriteBackMs) < NVS_WRITEBACK_INTERVAL_MS) return;
  saveTimeToNvs(getSystemDateTime());
}

// Bug 4 fix: weather NVS save deferred from BLE callback to main loop task.
void serviceWeatherNvsSave() {
  if (!g_weatherNvsSavePending) return;
  g_weatherNvsSavePending = false;
  saveWeatherToNvs();
}

void serviceBatteryUpdates() {
  if (g_otaInProgress) return;
  const uint32_t now = millis();
  if ((now - g_lastBatteryUpdateMs) < BATTERY_UPDATE_INTERVAL_MS) return;
  g_lastBatteryUpdateMs = now;
  updateBatteryCharacteristic(false);
}

void serviceSensorUpdates() {
  if (g_otaInProgress) return;
  const uint32_t now = millis();
  if ((now - g_lastSensorUpdateMs) < SENSOR_UPDATE_INTERVAL_MS) return;
  g_lastSensorUpdateMs = now;
  if (updateSensorReading()) notifyTx(buildSensorMessage());
}

void serviceOtaState() {
  if (g_otaInProgress && (millis() - g_otaLastActivityMs) > OTA_TIMEOUT_MS)
    abortOta("timeout");
  if (g_otaRebootPending && millis() >= g_otaRebootAtMs) {
    Serial.println("Rebooting into updated firmware");
    ESP.restart();
  }
}

// ── DextBot — MPU-6050 (accel + gyro) ───────────────────────

bool initMpu6050() {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(MPU6050_REG_PWR_MGMT);
  Wire.write(0x00);   // wake from sleep
  const uint8_t err = Wire.endTransmission();
  if (err != 0) {
    Serial.printf("MPU-6050 init failed (err %u) — AD0 must be HIGH for 0x69\n", err);
    return false;
  }
  // Set gyroscope full-scale range to ±250°/s (register 0x1B, bits 4:3 = 00)
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(0x1B);
  Wire.write(0x00);
  Wire.endTransmission();
  Serial.println("MPU-6050 ready at 0x69 (accel + gyro)");
  return true;
}

// Reads 6 bytes of accelerometer into g_dextAccelX/Y/Z
bool readAccel() {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(MPU6050_REG_ACCEL_XOUT);
  if (Wire.endTransmission(false) != 0) return false;
  Wire.requestFrom(MPU6050_ADDR, static_cast<uint8_t>(6));
  if (Wire.available() < 6) return false;
  g_dextAccelX = static_cast<int16_t>((Wire.read() << 8) | Wire.read());
  g_dextAccelY = static_cast<int16_t>((Wire.read() << 8) | Wire.read());
  g_dextAccelZ = static_cast<int16_t>((Wire.read() << 8) | Wire.read());
  return true;
}

// Reads 6 bytes of gyroscope into g_dextGyroX/Y/Z
// Also integrates Z-axis to track cumulative spin angle
bool readGyro() {
  Wire.beginTransmission(MPU6050_ADDR);
  Wire.write(MPU6050_REG_GYRO_XOUT);
  if (Wire.endTransmission(false) != 0) return false;
  Wire.requestFrom(MPU6050_ADDR, static_cast<uint8_t>(6));
  if (Wire.available() < 6) return false;
  g_dextGyroX = static_cast<int16_t>((Wire.read() << 8) | Wire.read());
  g_dextGyroY = static_cast<int16_t>((Wire.read() << 8) | Wire.read());
  g_dextGyroZ = static_cast<int16_t>((Wire.read() << 8) | Wire.read());

  // Integrate Z-axis spin angle (°) using elapsed time
  const uint32_t now = millis();
  if (g_lastGyroReadMs > 0) {
    const float dtSec = static_cast<float>(now - g_lastGyroReadMs) / 1000.0f;
    // 131 LSB per °/s at ±250°/s range
    g_gyroAngleZ += (static_cast<float>(g_dextGyroZ) / 131.0f) * dtSec;
    // Wrap to ±360
    if (g_gyroAngleZ >  360.0f) g_gyroAngleZ -= 360.0f;
    if (g_gyroAngleZ < -360.0f) g_gyroAngleZ += 360.0f;
  }
  g_lastGyroReadMs = now;
  return true;
}

// ── DextBot — VL53L0X ────────────────────────────────────────

bool initVl53() {
  if (!g_vl53.begin()) {
    Serial.println("VL53L0X init failed — check wiring");
    return false;
  }
  g_vl53.startRangeContinuous();
  Serial.println("VL53L0X ready");
  return true;
}

bool readVl53() {
  if (!g_vl53.isRangeComplete()) return false;
  VL53L0X_RangingMeasurementData_t m;
  g_vl53.getRangingMeasurement(&m, false);
  if (m.RangeStatus != 4) g_dextDistMm = m.RangeMilliMeter;
  return true;
}

// ── DextBot — cute gyro-enhanced robo-eye renderer ───────────
//
// Display: 32 cols × 8 rows. setColumn(col, byte): bit7=row0(top).
// Left eye: col 8  (spans 5–11)
// Right eye: col 23 (spans 20–26)
//
// ─── Expressions (VL53L0X distance) ─────────────────────────
//  SCARED    < 80 mm    Huge circles, frozen pupils (deer-in-headlights)
//  SURPRISE  80–200 mm  Wide open circles, pupils dart up
//  CURIOUS   200–400 mm Rounded ovals, pupils converge inward
//  HAPPY     400–700 mm Smile-edge eyes, tilt+gyro pupils
//  SLEEPY    > 700 mm   Droopy half-close, pupils wander lazily
//
// ─── Gyroscope special states (override expression) ─────────
//  DIZZY    Z-spin > GYRO_THRESH_SPIN     Spiral pupils orbiting center
//  BONK     XY shake > GYRO_THRESH_SHAKE  X-eyes (stars-after-hit)
//  SQUINT   Gentle sustained rotation     Pupils pulled by inertia lag
//
// ─── Pupil inertia ───────────────────────────────────────────
//  Gyro rate scales the lerp speed of pupils toward their tilt target.
//  Fast movement → pupils lag (inertia feel).
//  Slow / still  → pupils snap to tilt position quickly.
//
// ─── Blink ───────────────────────────────────────────────────
//  Eyelid descends from top. Rate adapts: SCARED = fast, SLEEPY = rare.
//  DIZZY / BONK suppress blinking.

enum DextbotExpr {
  DEXTBOT_SCARED   = 0,
  DEXTBOT_SURPRISE = 1,
  DEXTBOT_CURIOUS  = 2,
  DEXTBOT_HAPPY    = 3,
  DEXTBOT_SLEEPY   = 4,
  DEXTBOT_DIZZY    = 5,   // gyro: spinning fast
  DEXTBOT_BONK     = 6,   // gyro: sudden shake/impact
};

// 7-column eye shapes. index 0 = center-3, index 6 = center+3.
// bit7 = row 0 (top), bit0 = row 7 (bottom).
static const uint8_t kEyeShape[7][7] = {
  // SCARED — huge wide-open circles
  { 0x7E, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7E },
  // SURPRISE — open circle, slightly smaller than scared
  { 0x3C, 0x7E, 0xFF, 0xFF, 0xFF, 0x7E, 0x3C },
  // CURIOUS — rounded compact oval
  { 0x3C, 0x7E, 0x7E, 0x7E, 0x7E, 0x7E, 0x3C },
  // HAPPY — curved smile-bottom (cuter than a plain oval)
  { 0x38, 0x7C, 0x7E, 0x7E, 0x7E, 0x7C, 0x38 },
  // SLEEPY — droopy half-close (lower 3 rows)
  { 0x04, 0x0E, 0x1E, 0x1E, 0x1E, 0x0E, 0x04 },
  // DIZZY — full-open same as SURPRISE (pupils will orbit)
  { 0x3C, 0x7E, 0xFF, 0xFF, 0xFF, 0x7E, 0x3C },
  // BONK — squinted X-eye shape (narrow band)
  { 0x00, 0x18, 0x3C, 0x3C, 0x3C, 0x18, 0x00 },
};

// Pupil size: {pw, ph}. 0 = no pupil.
static const uint8_t kPupilW[7] = { 1, 2, 2, 2, 0, 2, 0 };
static const uint8_t kPupilH[7] = { 1, 2, 2, 2, 0, 2, 0 };

// ── Internal state ────────────────────────────────────────────
// Smooth pupil position (float for lerp)
static float    g_pupilX          = 0.0f;  // current rendered pupil X
static float    g_pupilY          = 0.0f;  // current rendered pupil Y
static float    g_pupilTargetX    = 0.0f;
static float    g_pupilTargetY    = 0.0f;
// Dizzy orbit
static float    g_dizzyAngle      = 0.0f;
// Sleepy wander
static float    g_wanderX         = 0.0f;
static float    g_wanderY         = 0.0f;
static float    g_wanderTargetX   = 0.0f;
static float    g_wanderTargetY   = 0.0f;
static uint32_t g_wanderNextMs    = 0;
// Expression / special state timing
static DextbotExpr g_dextExpr     = DEXTBOT_HAPPY;
static DextbotExpr g_baseExpr     = DEXTBOT_HAPPY;  // before gyro override
static uint32_t g_specialUntilMs  = 0;   // DIZZY / BONK hold timer
static uint32_t g_dextNextBlinkMs = 0;
// Bonk X pattern phase
static uint8_t  g_bonkPhase       = 0;
static uint32_t g_bonkPhaseMs     = 0;

// ── Helpers ───────────────────────────────────────────────────

DextbotExpr distanceToExpr(uint16_t distMm) {
  if (distMm < DEXTBOT_DIST_SCARED)   return DEXTBOT_SCARED;
  if (distMm < DEXTBOT_DIST_SURPRISE) return DEXTBOT_SURPRISE;
  if (distMm < DEXTBOT_DIST_CURIOUS)  return DEXTBOT_CURIOUS;
  if (distMm < DEXTBOT_DIST_HAPPY)    return DEXTBOT_HAPPY;
  return DEXTBOT_SLEEPY;
}

// Gyro magnitude across X and Y axes (shake = lateral jolt)
int32_t gyroShakeMag() {
  return static_cast<int32_t>(abs(g_dextGyroX)) +
         static_cast<int32_t>(abs(g_dextGyroY));
}

// Gyro magnitude on Z axis (spin around vertical)
int32_t gyroSpinMag() {
  return static_cast<int32_t>(abs(g_dextGyroZ));
}

// Draw one eye with pupil. expr selects shape + size.
// eyelidRows: 0 = fully open, 8 = fully closed (top-down).
// pupilDX/DY: pixel offset of pupil from eye center column.
//
// Column direction note: FC16 hardware has column 0 on the RIGHT side of the
// display (same reversal the visualizer compensates with 31-i). We mirror
// here with (31 - col) so that:
//   DEXTBOT_LEFT_COL  (8)  → physical col 23 = left side ✓
//   DEXTBOT_RIGHT_COL (23) → physical col  8 = right side ✓
void drawEye(MD_MAX72XX *gfx,
             uint8_t centerCol, DextbotExpr expr,
             uint8_t eyelidRows, int8_t pupilDX, int8_t pupilDY) {

  const uint8_t *shape = kEyeShape[static_cast<int>(expr)];
  const uint8_t  pw    = kPupilW[static_cast<int>(expr)];
  const uint8_t  ph    = kPupilH[static_cast<int>(expr)];
  const int8_t   pdx   = static_cast<int8_t>(constrain(static_cast<int>(pupilDX), -2, 1));
  const int8_t   pdy   = static_cast<int8_t>(constrain(static_cast<int>(pupilDY), -2, 1));

  for (int8_t offset = -3; offset <= 3; ++offset) {
    const int col = static_cast<int>(centerCol) + offset;
    if (col < 0 || col > 31) continue;

    uint8_t colByte = shape[offset + 3];

    // Eyelid: right-shift pushes pixels downward, top rows go dark
    if (eyelidRows >= 8) {
      colByte = 0x00;
    } else if (eyelidRows > 0) {
      colByte = static_cast<uint8_t>(colByte >> eyelidRows);
    }

    // Pupil cutout
    if (pw > 0 && ph > 0) {
      for (uint8_t pc = 0; pc < pw; ++pc) {
        if (offset == static_cast<int8_t>(pdx + static_cast<int8_t>(pc))) {
          for (uint8_t pr = 0; pr < ph; ++pr) {
            const int row = 3 + static_cast<int>(pdy) + static_cast<int>(pr);
            if (row >= 0 && row <= 7)
              colByte &= static_cast<uint8_t>(~(0x80u >> row));
          }
        }
      }
    }

    // Mirror column to match FC16 hardware direction (same as visualizer's 31-i)
    gfx->setColumn(static_cast<uint16_t>(31 - col), colByte);
  }
}

// Draw BONK X-eye: flashing × pattern on each eye (no pupils)
void drawBonkEye(MD_MAX72XX *gfx, uint8_t centerCol, uint8_t phase) {
  static const uint8_t kX0[7] = { 0x80,0x60,0x18,0x08,0x18,0x60,0x80 };
  static const uint8_t kX1[7] = { 0x02,0x0C,0x30,0x40,0x30,0x0C,0x02 };
  const uint8_t *pat = (phase == 0) ? kX0 : kX1;
  for (int8_t offset = -3; offset <= 3; ++offset) {
    const int col = static_cast<int>(centerCol) + offset;
    if (col >= 0 && col <= 31)
      gfx->setColumn(static_cast<uint16_t>(31 - col), pat[offset + 3]);
  }
}

void renderDextbotFrame() {
  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (!gfx) return;

  gfx->clear();
  const uint32_t now = millis();

  // ── Step 1: base expression from VL53L0X ────────────────
  g_baseExpr = g_vl53Available ? distanceToExpr(g_dextDistMm) : DEXTBOT_HAPPY;

  // ── Step 2: gyro override ────────────────────────────────
  //  Priority: BONK > DIZZY > base expression
  //  Each override latches for DEXTBOT_BONK_MS / DEXTBOT_DIZZY_MS
  //  then reverts to whatever the VL53 says.
  if (g_mpu6050Available) {
    const int32_t shake = gyroShakeMag();
    const int32_t spin  = gyroSpinMag();

    // Ignore gyro thresholds during the settle period — MPU-6050 outputs
    // large noise for ~200 ms after power-on which falsely triggers BONK.
    if (millis() < g_dextGyroSettleUntilMs) {
      g_dextExpr = g_baseExpr;
    } else if (shake > GYRO_THRESH_SHAKE) {
      g_dextExpr       = DEXTBOT_BONK;
      g_specialUntilMs = now + DEXTBOT_BONK_MS;
    } else if (spin > GYRO_THRESH_SPIN) {
      // Bug 11 fix: original check was (g_dextExpr != DEXTBOT_BONK) but
      // when BONK has just expired the expr is still DEXTBOT_BONK until
      // the else-if below runs — meaning DIZZY could never trigger right
      // after a BONK. Now check the timer directly instead.
      if (now < g_specialUntilMs && g_dextExpr == DEXTBOT_BONK) {
        // BONK still active — don't interrupt it
      } else {
        g_dextExpr       = DEXTBOT_DIZZY;
        g_specialUntilMs = now + DEXTBOT_DIZZY_MS;
      }
    } else if (now >= g_specialUntilMs) {
      g_dextExpr = g_baseExpr;
    }
  } else {
    g_dextExpr = g_baseExpr;
  }

  // ── Step 3: blink eyelid ─────────────────────────────────
  uint8_t eyelidRows = 0;
  const bool suppressBlink = (g_dextExpr == DEXTBOT_DIZZY ||
                               g_dextExpr == DEXTBOT_BONK);
  if (!suppressBlink && g_dextBlinking) {
    const uint32_t elapsed = now - g_dextBlinkStartMs;
    if (elapsed >= DEXTBOT_BLINK_DURATION_MS) {
      g_dextBlinking = false;
    } else {
      const uint8_t  maxClose = (g_dextExpr == DEXTBOT_SLEEPY) ? 3u : 8u;
      const uint32_t half     = DEXTBOT_BLINK_DURATION_MS / 2;
      eyelidRows = (elapsed < half)
        ? static_cast<uint8_t>(map(static_cast<long>(elapsed),
            0L, static_cast<long>(half), 0L, static_cast<long>(maxClose)))
        : static_cast<uint8_t>(map(static_cast<long>(elapsed),
            static_cast<long>(half), static_cast<long>(DEXTBOT_BLINK_DURATION_MS),
            static_cast<long>(maxClose), 0L));
    }
  }

  // ── Step 4: pupil position ───────────────────────────────

  int8_t finalPupilDX = 0;
  int8_t finalPupilDY = 0;

  if (g_dextExpr == DEXTBOT_BONK) {
    // X-eye flash — alternate pattern every 120 ms, no pupils
    if ((now - g_bonkPhaseMs) >= 120) {
      g_bonkPhase  = (g_bonkPhase + 1) % 2;
      g_bonkPhaseMs = now;
    }
    drawBonkEye(gfx, DEXTBOT_LEFT_COL,  g_bonkPhase);
    drawBonkEye(gfx, DEXTBOT_RIGHT_COL, g_bonkPhase);
    g_matrix.displayAnimate();
    return;  // early out — skip normal drawEye path

  } else if (g_dextExpr == DEXTBOT_DIZZY) {
    // Bug 12 fix: orbit speed was scaled by DEXTBOT_FRAME_MS constant (40ms)
    // but the actual frame interval varies. Use real elapsed time instead.
    const float dtSec    = static_cast<float>(now - g_dextLastFrameMs + 1) / 1000.0f;
    const float spinDeg  = static_cast<float>(abs(g_dextGyroZ)) / 131.0f;
    g_dizzyAngle += spinDeg * dtSec * 2.5f;
    if (g_dizzyAngle > 360.0f) g_dizzyAngle -= 360.0f;
    finalPupilDX = static_cast<int8_t>(1.4f * cosf(g_dizzyAngle * 0.01745f));
    finalPupilDY = static_cast<int8_t>(1.0f * sinf(g_dizzyAngle * 0.01745f));

  } else if (g_dextExpr == DEXTBOT_SLEEPY) {
    // Idle wander — lazy exponential drift
    if (now >= g_wanderNextMs) {
      g_wanderTargetX  = static_cast<float>(random(5) - 2);
      g_wanderTargetY  = static_cast<float>(random(3) - 1);
      g_wanderNextMs   = now + 1200UL + random(1800);
    }
    g_wanderX += (g_wanderTargetX - g_wanderX) * 0.05f;
    g_wanderY += (g_wanderTargetY - g_wanderY) * 0.05f;
    finalPupilDX = static_cast<int8_t>(g_wanderX);
    finalPupilDY = static_cast<int8_t>(g_wanderY);

  } else if (g_mpu6050Available) {
    // Tilt target from accelerometer
    g_pupilTargetX = static_cast<float>(
      map(static_cast<long>(g_dextAccelY), -16384L, 16384L, -2L, 2L));
    g_pupilTargetY = static_cast<float>(
      map(static_cast<long>(g_dextAccelX), -16384L, 16384L, -1L, 1L));

    // Gyro-driven inertia: fast spin/shake → slow lerp (pupils lag behind)
    // Calm → fast snap to tilt position
    const float gyroMag  = static_cast<float>(
      abs(g_dextGyroX) + abs(g_dextGyroY) + abs(g_dextGyroZ));
    // Map gyro mag to lerp factor: calm=0.35 (snappy), fast=0.04 (laggy)
    const float lerpSpeed = constrain(
      0.35f - (gyroMag / 200000.0f) * 0.31f, 0.04f, 0.35f);

    g_pupilX += (g_pupilTargetX - g_pupilX) * lerpSpeed;
    g_pupilY += (g_pupilTargetY - g_pupilY) * lerpSpeed;

    // Gentle gyro Z rotation adds a subtle spin pull to pupils
    if (abs(g_dextGyroZ) > GYRO_THRESH_GENTLE) {
      const float spinPull = static_cast<float>(g_dextGyroZ) / 65536.0f * 3.0f;
      g_pupilX = constrain(g_pupilX + spinPull, -2.0f, 2.0f);
    }

    // Expression overrides
    switch (g_dextExpr) {
      case DEXTBOT_SCARED:
        // Frozen centered pupils regardless of tilt
        g_pupilX = g_pupilX * 0.8f;  // drift toward 0
        g_pupilY = g_pupilY * 0.8f - 0.3f;  // nudge upward
        break;
      case DEXTBOT_SURPRISE:
        g_pupilY -= 0.5f;  // pupils look upward
        break;
      case DEXTBOT_CURIOUS:
        g_pupilX *= 0.4f;  // converge inward — looking at the object
        break;
      default:
        break;
    }

    finalPupilDX = static_cast<int8_t>(g_pupilX);
    finalPupilDY = static_cast<int8_t>(g_pupilY);
  }

  drawEye(gfx, DEXTBOT_LEFT_COL,  g_dextExpr, eyelidRows, finalPupilDX, finalPupilDY);
  drawEye(gfx, DEXTBOT_RIGHT_COL, g_dextExpr, eyelidRows, finalPupilDX, finalPupilDY);

  g_matrix.displayAnimate();
}

void serviceDextbot() {
  if (g_currentMode != DISPLAY_MODE_DEXTBOT) return;
  // Don't render eyes while any menu is open — the menu label is on the
  // display and rendering eyes would overwrite it every 40 ms.
  if (g_menuActive || g_clockToolsMenuActive) return;

  const uint32_t now = millis();

  if (g_mpu6050Available) {
    readAccel();
    readGyro();
  }
  if (g_vl53Available) readVl53();

  // Schedule next blink — rate adapts to expression
  // DIZZY/BONK suppress blinking entirely
  const bool suppressBlink = (g_dextExpr == DEXTBOT_DIZZY ||
                               g_dextExpr == DEXTBOT_BONK);
  if (!suppressBlink && !g_dextBlinking && now >= g_dextNextBlinkMs) {
    g_dextBlinking     = true;
    g_dextBlinkStartMs = now;
    uint32_t interval;
    switch (g_dextExpr) {
      case DEXTBOT_SCARED:   interval = 800UL  + random(700);  break; // nervous
      case DEXTBOT_SURPRISE: interval = 1200UL + random(800);  break;
      case DEXTBOT_SLEEPY:   interval = 4000UL + random(3000); break; // drowsy
      default:               interval = 2000UL + random(2000); break;
    }
    g_dextNextBlinkMs = now + interval;
  }

  if ((now - g_dextLastFrameMs) >= DEXTBOT_FRAME_MS) {
    g_dextLastFrameMs = now;
    renderDextbotFrame();
  }
}

void initDextbot() {
  g_mpu6050Available = initMpu6050();
  g_vl53Available    = initVl53();

  if (!g_mpu6050Available)
    Serial.println("DextBot: no MPU-6050 — pupils fixed, no gyro effects");
  if (!g_vl53Available)
    Serial.println("DextBot: no VL53L0X — expression fixed at HAPPY");

  g_dextNextBlinkMs       = millis() + 1500;
  g_wanderNextMs          = millis() + 2000;
  g_lastGyroReadMs        = 0;
  // MPU-6050 outputs large noise values for ~200 ms after power-on.
  // Block gyro threshold checks until the sensor has settled.
  g_dextGyroSettleUntilMs = millis() + 500;
  randomSeed(esp_random());
}


// ═══════════════════════════════════════════════════════════════
//  EXPERIMENTAL FEATURES
//  All three use the MPU-6050 accelerometer for tilt control.
//  Exit any experimental mode by pressing the MODE button.
//  These are marked experimental because they depend on the
//  MPU-6050 being wired and working correctly.
// ═══════════════════════════════════════════════════════════════

// ── Shared tilt helper ───────────────────────────────────────
// Returns tilt-mapped column position (0–31) from accelY.
// Used by all three games/features.
int8_t tiltToColumn(int8_t minCol, int8_t maxCol) {
  const int8_t col = static_cast<int8_t>(
    map(static_cast<long>(g_dextAccelY),
        -14000L, 14000L,
        static_cast<long>(minCol),
        static_cast<long>(maxCol)));
  return static_cast<int8_t>(constrain(static_cast<int>(col),
         static_cast<int>(minCol), static_cast<int>(maxCol)));
}

// ══════════════════════════════════════════════════════════════
//  PONG — two-player tilt game
//  Left player: tilt left/right to move paddle (accelY)
//  Right player (AI): follows ball with a slight lag
//  Ball bounces off top/bottom walls and paddles
//  Score shown briefly on display. First to 9 wins.
// ══════════════════════════════════════════════════════════════

constexpr uint8_t  PONG_PADDLE_H    = 3;    // paddle height in rows
constexpr uint32_t PONG_FRAME_MS    = 80;   // ~12 Hz
constexpr uint32_t PONG_SCORE_MS    = 1500; // show score for 1.5 s
constexpr uint8_t  PONG_WIN_SCORE   = 9;

struct PongState {
  float  ballX   = 16.0f;
  float  ballY   = 3.5f;
  float  ballDX  = 0.6f;   // cols per frame
  float  ballDY  = 0.4f;
  int8_t padL    = 2;      // left paddle top row (col 1)
  int8_t padR    = 2;      // right paddle top row (col 30)
  uint8_t scoreL = 0;
  uint8_t scoreR = 0;
  bool   showScore = false;
  uint32_t scoreUntilMs = 0;
  uint32_t lastFrameMs  = 0;
};
static PongState g_pong;

void initPong() {
  g_pong = PongState{};
  g_pong.ballDX = (random(2) ? 0.6f : -0.6f);
  g_pong.lastFrameMs = millis();
}

void drawPong(MD_MAX72XX *gfx) {
  gfx->clear();

  // Draw ball (single pixel)
  const int bx = constrain(static_cast<int>(g_pong.ballX), 0, 31);
  const int by = constrain(static_cast<int>(g_pong.ballY), 0, 7);
  // setPoint(row, col) — col mirrored for FC16
  gfx->setPoint(static_cast<uint8_t>(by), static_cast<uint16_t>(31 - bx), true);

  // Draw left paddle (col 1)
  for (int8_t r = 0; r < PONG_PADDLE_H; ++r) {
    const int row = static_cast<int>(g_pong.padL) + r;
    if (row >= 0 && row <= 7)
      gfx->setPoint(static_cast<uint8_t>(row), static_cast<uint16_t>(31 - 1), true);
  }

  // Draw right paddle (col 30)
  for (int8_t r = 0; r < PONG_PADDLE_H; ++r) {
    const int row = static_cast<int>(g_pong.padR) + r;
    if (row >= 0 && row <= 7)
      gfx->setPoint(static_cast<uint8_t>(row), static_cast<uint16_t>(31 - 30), true);
  }

  g_matrix.displayAnimate();
}

void servicePong() {
  if (g_currentMode != DISPLAY_MODE_PONG) return;
  if (g_menuActive || g_clockToolsMenuActive) return;

  const uint32_t now = millis();

  // Show score screen between points
  if (g_pong.showScore) {
    if (now >= g_pong.scoreUntilMs) {
      g_pong.showScore = false;
      // Check win condition
      if (g_pong.scoreL >= PONG_WIN_SCORE || g_pong.scoreR >= PONG_WIN_SCORE) {
        // Show winner then return to clock
        char buf[8];
        snprintf(buf, sizeof(buf), g_pong.scoreL >= PONG_WIN_SCORE ? "L WIN" : "R WIN");
        g_matrix.displayClear();
        g_matrix.displayText(buf, PA_CENTER, 50, 0, PA_SCROLL_LEFT, PA_SCROLL_LEFT);
        g_matrix.displayReset();
        delay(2000);
        applyDisplayMode(DISPLAY_MODE_CLOCK);
        return;
      }
      // Reset ball
      g_pong.ballX  = 16.0f;
      g_pong.ballY  = 3.5f;
      g_pong.ballDX = (random(2) ? 0.6f : -0.6f);
      g_pong.ballDY = (random(2) ? 0.4f : -0.4f);
    }
    return;
  }

  if ((now - g_pong.lastFrameMs) < PONG_FRAME_MS) return;
  g_pong.lastFrameMs = now;

  // Player paddle: tilt accelY → row 0..5
  if (g_mpu6050Available) {
    readAccel();
    g_pong.padL = static_cast<int8_t>(
      map(static_cast<long>(g_dextAccelY), -14000L, 14000L, 0L, 5L));
  }

  // AI paddle: follow ball with lag
  const float aiTarget = g_pong.ballY - PONG_PADDLE_H / 2.0f;
  if (g_pong.padR < aiTarget) g_pong.padR = static_cast<int8_t>(min<int>(g_pong.padR + 1, 5));
  else if (g_pong.padR > aiTarget) g_pong.padR = static_cast<int8_t>(max<int>(g_pong.padR - 1, 0));

  // Move ball
  g_pong.ballX += g_pong.ballDX;
  g_pong.ballY += g_pong.ballDY;

  // Top/bottom wall bounce
  if (g_pong.ballY <= 0.0f) { g_pong.ballY = 0.0f; g_pong.ballDY = fabsf(g_pong.ballDY); }
  if (g_pong.ballY >= 7.0f) { g_pong.ballY = 7.0f; g_pong.ballDY = -fabsf(g_pong.ballDY); }

  // Left paddle collision (col 1)
  if (g_pong.ballX <= 2.0f && g_pong.ballDX < 0.0f) {
    const float ballRow = g_pong.ballY;
    if (ballRow >= g_pong.padL && ballRow <= g_pong.padL + PONG_PADDLE_H) {
      g_pong.ballDX = fabsf(g_pong.ballDX) * 1.05f;  // slight speed up
      // Angle based on hit position
      const float rel = (ballRow - g_pong.padL) / PONG_PADDLE_H - 0.5f;
      g_pong.ballDY = rel * 0.8f;
    }
  }

  // Right paddle collision (col 30)
  if (g_pong.ballX >= 29.0f && g_pong.ballDX > 0.0f) {
    const float ballRow = g_pong.ballY;
    if (ballRow >= g_pong.padR && ballRow <= g_pong.padR + PONG_PADDLE_H) {
      g_pong.ballDX = -fabsf(g_pong.ballDX) * 1.05f;
      const float rel = (ballRow - g_pong.padR) / PONG_PADDLE_H - 0.5f;
      g_pong.ballDY = rel * 0.8f;
    }
  }

  // Clamp ball speed
  g_pong.ballDX = constrain(g_pong.ballDX, -1.4f, 1.4f);
  g_pong.ballDY = constrain(g_pong.ballDY, -0.9f, 0.9f);

  // Scoring — ball exits left or right
  if (g_pong.ballX < 0.0f) {
    g_pong.scoreR++;
    char buf[6]; snprintf(buf, sizeof(buf), "%u-%u", g_pong.scoreL, g_pong.scoreR);
    renderDisplayText(String(buf));
    g_pong.showScore    = true;
    g_pong.scoreUntilMs = now + PONG_SCORE_MS;
    notifyTx("PONG:SCORE," + String(g_pong.scoreL) + "," + String(g_pong.scoreR));
    return;
  }
  if (g_pong.ballX > 31.0f) {
    g_pong.scoreL++;
    char buf[6]; snprintf(buf, sizeof(buf), "%u-%u", g_pong.scoreL, g_pong.scoreR);
    renderDisplayText(String(buf));
    g_pong.showScore    = true;
    g_pong.scoreUntilMs = now + PONG_SCORE_MS;
    notifyTx("PONG:SCORE," + String(g_pong.scoreL) + "," + String(g_pong.scoreR));
    return;
  }

  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (gfx) drawPong(gfx);
}

// ══════════════════════════════════════════════════════════════
//  DODGE — tilt to dodge falling pixels
//  Player is a 3-wide block on the bottom row.
//  Tilt left/right to move. Falling pixels drop from the top.
//  Speed increases over time. Hit = game over. Score = time survived.
// ══════════════════════════════════════════════════════════════

constexpr uint8_t  DODGE_MAX_DROPS  = 8;
constexpr uint32_t DODGE_FRAME_MS   = 120;
constexpr uint32_t DODGE_GAMEOVER_MS= 2000;

struct DodgeDrop {
  int8_t  col  = 0;
  float   row  = 0.0f;
  float   speed= 0.0f;
  bool    active = false;
};

struct DodgeState {
  int8_t     playerCol = 14;           // center col of 3-wide player
  DodgeDrop  drops[DODGE_MAX_DROPS];
  uint32_t   lastFrameMs   = 0;
  uint32_t   lastSpawnMs   = 0;
  uint32_t   spawnInterval = 1200;     // ms between spawns
  uint32_t   startMs       = 0;
  uint32_t   scoreSeconds  = 0;
  bool       gameOver      = false;
  uint32_t   gameOverUntilMs = 0;
};
static DodgeState g_dodge;

void initDodge() {
  g_dodge = DodgeState{};
  g_dodge.lastFrameMs   = millis();
  g_dodge.lastSpawnMs   = millis();
  g_dodge.startMs       = millis();
}

void serviceDodge() {
  if (g_currentMode != DISPLAY_MODE_DODGE) return;
  if (g_menuActive || g_clockToolsMenuActive) return;

  const uint32_t now = millis();

  // Game over screen
  if (g_dodge.gameOver) {
    if (now >= g_dodge.gameOverUntilMs) {
      applyDisplayMode(DISPLAY_MODE_CLOCK);
    }
    return;
  }

  if ((now - g_dodge.lastFrameMs) < DODGE_FRAME_MS) return;
  g_dodge.lastFrameMs = now;

  // Update score
  g_dodge.scoreSeconds = (now - g_dodge.startMs) / 1000;

  // Player movement from tilt
  if (g_mpu6050Available) {
    readAccel();
    g_dodge.playerCol = tiltToColumn(1, 29);
  }

  // Spawn new drops
  if ((now - g_dodge.lastSpawnMs) >= g_dodge.spawnInterval) {
    g_dodge.lastSpawnMs   = now;
    g_dodge.spawnInterval = max<uint32_t>(400, g_dodge.spawnInterval - 20); // speed up
    for (uint8_t i = 0; i < DODGE_MAX_DROPS; ++i) {
      if (!g_dodge.drops[i].active) {
        g_dodge.drops[i].col    = static_cast<int8_t>(random(2, 30));
        g_dodge.drops[i].row    = 0.0f;
        g_dodge.drops[i].speed  = 0.3f + static_cast<float>(random(30)) / 100.0f;
        g_dodge.drops[i].active = true;
        break;
      }
    }
  }

  // Move drops and check collision
  bool hit = false;
  for (uint8_t i = 0; i < DODGE_MAX_DROPS; ++i) {
    if (!g_dodge.drops[i].active) continue;
    g_dodge.drops[i].row += g_dodge.drops[i].speed;
    if (g_dodge.drops[i].row >= 8.0f) {
      g_dodge.drops[i].active = false;
      continue;
    }
    // Collision: drop at row 7 hits player
    if (static_cast<int>(g_dodge.drops[i].row) >= 6) {
      const int dc = static_cast<int>(g_dodge.drops[i].col);
      const int pc = static_cast<int>(g_dodge.playerCol);
      if (dc >= pc - 1 && dc <= pc + 1) { hit = true; break; }
    }
  }

  if (hit) {
    // Game over
    g_dodge.gameOver        = true;
    g_dodge.gameOverUntilMs = now + DODGE_GAMEOVER_MS;
    char buf[12];
    snprintf(buf, sizeof(buf), "HIT %lus", static_cast<unsigned long>(g_dodge.scoreSeconds));
    g_matrix.displayClear();
    g_matrix.displayText(buf, PA_CENTER, 50, 0, PA_SCROLL_LEFT, PA_SCROLL_LEFT);
    g_matrix.displayReset();
    notifyTx("DODGE:GAMEOVER," + String(g_dodge.scoreSeconds));
    return;
  }

  // Draw
  MD_MAX72XX *gfx = g_matrix.getGraphicObject();
  if (!gfx) return;
  gfx->clear();

  // Player — 3 pixels wide on bottom row (row 7)
  for (int8_t dx = -1; dx <= 1; ++dx) {
    const int pc = static_cast<int>(g_dodge.playerCol) + dx;
    if (pc >= 0 && pc <= 31)
      gfx->setPoint(7, static_cast<uint16_t>(31 - pc), true);
  }

  // Drops
  for (uint8_t i = 0; i < DODGE_MAX_DROPS; ++i) {
    if (!g_dodge.drops[i].active) continue;
    const int dr = static_cast<int>(g_dodge.drops[i].row);
    const int dc = static_cast<int>(g_dodge.drops[i].col);
    if (dr >= 0 && dr <= 7 && dc >= 0 && dc <= 31)
      gfx->setPoint(static_cast<uint8_t>(dr), static_cast<uint16_t>(31 - dc), true);
  }

  g_matrix.displayAnimate();
}

// ══════════════════════════════════════════════════════════════
//  GESTURE — tilt directions trigger named actions over BLE
//  The user tilts the device and the display shows the gesture
//  name. BLE notifies the app so it can automate actions.
//
//  Gestures:
//   Tilt LEFT     → sends GESTURE:LEFT   → display "LEFT"
//   Tilt RIGHT    → sends GESTURE:RIGHT  → display "RGHT"
//   Tilt FORWARD  → sends GESTURE:FWD    → display "FWD"
//   Tilt BACK     → sends GESTURE:BACK   → display "BACK"
//   Tilt UP (Z)   → sends GESTURE:UP     → display "UP"
//   Shake         → sends GESTURE:SHAKE  → display "SHKE"
//
//  Each gesture has a hold threshold and a cooldown so it
//  doesn't fire repeatedly while held.
// ══════════════════════════════════════════════════════════════

constexpr int32_t  GESTURE_TILT_THRESH  = 9000;   // ~55° tilt
constexpr int32_t  GESTURE_SHAKE_THRESH = 18000;   // gyro XY magnitude
constexpr uint32_t GESTURE_COOLDOWN_MS  = 800;
constexpr uint32_t GESTURE_SHOW_MS      = 600;
constexpr uint32_t GESTURE_FRAME_MS     = 50;

struct GestureState {
  uint32_t lastGestureMs  = 0;
  uint32_t showUntilMs    = 0;
  char     lastLabel[6]   = {};
  uint32_t lastFrameMs    = 0;
};
static GestureState g_gesture;

void initGesture() {
  g_gesture = GestureState{};
}

void serviceGesture() {
  if (g_currentMode != DISPLAY_MODE_GESTURE) return;
  if (g_menuActive || g_clockToolsMenuActive) return;

  const uint32_t now = millis();
  if ((now - g_gesture.lastFrameMs) < GESTURE_FRAME_MS) return;
  g_gesture.lastFrameMs = now;

  if (g_mpu6050Available) {
    readAccel();
    readGyro();
  } else {
    // No sensor — just show idle text
    renderDisplayText(String("TILT"));
    return;
  }

  // Cooldown between gestures
  if ((now - g_gesture.lastGestureMs) < GESTURE_COOLDOWN_MS) {
    // Show last gesture label while in cooldown
    if (now < g_gesture.showUntilMs) {
      renderDisplayText(String(g_gesture.lastLabel));
    } else {
      // Idle — show arrow indicators based on current tilt
      if (g_dextAccelY < -GESTURE_TILT_THRESH)      renderDisplayText(String("<--"));
      else if (g_dextAccelY >  GESTURE_TILT_THRESH)  renderDisplayText(String("-->"));
      else if (g_dextAccelX < -GESTURE_TILT_THRESH)  renderDisplayText(String(" ^ "));
      else if (g_dextAccelX >  GESTURE_TILT_THRESH)  renderDisplayText(String(" v "));
      else                                            renderDisplayText(String("----"));
    }
    return;
  }

  // Detect gesture — priority order: shake > tilt
  const char *gestureName  = nullptr;
  const char *displayLabel = nullptr;

  const int32_t shakeXY = static_cast<int32_t>(abs(g_dextGyroX)) +
                           static_cast<int32_t>(abs(g_dextGyroY));

  if (shakeXY > GESTURE_SHAKE_THRESH) {
    gestureName  = "SHAKE";
    displayLabel = "SHKE";
  } else if (g_dextAccelY < -GESTURE_TILT_THRESH) {
    gestureName  = "LEFT";
    displayLabel = "LEFT";
  } else if (g_dextAccelY > GESTURE_TILT_THRESH) {
    gestureName  = "RIGHT";
    displayLabel = "RGHT";
  } else if (g_dextAccelX < -GESTURE_TILT_THRESH) {
    gestureName  = "FWD";
    displayLabel = "FWD";
  } else if (g_dextAccelX > GESTURE_TILT_THRESH) {
    gestureName  = "BACK";
    displayLabel = "BACK";
  } else if (g_dextAccelZ > 15000) {
    gestureName  = "UP";
    displayLabel = "UP";
  }

  if (gestureName) {
    g_gesture.lastGestureMs = now;
    g_gesture.showUntilMs   = now + GESTURE_SHOW_MS;
    strncpy(g_gesture.lastLabel, displayLabel, sizeof(g_gesture.lastLabel) - 1);
    renderDisplayText(String(displayLabel));
    char buf[32];
    snprintf(buf, sizeof(buf), "GESTURE:%s", gestureName);
    notifyTx(String(buf));
    Serial.printf("Gesture: %s\n", gestureName);
  } else {
    // Idle arrows
    if (g_dextAccelY < -4000)       renderDisplayText(String("<--"));
    else if (g_dextAccelY >  4000)  renderDisplayText(String("-->"));
    else if (g_dextAccelX < -4000)  renderDisplayText(String(" ^ "));
    else if (g_dextAccelX >  4000)  renderDisplayText(String(" v "));
    else                            renderDisplayText(String("----"));
  }
}

void serviceDisplayModes() {
  // When any menu is open the display must still animate — menu labels
  // are written by showMenuSelection/showClockToolSelection but displayAnimate()
  // must be called every loop for MD_Parola to push them to the hardware.
  // Previously this early return skipped animate entirely, so menu text
  // never appeared and users thought MODE button was broken in DEXTBOT mode.
  if (g_menuActive || g_clockToolsMenuActive) {
    g_matrix.displayAnimate();
    return;
  }
  switch (g_currentMode) {
    case DISPLAY_MODE_MESSAGE:
      if (g_messageModeDirty) startMessageDisplay();
      g_matrix.displayAnimate();
      break;
    case DISPLAY_MODE_VISUALIZER: {
      const uint32_t now = millis();
      if ((now - g_lastVisualizerFrameMs) >= VISUALIZER_FRAME_INTERVAL_MS) {
        g_lastVisualizerFrameMs = now;
        if (g_visualizerSource == VISUALIZER_SOURCE_DEVICE)
          renderDeviceVisualizerFrame();
        else if ((now - g_lastPhoneVisualizerUpdateMs) < 1000)
          renderPhoneVisualizerFrame();
      }
      break;
    }
    case DISPLAY_MODE_DEXTBOT:
      // serviceDextbot() handles its own frame rate and menu guard
      break;
    case DISPLAY_MODE_PONG:
    case DISPLAY_MODE_DODGE:
    case DISPLAY_MODE_GESTURE:
      // serviced by their own functions called from loop()
      break;
    default: break;
  }
}

// ── BLE security callbacks ───────────────────────────────────

class SecurityCallbacks : public BLESecurityCallbacks {
 public:
  uint32_t onPassKeyRequest() override { return BLE_STATIC_PASSKEY; }

  void onPassKeyNotify(uint32_t passKey) override {
    Serial.printf("BLE passkey: %06lu\n", static_cast<unsigned long>(passKey));
  }

  bool onSecurityRequest() override {
    Serial.println("BLE security request received"); return true;
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
      Serial.printf("BLE pairing failed: %d\n", authResult.fail_reason);
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

// ── BLE server callbacks ─────────────────────────────────────

class ServerCallbacks : public BLEServerCallbacks {
 public:
  void onConnect([[maybe_unused]] BLEServer *server) override {
    g_isConnected               = true;
    g_restartAdvertisingPending = false;
    g_lastAdvertisingKickMs     = millis();
    Serial.println("BLE client connected");
    updateSensorReading();
    updateBatteryCharacteristic(true);
    notifyTx("STATUS:CONNECTED");
    notifyTx(buildSystemInfoMessage());
    notifyTx(buildSensorMessage());
  }

  void onDisconnect([[maybe_unused]] BLEServer *server) override {
    g_isConnected               = false;
    g_disconnectTimestampMs     = millis();
    g_lastAdvertisingKickMs     = g_disconnectTimestampMs;
    Serial.println("BLE client disconnected");

    if (!g_timeNotSet && g_rtcAvailable) {
      const DateTime current = getSystemDateTime();
      saveTimeToRtc(current);
      saveTimeToNvs(current);   // belt-and-suspenders: persist to flash on every disconnect
      g_timeSourceState = TIME_SOURCE_RTC;
      Serial.println("Offline mode: RTC is primary time source");
    } else if (!g_timeNotSet) {
      saveTimeToNvs(getSystemDateTime());  // no RTC, save to NVS at minimum
    }

    if (g_otaInProgress) abortOta("disconnected");

    // FIX 12: set the pending flag and let restartAdvertisingIfNeeded() handle
    // the delayed restart — avoids a double startAdvertising() call.
    g_restartAdvertisingPending = true;
  }
};

// ── RX command handler ───────────────────────────────────────

class RxCallbacks : public BLECharacteristicCallbacks {
 public:
  void onWrite(BLECharacteristic *characteristic) override {
    const String value = characteristic->getValue();
    if (value.isEmpty()) { Serial.println("RX: empty payload"); return; }
    Serial.print("RX: "); Serial.println(value);

    // FIX 4: BAT: is now a query — respond with actual ADC level, not accept a value
    if (value.startsWith("BAT:")) {
      if (!g_batteryAvailable) {
        notifyTx("BATTERY:UNAVAILABLE");
      } else {
        char buf[20];
        snprintf(buf, sizeof(buf), "BATTERY:%u",
          static_cast<unsigned>(g_batteryLevel));
        notifyTx(String(buf));
      }
      return;
    }

    if (value.startsWith("MSG:")) {
      g_appMessage      = value.substring(4);
      g_messageModeDirty= true;
      notifyTx("STATUS:MESSAGE_UPDATED");
      if (g_currentMode == DISPLAY_MODE_MESSAGE && !g_menuActive) startMessageDisplay();
      return;
    }

    // MODE: setter — previously query-only, now also accepts a value
    if (value.startsWith("MODE:")) {
      const String mv = value.substring(5);
      DisplayMode newMode = g_currentMode;
      if      (mv.equalsIgnoreCase("CLOCK")      || mv.equalsIgnoreCase("CLK"))  newMode = DISPLAY_MODE_CLOCK;
      else if (mv.equalsIgnoreCase("MESSAGE")    || mv.equalsIgnoreCase("MSG"))  newMode = DISPLAY_MODE_MESSAGE;
      else if (mv.equalsIgnoreCase("VISUALIZER") || mv.equalsIgnoreCase("VIZ"))  newMode = DISPLAY_MODE_VISUALIZER;
      else if (mv.equalsIgnoreCase("SETTINGS")   || mv.equalsIgnoreCase("SET"))  newMode = DISPLAY_MODE_SETTINGS;
      else if (mv.equalsIgnoreCase("DEXTBOT")    || mv.equalsIgnoreCase("BOT"))     newMode = DISPLAY_MODE_DEXTBOT;
      else if (mv.equalsIgnoreCase("PONG"))                                           newMode = DISPLAY_MODE_PONG;
      else if (mv.equalsIgnoreCase("DODGE"))                                          newMode = DISPLAY_MODE_DODGE;
      else if (mv.equalsIgnoreCase("GESTURE")    || mv.equalsIgnoreCase("GSTR"))     newMode = DISPLAY_MODE_GESTURE;
      else { notifyTx("STATUS:MODE_INVALID"); return; }
      applyDisplayMode(newMode);
      notifyTx("MODE:" + String(displayModeToString(g_currentMode)));
      return;
    }

    if (value.startsWith("CTOOL:")) {
      const String tv = value.substring(6);
      if      (tv.equalsIgnoreCase("TIME") || tv.equalsIgnoreCase("CLOCK")) activateClockTool(CLOCK_TOOL_TIME);
      else if (tv.equalsIgnoreCase("ALARM"))     activateClockTool(CLOCK_TOOL_ALARM);
      else if (tv.equalsIgnoreCase("TIMER"))     activateClockTool(CLOCK_TOOL_TIMER);
      else if (tv.equalsIgnoreCase("STOPWATCH")) activateClockTool(CLOCK_TOOL_STOPWATCH);
      else { notifyTx("STATUS:CTOOL_INVALID"); return; }
      notifyTx("CTOOL:" + String(clockToolToString(g_activeClockTool)));
      return;
    }

    if (value.startsWith("ALARM_SET:")) {
      int hour = -1, minute = -1;
      if (sscanf(value.substring(10).c_str(), "%d:%d", &hour, &minute) != 2 ||
          hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        notifyTx("STATUS:ALARM_INVALID"); return;
      }
      g_alarmHour = hour; g_alarmMinute = minute; g_alarmEditMode = false;
      if (g_currentMode == DISPLAY_MODE_CLOCK) activateClockTool(CLOCK_TOOL_TIME);
      notifyTx("STATUS:ALARM_UPDATED"); notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_ON")) {
      g_alarmEnabled = true;
      if (g_currentMode == DISPLAY_MODE_CLOCK) activateClockTool(CLOCK_TOOL_TIME);
      notifyTx("STATUS:ALARM_UPDATED"); notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_OFF")) {
      g_alarmEnabled = false; g_alarmRinging = false;
      if (g_currentMode == DISPLAY_MODE_CLOCK) activateClockTool(CLOCK_TOOL_TIME);
      notifyTx("STATUS:ALARM_UPDATED"); notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("ALARM_CLEAR")) {
      g_alarmEnabled = false; g_alarmRinging = false;
      g_alarmHour = 0; g_alarmMinute = 0;
      if (g_currentMode == DISPLAY_MODE_CLOCK) activateClockTool(CLOCK_TOOL_TIME);
      notifyTx("STATUS:ALARM_UPDATED"); notifyTx(buildAlarmStateMessage());
      return;
    }

    if (value.startsWith("TIMER_SET:")) {
      int minutes = -1, seconds = -1;
      if (sscanf(value.substring(10).c_str(), "%d:%d", &minutes, &seconds) != 2 ||
          minutes < 0 || minutes > 99 || seconds < 0 || seconds > 59) {
        notifyTx("STATUS:TIMER_INVALID"); return;
      }
      g_timerTotalSeconds     = minutes * 60 + seconds;
      g_timerRemainingSeconds = g_timerTotalSeconds;
      g_timerRunning          = false;
      g_timerFinished         = false;
      g_timerEditMode         = false;
      g_timerConfigured       = g_timerTotalSeconds > 0;
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED"); notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.startsWith("TIMER_ADD:")) {
      const int addSeconds = value.substring(10).toInt();
      if (addSeconds <= 0) { notifyTx("STATUS:TIMER_INVALID"); return; }
      g_timerTotalSeconds     = min<int32_t>(g_timerTotalSeconds     + addSeconds, 99*60+59);
      g_timerRemainingSeconds = min<int32_t>(g_timerRemainingSeconds + addSeconds, 99*60+59);
      g_timerConfigured       = g_timerTotalSeconds > 0;
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED"); notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_START")) {
      if (g_timerRemainingSeconds <= 0) { notifyTx("STATUS:TIMER_INVALID"); return; }
      g_timerRunning = true; g_timerFinished = false;
      g_timerLastTickMs = millis();
      activateClockTool(CLOCK_TOOL_TIMER);
      notifyTx("STATUS:TIMER_UPDATED"); notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_PAUSE")) {
      g_timerRunning = false;
      notifyTx("STATUS:TIMER_UPDATED"); notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("TIMER_RESET")) {
      g_timerRunning = false; g_timerFinished = false;
      g_timerRemainingSeconds = g_timerTotalSeconds;
      notifyTx("STATUS:TIMER_UPDATED"); notifyTx(buildTimerStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_START")) {
      g_stopwatchRunning    = true;
      g_stopwatchLastTickMs = millis();
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED"); notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_PAUSE")) {
      g_stopwatchRunning = false;
      notifyTx("STATUS:SW_UPDATED"); notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_RESET")) {
      g_stopwatchRunning = false;
      g_stopwatchElapsedMs = g_stopwatchLapMs = 0;
      g_stopwatchLapFrozen = false;
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED"); notifyTx(buildStopwatchStateMessage());
      return;
    }

    if (value.equalsIgnoreCase("SW_LAP")) {
      if (!g_stopwatchRunning && g_stopwatchElapsedMs == 0) {
        notifyTx("STATUS:SW_INVALID"); return;
      }
      g_stopwatchLapMs     = g_stopwatchElapsedMs;
      g_stopwatchLapFrozen = !g_stopwatchLapFrozen;
      activateClockTool(CLOCK_TOOL_STOPWATCH);
      notifyTx("STATUS:SW_UPDATED");
      // FIX 13: build the lap message directly rather than relying on
      // a fragile .substring(3) offset into buildStopwatchStateMessage().
      {
        const uint32_t totalSecs = g_stopwatchLapMs / 1000;
        const uint32_t min       = (totalSecs / 60) % 100;
        const uint32_t sec       = totalSecs % 60;
        const char *state        = g_stopwatchRunning ? "RUNNING" : "PAUSED";
        char buf[40];
        snprintf(buf, sizeof(buf), "SW:LAP,%02lu:%02lu,%s",
          static_cast<unsigned long>(min),
          static_cast<unsigned long>(sec), state);
        notifyTx(String(buf));
      }
      return;
    }

    if (value.startsWith("MSGANIM:")) {
      const String av = value.substring(8);
      if      (av.equalsIgnoreCase("NONE"))   g_messageAnimationStyle = MESSAGE_ANIM_NONE;
      else if (av.equalsIgnoreCase("WAVE"))   g_messageAnimationStyle = MESSAGE_ANIM_WAVE;
      else if (av.equalsIgnoreCase("SCROLL")) g_messageAnimationStyle = MESSAGE_ANIM_SCROLL;
      else if (av.equalsIgnoreCase("RAIN"))   g_messageAnimationStyle = MESSAGE_ANIM_RAIN;
      else { notifyTx("STATUS:MSGANIM_INVALID"); return; }
      g_messageModeDirty = true;
      if (g_currentMode == DISPLAY_MODE_MESSAGE && !g_menuActive) startMessageDisplay();
      notifyTx("STATUS:MSGANIM_UPDATED");
      notifyTx("MSGANIM:" + String(messageAnimationToString(g_messageAnimationStyle)));
      return;
    }

    if (value.startsWith("VIZSRC:")) {
      const String sv = value.substring(7);
      if      (sv.equalsIgnoreCase("DEVICE")) g_visualizerSource = VISUALIZER_SOURCE_DEVICE;
      else if (sv.equalsIgnoreCase("PHONE"))  g_visualizerSource = VISUALIZER_SOURCE_PHONE;
      else { notifyTx("STATUS:VIZSRC_INVALID"); return; }
      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive)
        applyDisplayMode(DISPLAY_MODE_VISUALIZER);
      notifyTx("STATUS:VIZSRC_UPDATED");
      notifyTx("VIZSRC:" + String(visualizerSourceToString(g_visualizerSource)));
      return;
    }

    if (value.startsWith("VIZSTYLE:")) {
      const String sv = value.substring(9);
      if      (sv.equalsIgnoreCase("BARS"))   g_visualizerStyle = VISUALIZER_STYLE_BARS;
      else if (sv.equalsIgnoreCase("WAVE"))   g_visualizerStyle = VISUALIZER_STYLE_WAVE;
      else if (sv.equalsIgnoreCase("RADIAL")) g_visualizerStyle = VISUALIZER_STYLE_RADIAL;
      else { notifyTx("STATUS:VIZSTYLE_INVALID"); return; }
      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive) {
        if (g_visualizerSource == VISUALIZER_SOURCE_PHONE)
          renderPhoneVisualizerFrame();
        else
          renderDeviceVisualizerFrame();
      }
      notifyTx("STATUS:VIZSTYLE_UPDATED");
      notifyTx("VIZSTYLE:" + String(visualizerStyleToString(g_visualizerStyle)));
      return;
    }

    if (value.startsWith("VIZFRAME:")) {
      if (!parsePhoneVisualizerFrame(value.substring(9))) {
        notifyTx("STATUS:VIZFRAME_INVALID"); return;
      }
      if (g_currentMode == DISPLAY_MODE_VISUALIZER && !g_menuActive &&
          g_visualizerSource == VISUALIZER_SOURCE_PHONE)
        renderPhoneVisualizerFrame();
      notifyTx("STATUS:VIZFRAME_UPDATED");
      return;
    }

    if (value.startsWith("BRIGHTNESS:")) {
      const int bv = value.substring(11).toInt();
      if (bv < 0 || bv > 15) { notifyTx("STATUS:BRIGHTNESS_INVALID"); return; }
      applyDisplayBrightness(static_cast<uint8_t>(bv));
      notifyTx("STATUS:BRIGHTNESS_UPDATED");
      return;
    }

    if (value.startsWith("TIMEFMT:")) {
      const String fv = value.substring(8);
      if      (fv == "12") g_timeDisplayFormat = TIME_FORMAT_12H;
      else if (fv == "24") g_timeDisplayFormat = TIME_FORMAT_24H;
      else { notifyTx("STATUS:TIME_FORMAT_INVALID"); return; }
      if (g_currentMode == DISPLAY_MODE_CLOCK && !g_menuActive) {
        g_lastDisplayedTimeText = "";
        updateDisplayTime();
      }
      notifyTx("STATUS:TIME_FORMAT_UPDATED");
      notifyTx("TIMEFMT:" + String(timeFormatToString(g_timeDisplayFormat)));
      return;
    }

    if (value.startsWith("BTN:")) { handleVirtualButton(value.substring(4)); return; }

    if (value.equalsIgnoreCase("OTA_END"))   { Serial.println("RX: OTA_END");   finishOta(); return; }
    if (value.equalsIgnoreCase("OTA_ABORT")) { Serial.println("RX: OTA_ABORT"); abortOta("aborted"); return; }

    if (value.equalsIgnoreCase("OTA_STATUS")) {
      if (g_otaInProgress) {
        char buf[48];
        snprintf(buf, sizeof(buf), "IN_PROGRESS,%u/%u",
          static_cast<unsigned>(g_otaReceivedSize),
          static_cast<unsigned>(g_otaExpectedSize));
        sendOtaStatus(buf);
      } else {
        sendOtaStatus("IDLE");
      }
      return;
    }

    if (value.equalsIgnoreCase("INFO"))     { notifyTx(buildSystemInfoMessage()); return; }
    if (value.equalsIgnoreCase("VERSION?")) { notifyTx(buildVersionMessage());    return; }

    if (value.equalsIgnoreCase("TIME?")) {
      if (g_timeNotSet) {
        notifyTx("TIME:INVALID");
      } else {
        const DateTime now = getSystemDateTime();
        char buf[48];
        snprintf(buf, sizeof(buf), "TIME:%02d:%02d:%02d,%02d-%02d-%04d,SRC:%s",
          now.hour(), now.minute(), now.second(),
          now.day(), now.month(), now.year(),
          timeSourceToString(g_timeSourceState));
        notifyTx(String(buf));
      }
      return;
    }

    if (value.equalsIgnoreCase("MODE?"))      { notifyTx("MODE:"    + String(displayModeToString(g_currentMode)));       return; }
    if (value.equalsIgnoreCase("CTOOL?"))     { notifyTx("CTOOL:"   + String(clockToolToString(g_activeClockTool)));     return; }
    if (value.equalsIgnoreCase("ALARM?"))     { notifyTx(buildAlarmStateMessage());   return; }
    if (value.equalsIgnoreCase("TIMER?"))     { notifyTx(buildTimerStateMessage());   return; }
    if (value.equalsIgnoreCase("SW?"))        { notifyTx(buildStopwatchStateMessage()); return; }
    if (value.equalsIgnoreCase("MSGANIM?"))   { notifyTx("MSGANIM:" + String(messageAnimationToString(g_messageAnimationStyle)));  return; }
    if (value.equalsIgnoreCase("VIZSRC?"))    { notifyTx("VIZSRC:"  + String(visualizerSourceToString(g_visualizerSource)));      return; }
    if (value.equalsIgnoreCase("VIZSTYLE?"))  { notifyTx("VIZSTYLE:"+ String(visualizerStyleToString(g_visualizerStyle)));        return; }
    if (value.equalsIgnoreCase("BRIGHTNESS?")){ notifyTx("BRIGHTNESS:" + String(g_displayBrightness)); return; }
    if (value.equalsIgnoreCase("TIMEFMT?"))   { notifyTx("TIMEFMT:" + String(timeFormatToString(g_timeDisplayFormat)));   return; }

    if (value.equalsIgnoreCase("DEXTBOT?")) {
      char buf[64];
      snprintf(buf, sizeof(buf), "DEXTBOT:DIST:%u,AX:%d,AY:%d,AZ:%d,MPU:%s,VL53:%s",
        static_cast<unsigned>(g_dextDistMm),
        static_cast<int>(g_dextAccelX),
        static_cast<int>(g_dextAccelY),
        static_cast<int>(g_dextAccelZ),
        g_mpu6050Available ? "OK" : "FAIL",
        g_vl53Available    ? "OK" : "FAIL");
      notifyTx(String(buf));
      return;
    }

    if (value.equalsIgnoreCase("BAT?")) {
      notifyTx(g_batteryAvailable
        ? "BATTERY:" + String(g_batteryLevel)
        : "BATTERY:UNAVAILABLE");
      return;
    }

    if (value.equalsIgnoreCase("SENSOR?")) {
      updateSensorReading();
      notifyTx(buildSensorMessage());
      return;
    }

    // ── Weather BLE commands ──────────────────────────────────
    // WEATHER:T:22.5,F:20.1,C:Rain,H:65,CITY:London
    // All fields optional except T (temperature).
    // C (condition) should be the OWM "main" field: Clear, Clouds, Rain, etc.
    if (value.startsWith("WEATHER:")) {
      applyWeatherUpdate(value.substring(8));
      return;
    }

    // Query current stored weather data
    if (value.equalsIgnoreCase("WEATHER?")) {
      notifyTx(buildWeatherMessage());
      return;
    }

    // Clear stored weather (e.g. app wants a clean state)
    if (value.equalsIgnoreCase("WEATHER_CLEAR")) {
      g_hasWeather           = false;
      g_lastWeatherMs        = 0;
      g_weatherTempC         = NAN;
      g_weatherFeelsC        = NAN;
      g_weatherHumidity      = 0;
      g_weatherCondition[0]  = '\0';
      g_weatherCity[0]       = '\0';
      g_clockModeCycleStartMs= millis();
      g_lastDisplayedTimeText= "";
      // Bug 13 fix: do NOT call g_prefs here — NVS in BLE callback is a
      // race condition (same as Bug 4). Set the pending save flag with
      // cleared data so serviceWeatherNvsSave() handles it on the main loop.
      g_weatherNvsSavePending = true;
      notifyTx("STATUS:WEATHER_CLEARED");
      return;
    }

    if (value.startsWith("OTA_BEGIN:")) { Serial.println("RX: OTA_BEGIN"); beginOta(value.substring(10));   return; }
    if (value.startsWith("SYNC:"))      { Serial.println("RX: SYNC");      applyPhoneSync(value.substring(5)); return; }

    if (value.startsWith("OTA_CHUNK:")) {
      Serial.printf("RX: OTA_CHUNK len=%u\n", static_cast<unsigned>(value.length() - 10));
      writeOtaChunk(value.substring(10));
      return;
    }

    notifyTx("ECHO:" + value);
  }
};

// ── BLE service creation ─────────────────────────────────────

void createBatteryService() {
  BLEService *svc = g_server->createService(BATTERY_SERVICE_UUID);
  g_batteryCharacteristic = svc->createCharacteristic(
    BATTERY_LEVEL_UUID, BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);
  g_batteryCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ_ENCRYPTED);
  g_batteryCharacteristic->addDescriptor(new BLE2902());
  g_batteryCharacteristic->setValue(&g_batteryLevel, 1);
  svc->start();
}

void createDeviceInformationService() {
  BLEService *svc = g_server->createService(DEVICE_INFO_SERVICE_UUID);
  auto addReadChar = [&](const char *uuid, const char *value, uint32_t perm) {
    BLECharacteristic *c = svc->createCharacteristic(uuid, BLECharacteristic::PROPERTY_READ);
    c->setAccessPermissions(perm);
    c->setValue(value);
  };
  addReadChar(MANUFACTURER_NAME_UUID,  MANUFACTURER_NAME, ESP_GATT_PERM_READ);
  addReadChar(MODEL_NUMBER_UUID,       MODEL_NUMBER,      ESP_GATT_PERM_READ);
  addReadChar(FIRMWARE_REVISION_UUID,  FIRMWARE_REVISION, ESP_GATT_PERM_READ);
  svc->start();
}

void createCustomService() {
  BLEService *svc = g_server->createService(CUSTOM_SERVICE_UUID);
  BLECharacteristic *rx = svc->createCharacteristic(
    CUSTOM_RX_UUID,
    BLECharacteristic::PROPERTY_WRITE | BLECharacteristic::PROPERTY_WRITE_NR);
  rx->setAccessPermissions(ESP_GATT_PERM_WRITE_ENCRYPTED);
  rx->setCallbacks(new RxCallbacks());
  g_txCharacteristic = svc->createCharacteristic(
    CUSTOM_TX_UUID,
    BLECharacteristic::PROPERTY_NOTIFY | BLECharacteristic::PROPERTY_READ);
  g_txCharacteristic->setAccessPermissions(ESP_GATT_PERM_READ_ENCRYPTED);
  g_txCharacteristic->addDescriptor(new BLE2902());
  g_txCharacteristic->setValue("STATUS:READY");
  svc->start();
}

void startAdvertisingNow(const char *reason, bool forceRestart) {
  if (forceRestart) { BLEDevice::stopAdvertising(); delay(40); }
  BLEDevice::startAdvertising();
  g_lastAdvertisingKickMs = millis();
  Serial.print("BLE advertising "); Serial.println(reason);
}

void configureAdvertising() {
  BLEAdvertising *adv = BLEDevice::getAdvertising();
  BLEDevice::setPower(ESP_PWR_LVL_P9);
  adv->setAppearance(DEVICE_APPEARANCE);
  adv->setScanResponse(true);
  adv->setMinPreferred(0x06);
  adv->setMaxPreferred(0x12);
  adv->setMinInterval(160);
  adv->setMaxInterval(320);
  adv->addServiceUUID(BATTERY_SERVICE_UUID);
  adv->addServiceUUID(CUSTOM_SERVICE_UUID);
  adv->addServiceUUID(DEVICE_INFO_SERVICE_UUID);
  startAdvertisingNow("started", false);
}

void initBle() {
  BLEDevice::init(DEVICE_NAME);
  BLEDevice::setMTU(517);
  BLEDevice::setSecurityCallbacks(new SecurityCallbacks());

  BLESecurity *security = new BLESecurity();
  // FIX 2: correct single-enum API (was setAuthenticationMode(true, true, true))
  security->setAuthenticationMode(ESP_LE_AUTH_REQ_SC_MITM_BOND);
  security->setCapability(ESP_IO_CAP_OUT);
  // FIX 3: correct single-argument API (was setPassKey(true, BLE_STATIC_PASSKEY))
  security->setPassKey(BLE_STATIC_PASSKEY);
  security->setInitEncryptionKey(ESP_BLE_ENC_KEY_MASK | ESP_BLE_ID_KEY_MASK);
  security->setRespEncryptionKey(ESP_BLE_ENC_KEY_MASK | ESP_BLE_ID_KEY_MASK);

  g_server = BLEDevice::createServer();
  g_server->setCallbacks(new ServerCallbacks());
  g_server->advertiseOnDisconnect(false);  // manual control via restartAdvertisingIfNeeded

  createBatteryService();
  createDeviceInformationService();
  createCustomService();
  configureAdvertising();
}

// ── Advertising helpers ──────────────────────────────────────

void restartAdvertisingIfNeeded() {
  if (!g_restartAdvertisingPending) return;
  if ((millis() - g_disconnectTimestampMs) < RESTART_ADV_DELAY_MS) return;
  // FIX 12: this is now the ONLY advertising restart path after disconnect
  startAdvertisingNow("restarted", true);
  g_restartAdvertisingPending = false;
}

void serviceAdvertisingHealth() {
  if (g_isConnected) return;
  const uint32_t now = millis();
  if ((now - g_bootTimestampMs) < BOOT_ADVERTISING_STABILIZE_MS) return;
  if ((now - g_lastAdvertisingKickMs) < ADVERTISING_REFRESH_INTERVAL_MS) return;
  startAdvertisingNow("refreshed", true);
}

}  // namespace

// ── setup / loop ─────────────────────────────────────────────

void setup() {
  Serial.begin(SERIAL_BAUDRATE);
  delay(200);
  Serial.println();
  Serial.println("Booting DotMatrix Clock BLE firmware");
  Serial.printf("FW: %s  Reset reason: %d\n",
    FIRMWARE_REVISION, static_cast<int>(esp_reset_reason()));
  g_bootTimestampMs = millis();

  // FIX 6: initialise hardware watchdog before any peripheral init.
  // ESP-IDF v5 / Arduino core 3.x replaced the two-arg form with a config struct.
  {
    const esp_task_wdt_config_t wdtCfg = {
      .timeout_ms    = WDT_TIMEOUT_S * 1000,
      .idle_core_mask= 0,    // do not watch idle tasks
      .trigger_panic = true  // reboot on timeout
    };
    esp_task_wdt_init(&wdtCfg);
  }
  esp_task_wdt_add(nullptr);  // subscribe the main (loop) task

  // FIX 14: analogReadResolution set ONCE unconditionally (duplicate
  // inside the BATTERY_SENSE_PIN block has been removed)
  analogReadResolution(12);

  if (BATTERY_SENSE_PIN >= 0) {
    pinMode(BATTERY_SENSE_PIN, INPUT);
  }

  pinMode(MIC_INPUT_PIN, INPUT);
  if (MIC_SENSITIVITY_PIN >= 0) pinMode(MIC_SENSITIVITY_PIN, INPUT);

  pinMode(BUZZER_PIN, OUTPUT);
  writeBuzzer(false);

  pinMode(BUTTON_MODE_PIN,   INPUT_PULLUP);
  pinMode(BUTTON_NEXT_PIN,   INPUT_PULLUP);
  pinMode(BUTTON_BACK_PIN,   INPUT_PULLUP);
  pinMode(BUTTON_SELECT_PIN, INPUT_PULLUP);

  initDisplay();

  const bool rtcOk = initRtc();

  // ── Three-tier time recovery on boot ─────────────────────────
  // Tier 1: RTC with working battery (most accurate, survives power cut)
  // Tier 2: NVS flash (approximate — correct to within the last save
  //         interval, wrong by however long the device was off)
  // Tier 3: Wait for phone sync (shows "--:--")
  if (rtcOk && loadTimeFromRtc()) {
    Serial.println("Boot: time restored from RTC");
    // Also refresh NVS so it stays current even after a long power-off
    saveTimeToNvs(getSystemDateTime());
  } else if (loadTimeFromNvs()) {
    Serial.println("Boot: time estimated from NVS flash (RTC unavailable/battery dead)");
    Serial.println("      Time may be off by the duration the device was powered off.");
    Serial.println("      Connect the app to restore accurate time.");
    // onConnect handler already sends buildSystemInfoMessage() which includes
  // TIME:VALID and SRC:NVS_ESTIMATED — the app should use that to prompt
  // the user to sync when it sees SRC:NVS_ESTIMATED.
  } else {
    g_timeNotSet      = true;
    g_timeSourceState = TIME_INVALID;
    Serial.println("Boot: no time source available — waiting for app sync");
    showSyncNeeded();
  }

  g_dht.begin();
  initDextbot();
  loadWeatherFromNvs();   // restore last known weather before BLE starts
  initBle();
  updateSensorReading();
  updateBatteryCharacteristic(false);
  applyDisplayMode(DISPLAY_MODE_CLOCK);
}

void loop() {
  esp_task_wdt_reset();  // FIX 6: pet the watchdog every iteration

  serviceButtons();
  restartAdvertisingIfNeeded();
  serviceAdvertisingHealth();
  serviceOtaState();
  serviceBatteryUpdates();
  serviceSensorUpdates();
  serviceDisplayClock();
  serviceClockTools();
  serviceBuzzer();
  serviceDextbot();
  servicePong();
  serviceDodge();
  serviceGesture();
  serviceDisplayModes();
  serviceRtcWriteBack();
  serviceNvsWriteBack();
  serviceWeatherNvsSave();  // Bug 4 fix: deferred NVS write from BLE callback
  yield();
}
