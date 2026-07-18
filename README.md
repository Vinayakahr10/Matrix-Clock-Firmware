# DotMatrix Clock — Circuit Block Diagram & Workflow

---

## Top-Level Block Diagram

```
                        ┌─────────────┐
                        │   USB-C     │
                        │  5V Input   │
                        └──────┬──────┘
                               │ 5V
                        ┌──────▼──────┐
                        │   IP5306    │◄──── LiPo 3.7V 3000mAh
                        │  Charger +  │────► LiPo (charge)
                        │  5V Boost   │
                        └──────┬──────┘
                               │ 5V regulated
              ┌────────────────┼────────────────┐
              │                                 │
       ┌──────▼──────┐                  ┌───────▼──────┐
       │  MAX7219 ×4 │                  │   MP1584EN   │
       │  Dot Matrix │                  │ 5V → 3.3V    │
       │   Display   │                  │    Buck      │
       └──────▲──────┘                  └───────┬──────┘
              │ SPI (5V logic)                  │ 3.3V
       ┌──────┴──────┐          ┌───────────────┼───────────────────┐
       │  74HCT125   │          │               │                   │
       │ Level Shift │          │               │                   │
       │ 3.3V → 5V  │◄─SPI─┐   │        ┌──────▼──────┐    ┌───────▼──────┐
       └─────────────┘      │   │        │   DS1307    │    │  MPU-6050   │
                            │   │        │     RTC     │    │    IMU      │
                     ┌──────┴───▼──┐     │  I2C 0x68  │    │  I2C 0x69  │
                     │   ESP32     │     └─────────────┘    └─────────────┘
                     │ WROOM-32    │
                     │ BLE + Logic │     ┌─────────────┐    ┌─────────────┐
                     └──────┬──────┘     │  VL53L0X   │    │   DHT22    │
                            │            │   Laser     │    │ Temp + Hum │
                            │            │  I2C 0x29  │    │  GPIO 4    │
                     ┌──────┴──────┐     └─────────────┘    └─────────────┘
                     │  I2C Bus   ├─────────────────────────────────────────
                     │ SDA GPIO21 │
                     │ SCL GPIO22 │     ┌─────────────┐    ┌─────────────┐
                     └─────────────┘    │   MAX4466   │    │  Buttons ×4 │
                                        │    Mic      │    │ MODE NEXT   │
                                        │  GPIO 34   │    │ BACK SELECT │
                                        └─────────────┘    └─────────────┘

                                        ┌─────────────┐    ┌─────────────┐
                                        │   Buzzer   │    │  Battery   │
                                        │  GPIO 27   │    │  ADC GPIO35 │
                                        └─────────────┘    └─────────────┘
```

---

## Power Workflow

```
Step 1 — USB plugged in
──────────────────────
USB-C (5V)
    │
    ├── CC1 pin ──► 5.1kΩ ──► GND  (tells charger to deliver 5V)
    ├── CC2 pin ──► 5.1kΩ ──► GND  (tells charger to deliver 5V)
    └── VBUS ────► IP5306 VIN

Step 2 — IP5306 charges the battery
─────────────────────────────────────
IP5306 VIN (5V)
    ├── Charges LiPo at 2.1A via BAT pin
    └── Simultaneously outputs regulated 5V on VOUT pin

Step 3 — 5V splits into two rails
──────────────────────────────────
IP5306 VOUT (5V)
    ├──► MAX7219 VCC — powers display at full brightness
    └──► MP1584EN VIN — steps down to 3.3V

Step 4 — MP1584 provides clean 3.3V
─────────────────────────────────────
MP1584EN
    ├── Input  : 5V from IP5306
    ├── Output : 3.3V regulated (up to 3A)
    └── Powers : ESP32, DS1307, MPU-6050, VL53L0X,
                 DHT22, MAX4466, 74HCT125 VCC,
                 Buttons, Buzzer logic

Step 5 — Battery monitoring
─────────────────────────────
LiPo BAT+ ──► 100kΩ ──► GPIO35 (ADC) ──► 100kΩ ──► GND
    (4.2V max halved to 2.1V — safe for ESP32 ADC 3.3V max)
    Firmware reads ADC, maps to 0–100%

Step 6 — Charge detection
───────────────────────────
IP5306 MFB pin
    ├── 10kΩ pullup to 3.3V
    └── GPIO36 reads: LOW = charging, HIGH = full or unplugged
    Firmware debounces 2 seconds before updating charge state
```

---

## SPI Display Workflow

```
ESP32 (3.3V logic)              74HCT125              MAX7219 chain (5V logic)
──────────────────              ─────────              ───────────────────────
GPIO23 (DIN)  ─────────────►  Pin 2 (1A) ──► Pin 3 (1Y) ──► MAX7219 #1 DIN
GPIO18 (CLK)  ─────────────►  Pin 5 (2A) ──► Pin 6 (2Y) ──► ALL CLK pins
GPIO5  (CS)   ─────────────►  Pin 9 (3A) ──► Pin 8 (3Y) ──► ALL CS  pins

MAX7219 chain:
   #1 DOUT ──► #2 DIN ──► #3 DIN ──► #4 DIN
   All CLK pins tied together
   All CS  pins tied together

Why 74HCT125?
   ESP32 outputs 3.3V HIGH
   MAX7219 at 5V needs minimum 3.5V to recognise HIGH
   3.3V < 3.5V = unreliable on PCB with noise
   74HCT125 converts 3.3V input to 5V output reliably

OE pins (1, 4, 10, 13) all tied to GND = always enabled
VCC pin 14 = 5V
100nF decoupling cap on VCC to GND
```

---

## I2C Bus Workflow

```
ESP32 GPIO21 (SDA) ──┬──► DS1307  (0x68)
                     ├──► MPU-6050 (0x69)
                     └──► VL53L0X  (0x29)

ESP32 GPIO22 (SCL) ──┬──► DS1307  (0x68)
                     ├──► MPU-6050 (0x69)
                     └──► VL53L0X  (0x29)

Pullup resistors:
   SDA ──► 4.7kΩ ──► 3.3V
   SCL ──► 4.7kΩ ──► 3.3V
   (required for I2C to work — without pullups bus stays floating)

Address assignment:
   DS1307  = 0x68 (fixed, no way to change)
   MPU6050 = 0x69 (AD0 pin tied HIGH to 3.3V, default is 0x68)
   VL53L0X = 0x29 (fixed, only one per bus unless using XSHUT pin)

Boot sequence in firmware:
   Wire.begin(21, 22)
   ├── g_rtc.begin()        — check DS1307 responds
   ├── initMpu6050()        — wake from sleep, check respond
   └── g_vl53.begin()       — check VL53L0X responds
```

---

## Sensor Workflow

```
DHT22 (GPIO4)
──────────────
Every 5 seconds:
   g_dht.readTemperature() → g_lastTemperatureC
   g_dht.readHumidity()    → g_lastHumidity
   If read fails → retry after 2.2s minimum
   If no valid reading in 30s → mark as stale, hide from display

MPU-6050 (I2C 0x69) — DextBot + Games
────────────────────────────────────────
Every 40ms (DextBot frame rate):
   Read 6 bytes from 0x3B → AccelX, AccelY, AccelZ (each 16-bit)
   Read 6 bytes from 0x43 → GyroX,  GyroY,  GyroZ  (each 16-bit)

   Accel used for:
      Tilt detection  → Pong left paddle, Dodge dodge direction
      Gesture detect  → LEFT / RIGHT / FWD / BACK / UP
      DextBot AUTO    → expression based on distance (VL53L0X primary)

   Gyro used for:
      Shake detection → SHAKE gesture, BONK expression
      Spin detection  → DIZZY expression
      Angle integrate → g_gyroAngleZ += (raw / 131.0) × dtSec

VL53L0X (I2C 0x29) — DextBot face selection
──────────────────────────────────────────────
Every 40ms:
   g_vl53.rangingTest() → distance in mm
   Maps to expression:
      < 80mm   → SCARED
      80–200mm → SURPRISE
      200–400mm→ CURIOUS
      400–700mm→ HAPPY
      > 700mm  → SLEEPY
   Gyro can override to BONK or DIZZY (higher priority)

MAX4466 (GPIO34 ADC)
─────────────────────
Visualizer mode only, every 180ms:
   64 samples taken at 4000 Hz
   DC offset (2048) subtracted from each sample
   arduinoFFT computes frequency magnitudes
   8 frequency bins mapped to 8 display columns
   Each column height = magnitude clamped to 8 rows
```

---

## BLE Workflow

```
Boot
 └── initBle()
       ├── BLEDevice::init("DotMatrix Clock")
       ├── Create GATT server
       ├── Create custom service  (UUID 6b5f9001-...)
       ├── Create RX characteristic (UUID 6b5f9002-...) WRITE ENCRYPTED
       ├── Create TX characteristic (UUID 6b5f9003-...) NOTIFY + READ
       ├── Create Battery service  (UUID 180F)
       ├── Create Device Info service (UUID 180A)
       ├── Set passkey: 493271
       ├── Set security: SC + MITM + BOND
       └── Start advertising

Phone connects
 └── onConnect() fires
       ├── notifyTx("STATUS:CONNECTED")
       ├── notifyTx(buildSystemInfoMessage())  → all device state
       ├── notifyTx(buildSensorMessage())      → temperature, humidity
       ├── notifyTx("ALARM_LIST:...")           → alarm state
       └── notifyTx("GAME_SCORE:...")           → last game scores

Phone sends command
 └── RxCallbacks::onWrite() fires (on BLE RTOS task)
       ├── Parse string value
       ├── Match against command list
       ├── Update globals / set flags
       └── notifyTx(response)

Main loop sends notifications
 └── notifyTx() called when:
       ├── Alarm triggers   → "STATUS:ALARM_TRIGGERED,07:00"
       ├── Timer done       → "STATUS:TIMER_DONE,300"
       ├── Gesture fires    → "GESTURE:LEFT"
       ├── Game score       → "PONG:SCORE,3,9"
       ├── Game over        → "DODGE:GAMEOVER,47"
       ├── OTA progress     → "OTA:CHUNK_OK,bytesReceived"
       └── Notify expired   → "STATUS:NOTIFY_EXPIRED"

Phone disconnects
 └── onDisconnect() fires
       ├── Save time to RTC
       ├── Save time to NVS flash
       └── Set g_restartAdvertisingPending = true
             └── loop() restarts advertising after 250ms
```

---

## Display Workflow

```
loop() → serviceDisplayModes()
    │
    ├── [CLOCK mode]
    │     serviceDisplayClock() every 1 second
    │       ├── Calculate cycle offset (millis since cycle start)
    │       ├── Slide 0 (5s) → HH:MM or HH:MM AM/PM
    │       ├── Slide 1 (2s) → DD/MM/YYYY
    │       ├── Slide 2 (2s) → 28C (indoor temp)
    │       ├── Slide 3 (2s) → MON / TUE / WED...
    │       ├── Slide 4 (2s) → RAIN / SUN / CLDY  (if weather fresh)
    │       └── Slide 5 (2s) → o31C               (if weather fresh)
    │
    ├── [MESSAGE mode]
    │     renderDisplayText(g_msgBuffer)
    │     MD_Parola scrolls text with selected animation
    │     (SCROLL / WAVE / RAIN / NONE)
    │
    ├── [VISUALIZER mode]
    │     Every 180ms:
    │     Sample mic → FFT → 8 frequency bins
    │     → setColumn(0..7) with bar heights
    │
    ├── [DEXTBOT mode]
    │     Every 40ms (25fps):
    │     Read VL53L0X distance → base expression
    │     Read MPU-6050 gyro   → override to DIZZY or BONK
    │     drawEye(leftEye)  → 7 columns
    │     drawEye(rightEye) → 7 columns
    │     Pupil lerps smoothly toward tilt target
    │
    ├── [PONG mode]
    │     Every 80ms:
    │     Move ball → check paddle collision → check wall bounce
    │     AI paddle tracks ball Y position
    │     Player paddle = MPU-6050 AccelX tilt
    │     Score on miss → update display → notify phone
    │
    ├── [DODGE mode]
    │     Every 120ms:
    │     Spawn falling pixel at random column
    │     Player moves left/right via AccelY tilt
    │     Collision check → game over if hit
    │     Speed increases every spawn cycle
    │
    └── [GESTURE mode]
          Every 50ms:
          Read AccelX/Y/Z + GyroXY magnitude
          Map to gesture name
          Show arrow on display (← → ↑ ↓)
          notifyTx("GESTURE:LEFT") to phone
```

---

## OTA Update Workflow

```
Phone sends OTA_BEGIN:size,md5,token
    ├── Validate token against OTA_SHARED_SECRET
    ├── Call Update.begin(expectedSize)
    ├── requestOtaConnectionSpeed() → 7.5ms BLE interval
    ├── g_otaInProgress = true → loop() skips all other services
    └── notifyTx("OTA:READY")

Phone sends OTA_CHUNK:hexdata (repeated)
    ├── Decode hex → binary bytes
    ├── Update.write(buffer, chunkSize)
    ├── Draw progress bar on display
    ├── g_otaReceivedSize += chunkSize
    └── notifyTx("OTA:ACK,bytesReceived")

Phone sends OTA_END
    ├── Update.end(true) → verify MD5
    ├── restoreNormalConnectionSpeed()
    ├── Show "DONE" on display
    ├── notifyTx("OTA:DONE,v1.0.4")
    └── g_otaRebootPending = true
          └── serviceReboot() waits 1.2s → ESP.restart()
```

---

## NVS Flash Workflow

```
On boot:
    loadTimeFromRtc()   → try DS1307 first
    loadTimeFromNvs()   → fallback if RTC dead or invalid
    loadWeatherFromNvs()→ restore last weather data

Every 60 seconds:
    saveTimeToRtc(now)  → write to DS1307
    saveTimeToNvs(now)  → write epoch + save timestamp to flash

On phone sync (TIME:HH:MM:SS,DD-MM-YYYY):
    setSystemClock()    → update ESP32 software clock
    saveTimeToRtc()     → update DS1307 immediately
    saveTimeToNvs()     → update flash immediately

On weather update (WEATHER:T:31,...):
    applyWeatherUpdate()→ parse and store in globals
    g_weatherNvsSavePending = true  ← flag set (BLE task, not safe for NVS)
    serviceWeatherNvsSave() in loop()← actual flash write (main task, safe)

NVS keys used:
    epoch   → unix timestamp of last known time
    savets  → when epoch was saved (for staleness check)
    wtmp    → weather temperature × 10 (integer)
    wfls    → feels-like temperature × 10
    whum    → humidity
    wcnd    → condition string (Rain, Clear, Clouds...)
    wcty    → city name
    wts     → weather data timestamp
```

---

## Button Workflow

```
4 buttons wired as INPUT_PULLUP:
   HIGH = not pressed (pulled to 3.3V)
   LOW  = pressed (button connects to GND)

serviceButtons() called every loop:
   wasButtonPressed(button, now)
       ├── digitalRead(pin) != LOW → skip
       ├── millis - lastPress < 180ms → skip (debounce)
       └── Update lastPress → return true

handleVirtualButton(name) processes press:
   Priority order:
   1. Alarm or timer ringing → stop buzzer (any button)
   2. OTA in progress → ignore buttons
   3. Notify active → dismiss notification
   4. Edit mode (alarm/timer edit) → adjust field
   5. Menu open → navigate modes
   6. Clock tools menu → navigate tools
   7. Normal → open menu / control active tool

BLE virtual buttons (app sends BUTTON:MODE etc.):
   handleVirtualButton("MODE")
   handleVirtualButton("NEXT")
   handleVirtualButton("BACK")
   handleVirtualButton("SELECT")
   Identical logic to physical buttons
```

---

## Watchdog Workflow

```
setup():
    esp_task_wdt_init(30 seconds, trigger_panic=true)
    esp_task_wdt_add(NULL)  → subscribe main task

Every loop():
    esp_task_wdt_reset()    → "still alive" signal

If loop() gets stuck for 30 seconds:
    (sensor hang, I2C lockup, infinite loop, etc.)
    Hardware watchdog fires → prints crash dump → forces reboot
    Device recovers automatically
```

---

## Complete Signal Map

```
GPIO | Direction | Connected to
─────┼───────────┼────────────────────────────────────
4    | IN        | DHT22 data
5    | OUT       | MAX7219 CS (via 74HCT125)
18   | OUT       | MAX7219 CLK (via 74HCT125)
21   | IN/OUT    | I2C SDA (DS1307, MPU6050, VL53L0X)
22   | OUT       | I2C SCL
23   | OUT       | MAX7219 DIN (via 74HCT125)
25   | IN        | BACK button (INPUT_PULLUP)
26   | IN        | SELECT button (INPUT_PULLUP)
27   | OUT       | Buzzer
32   | IN        | MODE button (INPUT_PULLUP)
33   | IN        | NEXT button (INPUT_PULLUP)
34   | IN (ADC)  | MAX4466 microphone output
35   | IN (ADC)  | Battery voltage divider
36   | IN        | IP5306 MFB charge detect
```
