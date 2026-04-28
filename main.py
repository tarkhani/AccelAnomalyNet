import network
import time
from machine import Pin, I2C
from simple import MQTTClient
import ssl

try:
    import ntptime
except ImportError:
    ntptime = None

# === MPU6050 registers ===
MPU6050_ADDR = 0x68
PWR_MGMT_1 = 0x6B
ACCEL_XOUT_H = 0x3B

# === CONFIG ===
SSID = "Ahmadreza's iPhone"
PASSWORD = "Artorias1376!"

MQTT_TOPIC = b"iotproject/accelerometer"
MQTT_SERVER = "606be9cdd83841ab8aa160b075157595.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "tarkhani"
MQTT_PASSWORD = "Artorias1376!"


# === I2C + MPU6050 ===
def init_i2c():
    return I2C(0, scl=Pin(9), sda=Pin(8), freq=200000)


def write_byte(i2c, reg, data):
    i2c.writeto_mem(MPU6050_ADDR, reg, bytearray([data]))


def read_word(i2c, reg):
    data = i2c.readfrom_mem(MPU6050_ADDR, reg, 2)
    val = (data[0] << 8) | data[1]
    return val if val < 0x8000 else val - 0x10000


def init_mpu6050(i2c):
    write_byte(i2c, PWR_MGMT_1, 0)


def read_accel_g(i2c):
    ax = read_word(i2c, ACCEL_XOUT_H) / 16384.0
    ay = read_word(i2c, ACCEL_XOUT_H + 2) / 16384.0
    az = read_word(i2c, ACCEL_XOUT_H + 4) / 16384.0
    return ax, ay, az


# === WiFi ===
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(SSID, PASSWORD)

print("Connecting to Wi-Fi...")
while not wlan.isconnected():
    time.sleep(0.5)
print("Connected:", wlan.ifconfig())

# Real wall clock for epoch_ms payloads (RTC defaults are often wrong without NTP).
if ntptime is not None:
    for attempt in range(3):
        try:
            ntptime.settime()
            print("NTP time sync OK:", time.localtime())
            break
        except OSError as e:
            print("NTP sync failed (attempt %s): %s" % (attempt + 1, e))
            time.sleep(1)
else:
    print("ntptime not available; timestamps may be wrong — flash a build with ntptime")

# === Initialize MPU6050 ===
i2c = init_i2c()
init_mpu6050(i2c)
print("MPU6050 initialized")

# === MQTT Setup ===
context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.verify_mode = ssl.CERT_NONE

client = None
buffer = []

try:
    # mqtt client connect with proper error handling
    client = MQTTClient(
        client_id=b'tumi_picow',
        server=MQTT_SERVER,
        port=MQTT_PORT,
        user=MQTT_USER,
        password=MQTT_PASSWORD,
        ssl=context
    )

    # Actually connect to MQTT broker
    client.connect()
    print("Connected to MQTT broker successfully!")

    while True:
        ax, ay, az = read_accel_g(i2c)
        # Publish absolute epoch milliseconds so backend can plot directly
        # without remapping ticks to server time.
        ts_ms = int(time.time() * 1000)
        buffer.append(f"{ts_ms},{ax:.3f},{ay:.3f},{az:.3f}")

        if len(buffer) >= 50:
            payload = "\n".join(buffer)
            try:
                client.publish(MQTT_TOPIC, payload.encode())
                print("Published 50 samples to MQTT")
                buffer = []
            except Exception as publish_error:
                print(f"Publish failed: {publish_error}")
                # Clear buffer to prevent memory issues
                buffer = []

        time.sleep(0.05)  # ~20 Hz sampling

except Exception as e:
    print("Error:", e)

finally:
    # Safe cleanup - check if client exists before disconnecting
    if client is not None:
        try:
            client.disconnect()
            print("Disconnected from MQTT")
        except Exception as disconnect_error:
            print(f"Error disconnecting MQTT: {disconnect_error}")

    try:
        wlan.disconnect()
        print("Disconnected from Wi-Fi")
    except Exception as e:
        print(f"Error disconnecting Wi-Fi: {e}")

    print("Disconnected.")