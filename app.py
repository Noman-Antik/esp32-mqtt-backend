import os
import time
import threading
import paho.mqtt.client as mqtt
from flask import Flask

app = Flask(__name__)

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "room/hc_sr04/distance")


@app.route("/")
def home():
    return "ESP32 MQTT Backend is running."


def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Connected to HiveMQ:", reason_code)

    if reason_code == 0:
        client.subscribe(MQTT_TOPIC, qos=1)
        print("Subscribed to:", MQTT_TOPIC)


def on_message(client, userdata, msg):
    data = msg.payload.decode("utf-8", errors="replace")
    print(f"MQTT DATA | {msg.topic} | {data}")


def mqtt_worker():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set()

    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            print("Connecting to HiveMQ...")
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as e:
            print("MQTT error:", e)
            time.sleep(10)


threading.Thread(target=mqtt_worker, daemon=True).start()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
