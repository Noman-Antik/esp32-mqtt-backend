import os
import time
import threading

import psycopg2
import paho.mqtt.client as mqtt
from flask import Flask


app = Flask(__name__)


# =========================
# MQTT CONFIGURATION
# =========================

MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC",
    "room/hc_sr04/distance"
)


# =========================
# SUPABASE DATABASE
# =========================

DATABASE_URL = os.getenv("DATABASE_URL")


# =========================
# FLASK HOME ROUTE
# =========================

@app.route("/")
def home():
    return "ESP32 MQTT Backend is running."


# =========================
# SAVE DATA TO SUPABASE
# =========================

def save_to_database(data):

    conn = None
    cur = None

    try:

        # Convert MQTT data
        if data == "NO_ECHO":

            distance = None
            status = "NO_ECHO"

        else:

            distance = float(data)
            status = "NORMAL"


        # Connect to PostgreSQL
        conn = psycopg2.connect(DATABASE_URL)

        cur = conn.cursor()


        # Insert sensor data
        cur.execute(
            """
            INSERT INTO public.sensor_data
            (distance, status)
            VALUES (%s, %s)
            """,
            (distance, status)
        )


        # Save changes
        conn.commit()


        print(
            f"DATABASE SAVED | "
            f"distance={distance} | "
            f"status={status}"
        )


    except Exception as e:

        print(
            f"DATABASE ERROR: {e}"
        )


    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()


# =========================
# MQTT CONNECT
# =========================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties=None
):

    print(
        "Connected to HiveMQ:",
        reason_code
    )


    if reason_code == 0:

        client.subscribe(
            MQTT_TOPIC,
            qos=1
        )

        print(
            "Subscribed to:",
            MQTT_TOPIC
        )


# =========================
# MQTT MESSAGE RECEIVED
# =========================

def on_message(
    client,
    userdata,
    msg
):

    data = msg.payload.decode(
        "utf-8",
        errors="replace"
    )


    print(
        f"MQTT DATA | "
        f"{msg.topic} | "
        f"{data}"
    )


    # Save MQTT data to Supabase
    save_to_database(data)


# =========================
# MQTT WORKER
# =========================

def mqtt_worker():

    client = mqtt.Client(
        callback_api_version=
        mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )


    # MQTT authentication
    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )


    # Enable TLS
    client.tls_set()


    # MQTT callbacks
    client.on_connect = on_connect
    client.on_message = on_message


    # Keep reconnecting if connection fails
    while True:

        try:

            print(
                "Connecting to HiveMQ..."
            )


            client.connect(
                MQTT_HOST,
                MQTT_PORT,
                keepalive=60
            )


            client.loop_forever()


        except Exception as e:

            print(
                f"MQTT ERROR: {e}"
            )


            print(
                "Retrying in 10 seconds..."
            )


            time.sleep(10)


# =========================
# START MQTT THREAD
# =========================

mqtt_thread = threading.Thread(
    target=mqtt_worker,
    daemon=True
)

mqtt_thread.start()


# =========================
# START FLASK SERVER
# =========================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )


    app.run(
        host="0.0.0.0",
        port=port
    )
