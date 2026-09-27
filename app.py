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
# DATABASE
# =========================

DATABASE_URL = os.getenv("DATABASE_URL")


# =========================
# MOVEMENT DETECTION
# =========================

MOVEMENT_THRESHOLD_CM = 30.0

previous_distance = None


# =========================
# FLASK
# =========================

@app.route("/")
def home():
    return "ESP32 MQTT Backend is running."


# =========================
# SAVE DATA TO DATABASE
# =========================

def save_to_database(data):

    global previous_distance

    conn = None
    cur = None

    try:

        # -------------------------
        # NO ECHO
        # -------------------------

        if data == "NO_ECHO":

            distance = None
            status = "NO_ECHO"

        else:

            distance = float(data)

            # -------------------------
            # MOVEMENT DETECTION
            # -------------------------

            if previous_distance is None:

                status = "NORMAL"

            else:

                difference = abs(
                    distance - previous_distance
                )

                if difference >= MOVEMENT_THRESHOLD_CM:

                    status = "MOVEMENT"

                    print(
                        f"MOVEMENT DETECTED | "
                        f"Previous={previous_distance:.2f} cm | "
                        f"Current={distance:.2f} cm | "
                        f"Difference={difference:.2f} cm"
                    )

                else:

                    status = "NORMAL"


            # Update previous valid distance
            previous_distance = distance


        # -------------------------
        # CONNECT DATABASE
        # -------------------------

        conn = psycopg2.connect(
            DATABASE_URL
        )

        cur = conn.cursor()


        # -------------------------
        # INSERT DATA
        # -------------------------

        cur.execute(
            """
            INSERT INTO public.sensor_data
            (distance, status)
            VALUES (%s, %s)
            """,
            (
                distance,
                status
            )
        )


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
# MQTT MESSAGE
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


    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )


    client.tls_set()


    client.on_connect = on_connect
    client.on_message = on_message


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
# START FLASK
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
