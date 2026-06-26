import json
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT")
ALERT_TOPIC = "alerts"


def build_email_body(data: dict) -> str:
    """Construit le corps de l'email à partir des données d'alerte."""
    ts = datetime.fromtimestamp(data.get("timestamp", 0)).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"ALERTE CARDIAQUE DÉTECTÉE\n\n"
        f"Device ID  : {data.get('device_id', 'inconnu')}\n"
        f"BPM        : {data.get('bpm', '?')}\n"
        f"Horodatage : {ts}\n"
        f"Position   : lat={data.get('lat', '?')}, lng={data.get('lng', '?')}\n\n"
        f"Action immédiate requise."
    )


def send_alert_email(data: dict) -> None:
    """Envoie un email d'alerte via SMTP Gmail."""
    body = build_email_body(data)

    msg = MIMEText(body)
    msg["Subject"] = f"[ClockData] ALERTE BPM critique — device {data.get('device_id')}"
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_RECIPIENT

    # Connexion SMTP avec chiffrement TLS (port 587)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, ALERT_RECIPIENT, msg.as_string())

    print(f"[EMAIL] Alerte envoyée à {ALERT_RECIPIENT} pour device {data.get('device_id')}")


def main():
    # Consommateur Kafka : lit les alertes détectées par consumer_alert.py
    consumer = KafkaConsumer(
        ALERT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id="alert-notifier",
    )

    print(f"[consumer_notif] En écoute sur le topic '{ALERT_TOPIC}'...")

    for message in consumer:
        data = message.value
        try:
            send_alert_email(data)
        except Exception as e:
            # On logue l'erreur mais on ne crashe pas — le pipeline continue
            print(f"[ERREUR] Envoi email échoué : {e}")
        finally:
            # On committe dans tous les cas pour éviter les doublons
            consumer.commit()


if __name__ == "__main__":
    main()
