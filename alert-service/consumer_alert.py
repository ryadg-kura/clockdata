import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = "clockdata"
OUTPUT_TOPIC = "alerts"


def should_alert(bpm: int) -> bool:
    """Retourne True si le BPM est critique (inférieur à 40)."""
    return bpm < 40


def main():
    # Consommateur Kafka : lit les données des montres en temps réel
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",   # on ne retraite pas les anciens messages
        enable_auto_commit=False,     # on committe manuellement après traitement
        group_id="alert-detector",
    )

    # Producteur Kafka : publie les alertes dans le topic dédié
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[consumer_alert] En écoute sur le topic '{INPUT_TOPIC}'...")

    for message in consumer:
        data = message.value
        bpm = data.get("bpm", 999)

        if should_alert(bpm):
            print(f"[ALERTE] BPM critique : {bpm} — device {data.get('device_id')}")
            # On republie le message entier dans le topic alertes
            producer.send(OUTPUT_TOPIC, value=data)
            producer.flush()

        # On confirme la lecture du message (évite de le retraiter si crash)
        consumer.commit()


if __name__ == "__main__":
    main()
