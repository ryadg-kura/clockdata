import argparse
import json
import os
import random
import signal
import sys
import time
import uuid

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092") # adresse du broker (a verifier / changer en fonction)
TOPIC = "clockdata"  # topic consommé par alert-service et datalake-service

# Point géographique de référence autour duquel les montres "bougent"
# (par défaut : région parisienne)
BASE_LAT = 48.7
BASE_LNG = 2.4


class Watch:
    """Représente une montre simulée avec un état qui évolue dans le temps."""

    def __init__(self, device_id=None):
        self.device_id = device_id or uuid.uuid4().hex[:6]
        self.bpm = random.randint(60, 90)
        self.steps = random.randint(0, 5000)
        self.lat = BASE_LAT + random.uniform(-0.05, 0.05)
        self.lng = BASE_LNG + random.uniform(-0.05, 0.05)

    def tick(self, anomaly_rate=0.0):
        """Fait évoluer l'état de la montre de façon réaliste (marche aléatoire).

        anomaly_rate : probabilité (entre 0 et 1) que ce tick génère un BPM
        critique (< 40), pour pouvoir tester le Consumer Alertes en aval.
        """
        if anomaly_rate and random.random() < anomaly_rate:
            # Anomalie : rythme cardiaque critique -> doit déclencher l'alerte
            self.bpm = random.randint(25, 39)
        else:
            # Le rythme cardiaque varie doucement et reste dans des bornes plausibles
            self.bpm = max(50, min(180, self.bpm + random.randint(-4, 4)))
        # Les pas ne font qu'augmenter
        self.steps += random.randint(0, 25)
        # Petit déplacement géographique
        self.lat = round(self.lat + random.uniform(-0.0005, 0.0005), 6)
        self.lng = round(self.lng + random.uniform(-0.0005, 0.0005), 6)

    def to_message(self):
        """Construit le message au format attendu par le schéma du data lake."""
        return {
            "device_id": self.device_id,
            "timestamp": int(time.time()),
            "bpm": self.bpm,
            "steps": self.steps,
            "lat": round(self.lat, 4),
            "lng": round(self.lng, 4),
        }


def build_producer(bootstrap_servers):
    """Crée le producteur Kafka. Les messages sont sérialisés en JSON."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Génère de fausses données de montres et les publie "
                    f"dans le topic Kafka '{TOPIC}'."
    )
    parser.add_argument(
        "--interval", type=float, default=2.0,
        help="Intervalle en secondes entre deux envois (défaut : 2).",
    )
    parser.add_argument(
        "--devices", type=int, default=3,
        help="Nombre de montres simulées (défaut : 3).",
    )
    parser.add_argument(
        "--bootstrap-servers", default=KAFKA_BOOTSTRAP,
        help=f"Adresse du/des broker(s) Kafka (défaut : {KAFKA_BOOTSTRAP}, "
             "issu de KAFKA_BOOTSTRAP dans .env).",
    )
    parser.add_argument(
        "--topic", default=TOPIC,
        help=f"Nom du topic Kafka (défaut : {TOPIC}).",
    )
    parser.add_argument(
        "--anomaly-rate", type=float, default=0.02,
        help="Probabilité [0-1] qu'un message ait un BPM critique < 40, "
             "pour tester le consumer d'alertes (défaut : 0.02).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        producer = build_producer(args.bootstrap_servers)
    except NoBrokersAvailable:
        print(
            f"[ERREUR] Impossible de joindre Kafka sur {args.bootstrap_servers}. ",
            file=sys.stderr,
        )
        sys.exit(1)

    watches = [Watch() for _ in range(args.devices)]

    # Arrêt propre sur Ctrl+C
    running = {"on": True}

    def stop(signum, frame):
        print("\n[INFO] Arrêt demandé, fermeture du producer...")
        running["on"] = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(
        f"[INFO] Envoi vers le topic '{args.topic}' "
        f"({args.devices} montre(s), toutes les {args.interval}s). "
        "Ctrl+C pour arrêter."
    )

    try:
        while running["on"]:
            for watch in watches:
                watch.tick(anomaly_rate=args.anomaly_rate)
                message = watch.to_message()
                producer.send(args.topic, key=watch.device_id, value=message)
                print(json.dumps(message))

            producer.flush()
            time.sleep(args.interval)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()