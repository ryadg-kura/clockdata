# data_gen — Générateur de données montres

Simule des montres connectées et publie leurs mesures dans le topic Kafka
`clockdata`, consommé par `alert-service` et `datalake-service`.

## Format d'un message

```json
{
  "device_id": "abc123",
  "timestamp": 1714123456,
  "bpm": 72,
  "steps": 342,
  "lat": 48.7,
  "lng": 2.4
}
```

## Installation

```bash
pip install -r requirements.txt
cp .env.example .env   # puis ajuster KAFKA_BOOTSTRAP si besoin
```

## Lancement

Le broker Kafka doit tourner (voir `alert-service/docker-compose.yml`).

```bash
python producer.py                       # 3 montres, toutes les 2s
python producer.py --devices 5 --interval 1
python producer.py --anomaly-rate 0.3    # force des BPM critiques (< 40)
```

## Options

| Option                | Défaut       | Rôle                                        |
|-----------------------|--------------|---------------------------------------------|
| `--interval`          | `2`          | Secondes entre deux envois (le « X »)       |
| `--devices`           | `3`          | Nombre de montres simulées                  |
| `--topic`             | `clockdata`  | Topic Kafka de destination                  |
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP` (.env) | Adresse du broker Kafka         |
| `--anomaly-rate`      | `0.02`       | Proba d'un BPM critique < 40 (test alertes) |