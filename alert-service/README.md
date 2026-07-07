# alert-service — Service Alerte (Scala)

Consomme le topic Kafka `clockdata`, détecte les BPM critiques (< 40), republie l'alerte dans
le topic `alerts`, puis envoie un email au destinataire configuré.

## Architecture

```
[clockdata] → ConsumerAlert → filtre bpm < 40 → [alerts] → ConsumerNotif → email SMTP
```

## Format d'un message

```json
{
  "device_id": "device_001",
  "timestamp": 1714123456,
  "bpm": 35,
  "steps": 342,
  "lat": 48.7,
  "lng": 2.4
}
```

## Prérequis

- sbt installé
- Un broker Kafka déjà lancé sur `localhost:9092` (voir `simulator/` pour générer des données
  de test)

## Installation

```bash
cp .env.example .env   # puis ajuster SMTP_USER, SMTP_PASSWORD, ALERT_RECIPIENT
export $(grep -v '^#' .env | xargs)
```

L'export doit être refait dans chaque terminal où l'on lance un des deux consumers.

## Lancement

Dans deux terminaux séparés :

```bash
sbt "runMain ConsumerAlert"
```

```bash
sbt "runMain ConsumerNotif"
```

## Tests

```bash
sbt test
```

Les tests couvrent la logique pure (seuil d'alerte, parsing JSON, formatage de l'email). La
boucle Kafka et l'envoi SMTP sont vérifiés manuellement contre un broker/compte réel.
