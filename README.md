# ClockData
_"Data to save lives"_

## Membres
- Mehdi AZOUZ
- Hani BOUZIDA
- Ryad GAZENAY
- Enzo FRANCIL

## Sujet 
ClockData est une entreprise qui fabrique des montres lifestyle pour le bien être de ses clients. Etant une entreprise soucieuse de la santé de ses clients, elle décide de rajouter un nouveau service d'analyse des statistiques de santé qui sera affiché via un dashboard où les clients pourront voir leurs statistiques de BPM, pas par jour et d'autres informations utiles pour leurs bien être. Ainsi qu'un service d'alerte d'urgence qui peut alerter un proche et soi-même pour prévenir d'un arrêt cardiaque à venir via un système de notification.

## Questions préliminaires

### 1.a Contraintes du stockage pour les statistiques
- Stocker un grand volume de données (200Go/jour, millions de devices)
- Pouvoir scaler horizontalement quand le nombre de montres augmente
- Supporter des lectures analytiques rapides (agrégations, moyennes, historiques)
- Ne pas perdre de données
- Stocker les données sur le long terme

### 1.b Composants nécessaires
Il faut un stockage distribué de type data lake car il répond à l'ensemble des critères listés ci-dessus : scalabilité horizontale, stockage long terme et coût efficace pour de grands volumes.

### 2.a Contrainte pour le service Alerte
La contrainte principale est la latence, si quelqu'un fait un arrêt cardiaque, l'alerte doit partir en quelques secondes maximum. On ne peut pas se permettre d'attendre qu'un batch processing tourne ce qui pourrait avoir des conséquences graves pour l'utilisateur.

### 2.b Composant à choisir
Un stream distribué avec un consumer dédié aux alertes. Le stream permet de traiter chaque message dès qu'il arrive, sans attendre. Le consumer évalue immédiatement si le BPM est critique et déclenche la notification.

# Architecture

```mermaid
flowchart TD

  subgraph Legende
    S[(Storage)]:::storage
    P[Processing]:::process
    ST{{"Stream"}}:::stream
  end

  subgraph Devices["Objets IOT"]
    D["Montres connectées\n(BPM, pas, GPS, device_id, ...)"]:::device
  end

  subgraph Ingestion
    MQ{{"Stream distribué"}}:::stream
  end

  subgraph Consumers["Consumers temps réel"]
    CA[Consumer Alertes Détection BPM < 40]:::alert
    CDL[Consumer Data Lake]:::process
  end

  subgraph Alerting["Service Alerte"]
    SA{{"Stream d'alertes"}}:::stream
    CN[Consumer Notification email, SMS]:::alert
  end

  subgraph DataLake["Data Lake"]
    BRONZE[(Bronze: Données brutes Format: JSON)]:::bronze
    SILVER[(Silver: Données nettoyées Format: AVRO)]:::silver
    GOLD[(Gold: Données agrégées Format: PARQUET)]:::gold
  end

  subgraph Processing["Batch Processing"]
    P1[Nettoyage: JSON → AVRO]:::process
    P2[Agrégation: AVRO → PARQUET]:::process
  end

  subgraph Dashboard["Dashboard"]
    VIZ[Visualisation\ndes statistiques]:::process
  end
  D -->|"données toutes les X secondes"| MQ
  MQ --> CA
  MQ --> CDL
  CA --> |"alerte détectée"| SA
  CDL -->|"JSON brut"| BRONZE
  BRONZE --> P1
  P1 -->|"AVRO nettoyé"| SILVER
  SILVER --> P2
  SA --> CN
  P2 -->|"PARQUET agrégé"| GOLD
  GOLD --> VIZ
  
classDef storage fill:#E1F5EE,stroke:#0F6E56,color:#04342C,stroke-width:1.5px
classDef process fill:#EEEDFE,stroke:#534AB7,color:#26215C,stroke-width:1.5px
classDef stream fill:#FAEEDA,stroke:#854F0B,color:#412402,stroke-width:1.5px
classDef device fill:#E6F1FB,stroke:#185FA5,color:#042C53,stroke-width:1.5px
classDef alert fill:#FCEBEB,stroke:#A32D2D,color:#501313,stroke-width:2px
classDef bronze fill:#F5C4B3,stroke:#993C1D,color:#4A1B0C,stroke-width:1.5px
classDef silver fill:#D3D1C7,stroke:#5F5E5A,color:#2C2C2A,stroke-width:1.5px
classDef gold fill:#FAC775,stroke:#854F0B,color:#412402,stroke-width:1.5px
```
