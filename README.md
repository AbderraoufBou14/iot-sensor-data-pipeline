# Pipeline de données streaming temps réel – IoT Smart Building (Kafka, Spark Streaming, S3 Datalake, FastAPI)

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Spark-Structured%20Streaming-E25A1C?logo=apachespark)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-Containerization-0db7ed?logo=docker)
![S3](https://img.shields.io/badge/S3-Data%20Lake-569A31?logo=amazon-aws)
![Parquet](https://img.shields.io/badge/Parquet-Columnar-0E6FBF?logo=apache)
![Scikit-learn](https://img.shields.io/badge/ML-Scikit--Learn-F7931E?logo=scikitlearn)
![Git](https://img.shields.io/badge/Git-Version%20Control-F05032?logo=git&logoColor=white)

---

## 📊 Données utilisées

- Issues du *Smart Building System Dataset* (UC Berkeley).  
- 255 séries temporelles provenant de capteurs IoT dans 51 salles.  
- 5 types de mesures : température, humidité, CO₂, luminosité, mouvement PIR.  
- Fréquence d’échantillonnage : 5 à 10 secondes selon le capteur.  
- Période couverte : 23 au 31 août 2013.

**Source Kaggle :**  
https://www.kaggle.com/datasets/mdelfavero/smart-building-system

---

## 🧬 Description pipeline :

- Le producer lit automatiquement les CSV S3 de chaque capteur et room, rejoue les mesures en flux continu, puis les envoie dans les topics Kafka en respectant soit un débit fixe (rate), soit les intervalles réels du dataset (timewarp), parametres a modifer depuis le .env
- Le consumer Spark streaming lit les messages Kafka en Json et les écrit en format parquet sur la couche bronze, partitionnée par (date, room et type de capteur. Un checkpoint garantit la reprise du streaming en cas des pannes.
- Un premier job spark enrichit les données Bronze (room, sensor, qualité), calcule event_date et les écrit en Parquet. Le résultat est une couche Silver propre, partitioné par date , rapide et prete pour l'analyse.
- Un deuxième job Spark agrège les données Silver en KPIs horaires et journalières par room, et les sauvegarde en tables Parquet prêtes à l’usage sur la couche Gold. Airflow orchestre son exécution quotidienne.
---

## 🏗️ Architecture globale du pipeline

```mermaid
flowchart LR

    CSV[CSV capteurs]
    Producer[Replay Producer]

    Kafka[(Kafka Broker)]

    Consumer{{Spark Consumer Bronze}}

    Bronze[(Bronze Layer)]

    SilverJob{{Spark Silver Job}}

    Silver[(Silver Layer)]

    GoldJob{{Spark Gold Job}}

    Gold[(Gold Layer)]

    API[FastAPI API]

    CSV --> Producer --> |Json| Kafka --> Consumer -->|Parquet| Bronze --> SilverJob -->|Parquet| Silver --> GoldJob -->|Parquet| Gold
    Silver --> API
```
---

## Captures d’écran

---
## 👨‍💻 Auteur

**Abderraouf Boukarma**  

📧 **Email :** [boukarmaabderraouf@gmail.com](mailto:boukarma.abderraouf@gmail.com)  
🌐 **LinkedIn :** [linkedin.com/in/abderraouf-boukarma](https://www.linkedin.com/in/abderraouf-boukarma)  
💻 **GitHub :** [github.com/AbderraoufBou14](https://github.com/AbderraoufBou14)
