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
![Automation](https://img.shields.io/badge/Makefile-Automation-lightgrey)

---

## 📊 Données utilisées

- Issues du *Smart Building System Dataset* (UC Berkeley).  
- 255 capteurs IoT réparties sur 51 salles (5 capteurs/room).  
- 5 types de capteurs : température, humidité, CO₂, luminosité, mouvement PIR.  
- Fréquence d’échantillonnage : 5 à 10 secondes selon le capteur.  
- Période couverte : 23 au 31 août 2013.

**Source du dateset :**  
https://www.kaggle.com/datasets/mdelfavero/smart-building-system

---

## 🧬 Description pipeline :

- Le producer lit automatiquement les CSV stockés sur une couche /raw d’un data lake AWS S3, rejoue les mesures en flux continu, puis les envoie dans les topics Kafka. Le script de replay (producer) offre le choix entre deux modes : soit un débit fixe (rate), soit le respect des intervalles réels du dataset (timewarp), ce mode simulant le comportement réel des capteurs. Les paramètres des modes sont modifiables depuis le fichier .env.
- Le consumer Spark streaming lit les messages depuis les topics Kafka ,en format Json, et les écrit en format parquet sur la couche bronze. Les données sont partitionnées par (date, room et type de capteur. Un checkpoint garantit la reprise du streaming en cas des pannes.
- Un premier job spark " micro batch" enrichit les données Bronze (room, sensor, qualité), calcule event_date et les écrit en Parquet. Le résultat est une couche Silver propre, partitioné par date , rapide et prete pour l'analyse.
- Un deuxième job Spark "batch" agrège les données Silver en KPIs horaires et journalières par room, et les sauvegarde en tables Parquet prêtes à l’usage sur la couche Gold. Airflow  l'exécution de ce job quotidiennement.
- Une API "FastApi" expose les données Silver du datalake S3 via des endpoints permettant de filtrer mesures et métadonnées. Elle offre un accès rapide aux lectures nettoyées par date, room et type de capteur. C’est la couche d’accès technique aux données brutes enrichies.
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


[<img src="docs/screenshots/s3-1.png" width="150"/>](docs/screenshots/s3-1.png)
[<img src="docs/screenshots/s3-2.png" width="150"/>](docs/screenshots/s3-2.png)
[<img src="docs/screenshots/s3-4.png" width="150"/>](docs/screenshots/s3-4.png)

[<img src="docs/screenshots/kafka-brocker-1.png" width="150"/>](docs/screenshots/kafka-brocker-1.png)
[<img src="docs/screenshots/kafka-brocker.png" width="150"/>](docs/screenshots/kafka-brocker.png)

[<img src="docs/screenshots/s3.png" width="150"/>](docs/screenshots/s3.png)
[<img src="docs/screenshots/saprk.png" width="150"/>](docs/screenshots/saprk.png)
[<img src="docs/screenshots/spark_2.png" width="150"/>](docs/screenshots/spark_2.png)

[<img src="docs/screenshots/api-1.png" width="150"/>](docs/screenshots/api-1.png)
[<img src="docs/screenshots/api-3.png" width="150"/>](docs/screenshots/api-3.png)
[<img src="docs/screenshots/api.png" width="150"/>](docs/screenshots/api.png)

[<img src="docs/screenshots/vscode.png" width="150"/>](docs/screenshots/vscode.png)

---
## 👨‍💻 Auteur

**Abderraouf Boukarma**  

📧 **Email :** [boukarmaabderraouf@gmail.com](mailto:boukarma.abderraouf@gmail.com)  
🌐 **LinkedIn :** [linkedin.com/in/abderraouf-boukarma](https://www.linkedin.com/in/abderraouf-boukarma)  
💻 **GitHub :** [github.com/AbderraoufBou14](https://github.com/AbderraoufBou14)
