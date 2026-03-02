<div align="center">

# 🚖 NYC Yellow Taxi — Data Pipeline & Cloud Data Warehouse

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.7-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?logo=googlebigquery)](https://cloud.google.com/bigquery)
[![GCP](https://img.shields.io/badge/Cloud%20Composer-GCP-FBBC04?logo=googlecloud)](https://cloud.google.com/composer)

</div>

---

## 📖 Executive Summary

Pipeline ETL automatisé et scalable conçu pour l'ingestion, le nettoyage et le chargement des données massives des taxis jaunes de New York (NYC TLC) vers un Data Warehouse d'entreprise (**Google BigQuery**).

L'orchestration est gérée par **Apache Airflow 3.x** avec une architecture modulaire garantissant l'idempotence des tâches, la résilience face aux changements de schémas sources et la qualité des données avant leur mise à disposition pour l'analytique.

---

## ⚙️ Architecture & Data Flow

```
[Source] NYC TLC Parquet Files
   │
   ├─ 1. EXTRACT  : Téléchargement asynchrone          (actions.py)
   ├─ 2. PROCESS  : Nettoyage, standardisation, typage  (process.py)
   ├─ 3. LOAD     : Upsert / Append idempotent vers GCP (load.py)
   │
[Target] Google BigQuery — Tables partitionnées
```

| Composant       | Stack                                                   |
|:----------------|:--------------------------------------------------------|
| Orchestration   | Apache Airflow 3.1.7 (Local) / Cloud Composer 3 (Prod) |
| Processing      | Python · Pandas · PyArrow                              |
| Data Warehouse  | Google BigQuery                                        |
| Infrastructure  | Google Cloud Platform (GCP)                            |

---

## 📁 Structure du projet

```
New-York-Yellow-taxi-ETL/
├── dags/
│   ├── dags.py          # Définition du DAG principal et configuration
│   ├── tasks.py         # Regroupement logique des tâches (TaskGroups)
│   ├── actions.py       # Interfaçage avec l'API source (NYC TLC)
│   ├── process.py       # Moteur de transformation et validation métier
│   └── load.py          # Connecteurs et requêtes d'insertion BigQuery
├── src/                 # Exploration et analyse des données
├── requirements.txt     # Dépendances figées
├── .gitignore
└── README.md
```

---

## 🗓️ Configuration du DAG

| Paramètre    | Valeur              | Description                                     |
|:-------------|:--------------------|:------------------------------------------------|
| `dag_id`     | `nyc_yellow_trips_etl` | Identifiant unique du pipeline               |
| `schedule`   | `0 23 * * 5`        | Exécution CRON — chaque vendredi à 23h00        |
| `start_date` | `2024-01-01`        | Date de début logique                           |
| `catchup`    | `False`             | Désactivé pour éviter la surcharge initiale     |
| `retries`    | `1` (delay: 5 min)  | Résilience aux micro-coupures réseau            |

```
DAG
 ├── Extract
 │    └── download_data      # Télécharge le Parquet mensuel TLC
 └── Load
      └── load_data          # Charge vers BigQuery
```

---

## 🧠 Défis Techniques & Décisions de Design

La donnée source (NYC TLC) présente de fortes irrégularités historiques. Le pipeline intègre plusieurs mécanismes de **Data Quality** et d'ingénierie défensive.

### 1. Évolution dynamique du schéma source (Data Drift)

Le schéma des fichiers Parquet bascule d'un système GPS (`pickup_latitude/longitude`) avant 2016 à un système de zones (`PULocationID`, `DOLocationID`) post-2016. Une fonction de détection dynamique des colonnes dans `process.py` adapte la stratégie de transformation en fonction de la signature du fichier, évitant les crashs de type `KeyError`.

### 2. Friction de typage Pandas ↔ BigQuery

Les `NaN` Pandas forcent les colonnes entières en `float64`. BigQuery (typage strict) rejette ces données lors de l'insertion dans des champs `INTEGER`. Fix appliqué :

- Conversion explicite vers `Int64` (nullable integer Pandas)
- Standardisation des `datetime64[us]` → `TIMESTAMP` GCP
- Normalisation systématique des en-têtes en `snake_case`

### 3. Validation et filtrage des outliers

Rejet systématique des courses avec `fare_amount` négatif, `trip_distance` nul, ou dates absurdes (2001, 2090 liées à des bugs de compteurs). L'intégrité métier est garantie avant facturation du stockage BigQuery.

### 4. Migration proactive vers Airflow 3

| Composant     | Airflow 2                          | Airflow 3 ✅                                        |
|:--------------|:-----------------------------------|:----------------------------------------------------|
| Opérateurs    | `airflow.operators.python`         | `airflow.providers.standard.operators.python`       |
| Task Groups   | `airflow.decorators`               | `airflow.sdk`                                       |
| Chain         | `airflow.models.baseoperator`      | `airflow.sdk.bases.operator`                        |
| Contexte      | `provide_context=True`             | Natif via `**context`                               |

---

## 🚀 Guide de Déploiement

### Local (Dev)

```bash
# 1. Clonage et environnement virtuel
git clone https://github.com/<user>/New-York-Yellow-taxi-ETL.git
cd New-York-Yellow-taxi-ETL
python3 -m venv .venv && source .venv/bin/activate

# 2. Installation des dépendances
pip install -r requirements.txt

# 3. Initialisation Airflow 3
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow standalone
```

### Production (Google Cloud Composer)

```bash
# Création de l'environnement Composer
gcloud composer environments create nyc-taxi-etl \
    --location=us-central1 \
    --image-version=composer-3-airflow-2.10.3

# Synchronisation des DAGs vers le bucket GCS
BUCKET=$(gcloud composer environments describe nyc-taxi-etl \
    --location=us-central1 \
    --format="get(config.dagGcsPrefix)")

gsutil rsync -r dags/ $BUCKET/
```

> ⚠️ Cloud Composer tourne sur **Airflow 2.x**. Les imports `airflow.sdk` présents dans ce projet doivent être adaptés pour la mise en production sur Composer.

### Permissions IAM requises

| Rôle                            | Usage                          |
|:--------------------------------|:-------------------------------|
| `roles/composer.worker`         | Exécuter les tâches Airflow    |
| `roles/logging.logWriter`       | Écrire les logs Cloud Logging  |
| `roles/monitoring.metricWriter` | Envoyer les métriques          |
| `roles/storage.objectAdmin`     | Accès DAGs / artefacts GCS     |
| `roles/bigquery.dataEditor`     | Écriture BigQuery              |

---

## 🧪 Tests & Validation

```bash
# Vérification de la compilation du DAG
python dags/dags.py
airflow dags list

# Test isolé d'une tâche (Dry Run)
airflow tasks test nyc_yellow_trips_etl extract.download_data 2024-01-01
```

---

## 📈 Roadmap

- [ ] **CI/CD** — Déploiement automatisé des DAGs sur GCS via GitHub Actions
- [ ] **ELT & dbt** — Basculer les transformations lourdes vers BigQuery via dbt pour tirer parti de la puissance de calcul du DWH
- [ ] **Data Quality** — Intégration de Great Expectations ou Soda pour bloquer le pipeline en cas d'anomalies en amont

---

<div align="center">

**Moussa SISSOKO** · Cloud Shell / GCP / Python / Airflow  
Projet Data Engineering · Mars 2026

</div>
