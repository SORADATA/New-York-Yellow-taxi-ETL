````markdown
# 🚖 New York Yellow Taxi — ETL Pipeline

Pipeline ETL automatisé pour l'ingestion, le traitement et le chargement des données de taxis jaunes de New York (NYC TLC) vers BigQuery, orchestré avec Apache Airflow.

---

## 📁 Structure du projet

```
New-York-Yellow-taxi-ETL/
├── dags/
│   ├── dags.py          # Définition du DAG principal
│   ├── tasks.py         # Task groups (Extract, Load)
│   ├── actions.py       # Téléchargement des fichiers Parquet (TLC)
│   ├── process.py       # Nettoyage et transformation des données
│   └── load.py          # Chargement vers BigQuery
├── .venv/
├── .gitignore
└── README.md
```

---

## ⚙️ Architecture

```
[NYC TLC API] ──→ [actions.py] ──→ [process.py] ──→ [load.py] ──→ [BigQuery]
```

Orchestration : **Apache Airflow 3.1.7** (local) → déployable sur **Google Cloud Composer**

---

## 🗓️ DAG

| Paramètre    | Valeur                      |
|--------------|-----------------------------|
| `dag_id`     | `nyc_yellow_trips_etl`      |
| `schedule`   | `0 23 * * 5` (vendredi 23h) |
| `start_date` | 2024-01-01                  |
| `catchup`    | False                       |
| `retries`    | 1 (retry delay : 5 min)     |

```
Extract() └── download_data
Load()    └── load_data
```

---

## 🛠️ Installation

```bash
git clone https://github.com/<user>/New-York-Yellow-taxi-ETL.git
cd New-York-Yellow-taxi-ETL

python3 -m venv .venv
source .venv/bin/activate

pip install apache-airflow==3.1.7
pip install apache-airflow-providers-standard
pip install pandas pyarrow google-cloud-bigquery

export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow standalone
```

---

## ☁️ Déploiement Cloud Composer

```bash
gcloud composer environments create nyc-taxi-etl \
    --location=us-central1 \
    --image-version=composer-3-airflow-2.10.3

BUCKET=$(gcloud composer environments describe nyc-taxi-etl \
    --location=us-central1 \
    --format="get(config.dagGcsPrefix)")

gsutil cp dags/*.py $BUCKET/
```

> ⚠️ Cloud Composer tourne sur **Airflow 2.x** — les imports doivent être adaptés (voir section suivante).

---

## ⚠️ Difficultés rencontrées

### Évolution du schéma source

Le schéma des fichiers Parquet **change selon la période** : avant 2016, la localisation est en coordonnées GPS (`pickup_latitude/longitude`) ; après 2016, elle passe en zone ID (`PULocationID`, `DOLocationID` de 1 à 263). Une détection dynamique des colonnes présentes est nécessaire avant toute transformation.

### Types incohérents entre fichiers

Plusieurs colonnes changent de type selon le millésime : `passenger_count`, `RatecodeID`, `VendorID` et `payment_type` sont attendus en `int64` mais arrivent souvent en `float64` à cause de valeurs nulles encodées. `store_and_fwd_flag` arrive en string `"Y"/"N"` au lieu d'un booléen. Des casts explicites sont appliqués dans `process.py` après `fillna`.

### Valeurs aberrantes

Présence de `fare_amount` négatifs, de `trip_distance` à zéro, de dates hors plage (années 2001 ou 2090) et de `total_amount` supérieurs à 10 000 $. Des filtres de validation sont appliqués systématiquement avant chargement.

### Processing ↔ Data Warehouse

La friction principale vient des différences de typage entre Pandas et BigQuery :
- Les `NaN` Pandas sont des `float` — BigQuery refuse un champ `INTEGER` contenant des `NaN` → conversion en `Int64` nullable obligatoire
- Les `datetime64[us]` Pandas doivent correspondre au type `TIMESTAMP` ou `DATETIME` BigQuery
- Les noms de colonnes avec espaces ou majuscules doivent être normalisés en `snake_case`
- BigQuery applique un schéma strict — toute colonne absente ou mal typée lève une erreur au chargement

### Migration Airflow 2 → Airflow 3

| Élément                | Airflow 2                     | Airflow 3 ✅                                          |
|------------------------|-------------------------------|-------------------------------------------------------|
| `chain`                | `airflow.models.baseoperator` | `airflow.sdk.bases.operator`                          |
| `task_group`           | `airflow.decorators`          | `airflow.sdk`                                         |
| `PythonOperator`       | `airflow.operators.python`    | `airflow.providers.standard.operators.python`         |
| `provide_context=True` | ✅ Supporté                   | ❌ Supprimé — utiliser `**context` directement        |
| `email_on_failure`     | Dans `default_args`           | ❌ Deprecated → migrer vers `SmtpNotifier`            |

---

## 🧪 Validation

```bash
python dags/dags.py
airflow dags list
airflow tasks test nyc_yellow_trips_etl extract.download_data 2024-01-01
```

---

## 👤 Auteur

**Moussa SISSOKO** — Cloud Shell / GCP  
Projet ETL — Données NYC TLC Yellow Taxi — Mars 2026
````