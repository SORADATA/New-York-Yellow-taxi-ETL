import pandas as pd

def process_nyc_yellow_taxi(df:pd.DataFrame)-> pd.DataFrame:
    """
    T : Transform DataFrame for ETL
    """
    #  Normalise toutes les colonnes en minuscules
    df.columns = df.columns.str.lower()
    # Colonnes manquantes
    for col in ["congestion_surcharge", "airport_fee"]:
        if col not in df.columns:
            df[col] = 0.0
    # Suppression des NAN
    df = df.dropna(subset=["passenger_count", "ratecodeid", 
                               "store_and_fwd_flag", "congestion_surcharge", 
                               "airport_fee"])
    # Renommage 
    df = df.rename(columns={
        "vendorid":"id_vendor",
        "ratecodeid":"id_ratecode",
        "pulocationid":"id_pulocation",
        "dolocationid":"id_dolocation",
    })
    # Gestion des types
    df["passenger_count"] = df["passenger_count"].astype("Int64")
    df["id_ratecode"] = df["id_ratecode"].astype("Int64")
    df["id_vendor"] = df["id_vendor"].astype("Int32")
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": True, "N": False})

    # Filtrage
    df = df[df["fare_amount"]>0]
    df = df[df["trip_distance"] > 0]
    df = df[df["passenger_count"] > 0]
    df = df[df["total_amount"] > 0]

    # COl utiles
    df["trip_duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_weekday"] = df["tpep_pickup_datetime"].dt.day_name()
    return df