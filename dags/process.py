import pandas as pd

def process_nyc_yellow_taxi(df:pd.DataFrame)-> pd.DataFrame:
    """
    T : Transform DataFrame for ETL
    """
    # Suppression des NAN
    df = df.dropna(subset=["passenger_count", "RatecodeID", 
                               "store_and_fwd_flag", "congestion_surcharge", 
                               "airport_fee"])
    # Renommage 
    df = df.rename(columns={
        "VendorID":"id_vendor",
        "RatecodeID":"id_ratecode",
        "PULocationID":"id_pulocation",
        "DOLocationID":"id_dolocation",
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