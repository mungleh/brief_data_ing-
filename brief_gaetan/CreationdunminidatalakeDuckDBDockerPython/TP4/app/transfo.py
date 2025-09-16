import pandas as pd

# List of CSV files
csv_files = ["TP4/app/data/datacsv/2016.csv", "TP4/app/data/datacsv/2018.csv", "TP4/app/data/datacsv/2020.csv", "TP4/app/data/datacsv/2022.csv", "TP4/app/data/datacsv/2024.csv"]

for csv_file in csv_files:
    # Read CSV
    df = pd.read_csv(csv_file)

    # Ensure dt_creation is datetime (handles fractional seconds too)
    df["date_debut_edition"] = pd.to_datetime(df["date_debut_edition"], errors="coerce")

    # Extract parts
    df["year"] = df["date_debut_edition"].dt.year
    df["month"] = df["date_debut_edition"].dt.month
    df["day"] = df["date_debut_edition"].dt.day

    # Build output filename
    parquet_file = csv_file.replace(".csv", ".parquet")

    # Save to Parquet
    df.to_parquet(parquet_file, engine="pyarrow", index=False, compression="snappy")
