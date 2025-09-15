import pandas as pd

# List of CSV files
csv_files = ["TP4/app/data/2016.csv", "TP4/app/data/2018.csv", "TP4/app/data/2020.csv", "TP4/app/data/2022.csv"]

for csv_file in csv_files:
    # Read CSV
    df = pd.read_csv(csv_file)

    # Ensure dt_creation is datetime (handles fractional seconds too)
    df["dt_creation"] = pd.to_datetime(df["dt_creation"], errors="coerce")

    # Extract parts
    df["year"] = df["dt_creation"].dt.year
    df["month"] = df["dt_creation"].dt.month
    df["day"] = df["dt_creation"].dt.day

    # Build output filename
    parquet_file = csv_file.replace(".csv", ".parquet")

    # Save to Parquet
    df.to_parquet(parquet_file, engine="pyarrow", index=False, compression="snappy")
