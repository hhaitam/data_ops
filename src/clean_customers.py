import pandas as pd
import numpy as np
import re
from email_validator import validate_email, EmailNotValidError
from datetime import datetime
from pathlib import Path
import sys


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

ALLOWED_TIERS = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}


def clean_email(email):
    if pd.isna(email):
        return np.nan
    try:
        return validate_email(email).email.lower()
    except EmailNotValidError:
        return np.nan


def clean_amount(value):
    if pd.isna(value):
        return np.nan
    value = re.sub(r"[€, $]", "", str(value))
    try:
        value = float(value)
        return value if value >= 0 else np.nan
    except ValueError:
        return np.nan


def clean_data(df):
    df = df.copy()

    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")

    df["full_name"] = df["full_name"].astype(str).str.strip()

    df["email"] = df["email"].apply(clean_email)

    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df.loc[df["signup_date"] > datetime.now(), "signup_date"] = pd.NaT
    df["signup_date"] = df["signup_date"].dt.date

    df["country"] = (
        df["country"]
        .astype(str)
        .str.upper()
        .replace({"NAN": "UNKNOWN", "": "UNKNOWN"})
    )

    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df.loc[(df["age"] < 18) | (df["age"] > 100), "age"] = np.nan

    df["last_purchase_amount"] = df["last_purchase_amount"].apply(clean_amount)

    df["loyalty_tier"] = (
        df["loyalty_tier"]
        .astype(str)
        .str.upper()
        .apply(lambda x: x if x in ALLOWED_TIERS else "UNKNOWN")
    )

    df = df.drop_duplicates(subset=["customer_id"])

    return df


def main():
    PROCESSED_DIR.mkdir(exist_ok=True)

    input_file = sys.argv[1] if len(sys.argv) > 1 else None

    if input_file:
        raw_files = [RAW_DIR / input_file]
    else:
        raw_files = list(RAW_DIR.glob("customers_dirty*.csv"))

    #raw_files = list(RAW_DIR.glob("customers_dirty*.csv"))

    #if not raw_files:
    #    raise FileNotFoundError("❌ No raw customer files found.")

    for file_path in raw_files:
        print(f"🔄 Processing {file_path.name}")

        df_raw = pd.read_csv(file_path)
        df_clean = clean_data(df_raw)

        output_name = file_path.stem + "_clean_v1.csv"
        output_path = PROCESSED_DIR / output_name

        df_clean.to_csv(output_path, index=False)

        print(f"✅ Output generated: {output_path}")


if __name__ == "__main__":
    main()

