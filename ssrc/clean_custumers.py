import pandas as pd
import numpy as np
import re
from email_validator import validate_email, EmailNotValidError
from datetime import datetime
from pathlib import Path

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
OUTPUT_FILE = PROCESSED_DIR / "customers_clean_v1.csv"

ALLOWED_TIERS = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}


def load_raw_data():
    dfs = []
    for file in RAW_DIR.glob("customers_dirty*.csv"):
        df = pd.read_csv(file)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def clean_email(email):
    if pd.isna(email):
        return np.nan
    email = str(email).strip().lower()  # remove spaces & lowercase
    # Simple regex validation (lenient)
    if re.match(r"^[\w\.\+\-]+@[\w\.\-]+\.[a-z]{2,}$", email):
        return email
    else:
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

    # --- ID ---
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")

    # --- Name ---
    df["full_name"] = df["full_name"].astype(str).str.strip()
    df.loc[df["full_name"] == "", "full_name"] = np.nan  # empty strings → NaN

    # --- Email ---
    df["email"] = df["email"].apply(clean_email)

    # --- Drop rows where name OR email is missing (NEW RULE) ---
    df = df.dropna(subset=["full_name", "email"])

    # --- Signup Date ---
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df.loc[df["signup_date"] > datetime.now(), "signup_date"] = pd.NaT
    df["signup_date"] = df["signup_date"].dt.date

    # --- Country ---
    df["country"] = (
        df["country"]
        .astype(str)
        .str.upper()
        .replace({"NAN": "UNKNOWN", "": "UNKNOWN"})
    )

    # --- Age ---
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df.loc[(df["age"] < 18) | (df["age"] > 100), "age"] = np.nan

    # --- Last purchase amount ---
    df["last_purchase_amount"] = df["last_purchase_amount"].apply(clean_amount)

    # --- Loyalty tier ---
    df["loyalty_tier"] = (
        df["loyalty_tier"]
        .astype(str)
        .str.upper()
        .apply(lambda x: x if x in ALLOWED_TIERS else "UNKNOWN")
    )

    # --- Remove duplicates by customer_id ---
    df = df.drop_duplicates(subset=["customer_id"])

    return df



def main():
    PROCESSED_DIR.mkdir(exist_ok=True)

    raw_df = load_raw_data()
    clean_df = clean_data(raw_df)

    clean_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Pipeline terminé. Fichier généré : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
