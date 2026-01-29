import numpy as np
import pandas as pd
from src.clean_customers import clean_email, clean_data

def test_clean_email_valid():
    email = "John.Doe@Email.COM"
    result = clean_email(email)
    assert result == "john.doe@email.com"

def test_clean_email_invalid():
    email = "not-an-email"
    result = clean_email(email)
    assert np.isnan(result)

def test_age_out_of_bounds():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "full_name": ["A", "B"],
        "email": ["a@test.com", "b@test.com"],
        "signup_date": ["2020-01-01", "2020-01-01"],
        "country": ["FR", "FR"],
        "age": [10, 150],  # invalid
        "last_purchase_amount": [100, 200],
        "loyalty_tier": ["GOLD", "SILVER"]
    })

    cleaned = clean_data(df)

    assert cleaned["age"].isna().all()

def test_purchase_amount_negative_or_symbol():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "full_name": ["A", "B"],
        "email": ["a@test.com", "b@test.com"],
        "signup_date": ["2020-01-01", "2020-01-01"],
        "country": ["FR", "FR"],
        "age": [30, 40],
        "last_purchase_amount": ["€1,200", -50],
        "loyalty_tier": ["GOLD", "SILVER"]
    })

    cleaned = clean_data(df)

    assert cleaned.loc[0, "last_purchase_amount"] == 1200
    assert np.isnan(cleaned.loc[1, "last_purchase_amount"])

def test_loyalty_tier_standardization():
    df = pd.DataFrame({
        "customer_id": [1, 2],
        "full_name": ["A", "B"],
        "email": ["a@test.com", "b@test.com"],
        "signup_date": ["2020-01-01", "2020-01-01"],
        "country": ["FR", "FR"],
        "age": [30, 40],
        "last_purchase_amount": [100, 200],
        "loyalty_tier": ["gold", "diamond"]
    })

    cleaned = clean_data(df)

    assert cleaned.loc[0, "loyalty_tier"] == "GOLD"
    assert cleaned.loc[1, "loyalty_tier"] == "UNKNOWN"

def test_duplicate_customers_removed():
    df = pd.DataFrame({
        "customer_id": [1, 1],
        "full_name": ["A", "A"],
        "email": ["a@test.com", "a@test.com"],
        "signup_date": ["2020-01-01", "2020-01-01"],
        "country": ["FR", "FR"],
        "age": [30, 30],
        "last_purchase_amount": [100, 100],
        "loyalty_tier": ["GOLD", "GOLD"]
    })

    cleaned = clean_data(df)

    assert len(cleaned) == 1
