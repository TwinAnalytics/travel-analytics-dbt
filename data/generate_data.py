"""
Generate synthetic travel & expense data for the Perk Analytics Engineering project.
Simulates a SaaS travel management platform similar to TravelPerk/Perk.
"""

import numpy as np
import pandas as pd
from pathlib import Path

SEED = 42
rng = np.random.default_rng(SEED)

OUTPUT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

CITIES = [
    ("Barcelona", "Spain"), ("London", "United Kingdom"), ("Berlin", "Germany"),
    ("Paris", "France"), ("Amsterdam", "Netherlands"), ("Madrid", "Spain"),
    ("Munich", "Germany"), ("Rome", "Italy"), ("Vienna", "Austria"),
    ("Lisbon", "Portugal"), ("New York", "United States"), ("San Francisco", "United States"),
    ("Chicago", "United States"), ("Toronto", "Canada"), ("Singapore", "Singapore"),
    ("Tokyo", "Japan"), ("Sydney", "Australia"), ("Dubai", "UAE"),
    ("Stockholm", "Sweden"), ("Copenhagen", "Denmark"), ("Oslo", "Norway"),
    ("Zurich", "Switzerland"), ("Brussels", "Belgium"), ("Warsaw", "Poland"),
    ("Prague", "Czech Republic"),
]

DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Finance", "HR", "Operations", "Product", "Legal"]
SENIORITIES = ["junior", "mid", "senior", "manager", "director"]
SENIORITY_WEIGHTS = [0.20, 0.30, 0.25, 0.18, 0.07]

INDUSTRIES = ["Technology", "Finance", "Healthcare", "Retail", "Manufacturing",
              "Media", "Consulting", "Education", "Logistics", "Energy"]
SIZE_TIERS = ["SMB", "Mid-Market", "Enterprise"]

COMPANY_NAMES = [
    "Acme Corp", "Globex", "Initech", "Umbrella Ltd", "Hooli", "Pied Piper",
    "Dunder Mifflin", "Vandelay Industries", "Bluth Company", "Soylent Corp",
    "Massive Dynamic", "Oceanic Airlines", "Stark Industries", "Wayne Enterprises",
    "LexCorp", "Cyberdyne Systems", "Weyland-Yutani", "Rekall Inc", "OCP",
    "Tyrell Corporation", "Oscorp", "Aperture Science", "Black Mesa",
    "Virtucon", "Spectre", "Gekko & Co", "Nakatomi Trading", "Ewing Oil",
    "Blue Bell Ice Cream", "Prestige Worldwide", "Hammertime LLC", "Digital Storm",
    "CloudNine Software", "NexGen Analytics", "DataFlow Systems", "TechBridge",
    "SkyLine Ventures", "PeakOps", "NorthStar Consulting", "GreenLeaf Solutions",
    "RedRock Capital", "BlueSky Media", "IronClad Security", "GoldPath Finance",
    "SilverWave Tech", "CopperLeaf", "Titanium Works", "QuickSilver Labs",
    "NanoForge", "QuantumLeap Inc",
]

CURRENCIES = ["EUR", "USD", "GBP"]
CURRENCY_WEIGHTS = [0.55, 0.30, 0.15]


# ---------------------------------------------------------------------------
# Companies (50 rows)
# ---------------------------------------------------------------------------

def generate_companies(n: int = 50) -> pd.DataFrame:
    size_tiers = rng.choice(SIZE_TIERS, size=n, p=[0.40, 0.35, 0.25])
    mrr_map = {"SMB": (500, 2_000), "Mid-Market": (2_000, 10_000), "Enterprise": (10_000, 50_000)}

    monthly_mrr = np.array([
        int(rng.integers(mrr_map[t][0], mrr_map[t][1]))
        for t in size_tiers
    ])

    contract_starts = pd.to_datetime("2020-01-01") + pd.to_timedelta(
        rng.integers(0, 365 * 4, size=n), unit="D"
    )

    countries = rng.choice(
        ["Spain", "United Kingdom", "Germany", "France", "Netherlands",
         "United States", "Sweden", "Switzerland", "Italy", "Portugal"],
        size=n,
    )

    df = pd.DataFrame({
        "company_id": [f"C{str(i).zfill(4)}" for i in range(1, n + 1)],
        "company_name": COMPANY_NAMES[:n],
        "industry": rng.choice(INDUSTRIES, size=n),
        "size_tier": size_tiers,
        "country": countries,
        "contract_start": contract_starts.strftime("%Y-%m-%d"),
        "monthly_mrr_eur": monthly_mrr,
    })
    return df


# ---------------------------------------------------------------------------
# Employees (500 rows)
# ---------------------------------------------------------------------------

def generate_employees(companies: pd.DataFrame, n: int = 500) -> pd.DataFrame:
    # Weight employees toward larger companies
    tier_weights = companies["size_tier"].map(
        {"SMB": 1, "Mid-Market": 3, "Enterprise": 8}
    ).values.astype(float)
    tier_weights /= tier_weights.sum()

    company_ids = rng.choice(companies["company_id"], size=n, p=tier_weights)
    seniorities = rng.choice(SENIORITIES, size=n, p=SENIORITY_WEIGHTS)

    start_dates = pd.to_datetime("2018-01-01") + pd.to_timedelta(
        rng.integers(0, 365 * 6, size=n), unit="D"
    )

    df = pd.DataFrame({
        "employee_id": [f"E{str(i).zfill(5)}" for i in range(1, n + 1)],
        "company_id": company_ids,
        "department": rng.choice(DEPARTMENTS, size=n),
        "seniority": seniorities,
        "country": rng.choice(
            ["Spain", "United Kingdom", "Germany", "France", "Netherlands",
             "United States", "Sweden", "Switzerland", "Italy", "Portugal"],
            size=n,
        ),
        "start_date": start_dates.strftime("%Y-%m-%d"),
        "is_active": rng.choice([True, False], size=n, p=[0.88, 0.12]),
    })
    return df


# ---------------------------------------------------------------------------
# Bookings (5,000 rows)
# ---------------------------------------------------------------------------

def generate_bookings(employees: pd.DataFrame, n: int = 5_000) -> pd.DataFrame:
    employee_ids = rng.choice(employees["employee_id"], size=n)
    company_ids = employees.set_index("employee_id").loc[employee_ids, "company_id"].values

    booking_dates = pd.to_datetime("2023-01-01") + pd.to_timedelta(
        rng.integers(0, 365 * 2, size=n), unit="D"
    )

    advance_days = rng.integers(0, 60, size=n)
    travel_dates = booking_dates + pd.to_timedelta(advance_days, unit="D")

    city_idx = rng.integers(0, len(CITIES), size=n)
    dest_cities = [CITIES[i][0] for i in city_idx]
    dest_countries = [CITIES[i][1] for i in city_idx]

    trip_types = rng.choice(["flight", "hotel", "train", "car"], size=n, p=[0.45, 0.30, 0.18, 0.07])
    currencies = rng.choice(CURRENCIES, size=n, p=CURRENCY_WEIGHTS)

    # Amount ranges by trip type (in cents)
    amount_ranges = {
        "flight":  (15_000, 120_000),
        "hotel":   (8_000,  60_000),
        "train":   (2_000,  20_000),
        "car":     (3_000,  25_000),
    }
    amount_cents = np.array([
        int(rng.integers(amount_ranges[t][0], amount_ranges[t][1]))
        for t in trip_types
    ])

    statuses = rng.choice(["confirmed", "cancelled", "pending"], size=n, p=[0.75, 0.15, 0.10])
    policy_compliant = rng.choice([True, False], size=n, p=[0.82, 0.18])

    df = pd.DataFrame({
        "booking_id": [f"B{str(i).zfill(6)}" for i in range(1, n + 1)],
        "employee_id": employee_ids,
        "company_id": company_ids,
        "booking_date": pd.Series(booking_dates).dt.strftime("%Y-%m-%d").values,
        "travel_date": pd.Series(travel_dates).dt.strftime("%Y-%m-%d").values,
        "destination_city": dest_cities,
        "destination_country": dest_countries,
        "trip_type": trip_types,
        "amount_cents": amount_cents,
        "currency": currencies,
        "status": statuses,
        "advance_days": advance_days,
        "policy_compliant": policy_compliant,
    })
    return df


# ---------------------------------------------------------------------------
# Expenses (8,000 rows)
# ---------------------------------------------------------------------------

def generate_expenses(employees: pd.DataFrame, n: int = 8_000) -> pd.DataFrame:
    employee_ids = rng.choice(employees["employee_id"], size=n)
    company_ids = employees.set_index("employee_id").loc[employee_ids, "company_id"].values

    expense_dates = pd.to_datetime("2023-01-01") + pd.to_timedelta(
        rng.integers(0, 365 * 2, size=n), unit="D"
    )

    categories = rng.choice(
        ["meals", "transport", "accommodation", "entertainment", "other"],
        size=n,
        p=[0.30, 0.25, 0.20, 0.15, 0.10],
    )

    amount_ranges = {
        "meals":         (500,   8_000),
        "transport":     (1_000, 15_000),
        "accommodation": (5_000, 50_000),
        "entertainment": (2_000, 20_000),
        "other":         (500,   10_000),
    }
    amount_cents = np.array([
        int(rng.integers(amount_ranges[c][0], amount_ranges[c][1]))
        for c in categories
    ])

    currencies = rng.choice(CURRENCIES, size=n, p=CURRENCY_WEIGHTS)
    statuses = rng.choice(["approved", "rejected", "pending"], size=n, p=[0.70, 0.15, 0.15])
    receipt_attached = rng.choice([True, False], size=n, p=[0.85, 0.15])
    policy_compliant = rng.choice([True, False], size=n, p=[0.80, 0.20])

    df = pd.DataFrame({
        "expense_id": [f"X{str(i).zfill(6)}" for i in range(1, n + 1)],
        "employee_id": employee_ids,
        "company_id": company_ids,
        "expense_date": pd.Series(expense_dates).dt.strftime("%Y-%m-%d").values,
        "category": categories,
        "amount_cents": amount_cents,
        "currency": currencies,
        "status": statuses,
        "receipt_attached": receipt_attached,
        "policy_compliant": policy_compliant,
    })
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating synthetic Perk travel & spend data...")

    companies = generate_companies(50)
    companies.to_csv(OUTPUT_DIR / "companies.csv", index=False)
    print(f"  companies.csv       — {len(companies):,} rows")

    employees = generate_employees(companies, 500)
    employees.to_csv(OUTPUT_DIR / "employees.csv", index=False)
    print(f"  employees.csv       — {len(employees):,} rows")

    bookings = generate_bookings(employees, 5_000)
    bookings.to_csv(OUTPUT_DIR / "bookings.csv", index=False)
    print(f"  bookings.csv        — {len(bookings):,} rows")

    expenses = generate_expenses(employees, 8_000)
    expenses.to_csv(OUTPUT_DIR / "expenses.csv", index=False)
    print(f"  expenses.csv        — {len(expenses):,} rows")

    print("Done. CSVs written to:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
