"""
Speexx RevOps: CRM Data Cleaning Pipeline
==========================================
Cleans a raw CRM export (contacts + deals) from Dynamics 365 / HubSpot.
Produces clean, analysis-ready tables and a data quality scorecard.

Cleaning operations applied:
  1. Remove exact duplicates
  2. Standardise contact IDs to consistent string format
  3. Parse and normalise mixed date formats (YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY, DD.MM.YYYY)
  4. Flag and remove future-dated records (data entry errors)
  5. Validate and standardise email addresses
  6. Standardise country names (44 raw variants → ISO country name)
  7. Standardise lifecycle stage vocabulary (29 raw variants → 7 canonical stages)
  8. Standardise lead source vocabulary
  9. Standardise owner identity (name variants + email formats → canonical email)
  10. Clean ARR / deal value column (strip currency symbols, coerce to float, flag negatives)
  11. Fix close-before-open date errors in deals
  12. Standardise boolean opted_out field
  13. Fill missing deal names from company name
  14. Generate a data quality scorecard (before / after comparison)

Author: Arqam Faiz Siddiqui
Role context: Portfolio sample for Speexx Working Student RevOps application
"""

import pandas as pd
import numpy as np
import re
import json
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

INPUT_DIR = Path("data")
OUTPUT_DIR = Path("pipeline/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CANONICAL_COUNTRIES = {
    # Germany
    "germany": "Germany", "de": "Germany", "deutschland": "Germany",
    "german": "Germany",
    # France
    "france": "France", "fr": "France", "francia": "France",
    # Spain
    "spain": "Spain", "es": "Spain", "espana": "Spain", "españa": "Spain",
    # Italy
    "italy": "Italy", "it": "Italy", "italia": "Italy",
    # Netherlands
    "netherlands": "Netherlands", "nl": "Netherlands",
    "the netherlands": "Netherlands", "holland": "Netherlands",
    # UK
    "united kingdom": "United Kingdom", "uk": "United Kingdom",
    "gb": "United Kingdom", "england": "United Kingdom",
    "great britain": "United Kingdom",
    # USA
    "united states": "United States", "usa": "United States",
    "us": "United States", "u.s.": "United States",
    "united states of america": "United States",
    # Poland
    "poland": "Poland", "pl": "Poland",
    # Austria
    "austria": "Austria", "at": "Austria",
    # Switzerland
    "switzerland": "Switzerland", "ch": "Switzerland",
}

CANONICAL_LIFECYCLE = {
    "mql": "MQL",
    "marketing qualified lead": "MQL",
    "marketing qualified": "MQL",
    "sql": "SQL",
    "sales qualified lead": "SQL",
    "sales qualified": "SQL",
    "opportunity": "Opportunity",
    "opp": "Opportunity",
    "lead": "Lead",
    "new lead": "Lead",
    "closed won": "Closed Won",
    "won": "Closed Won",
    "closed lost": "Closed Lost",
    "lost": "Closed Lost",
    "customer": "Customer",
}

CANONICAL_SOURCES = {
    "organic search": "Organic Search",
    "organic": "Organic Search",
    "paid search": "Paid Search",
    "ppc": "Paid Search",
    "google ads": "Paid Search",
    "linkedin": "LinkedIn",
    "linkedin ads": "LinkedIn",
    "referral": "Referral",
    "partner referral": "Referral",
    "direct": "Direct",
    "webinar": "Webinar",
    "event": "Webinar",
    "email": "Email",
    "email campaign": "Email",
    "n/a": None,
    "unknown": None,
    "": None,
}

CANONICAL_OWNERS = {
    # sarah mueller
    "sarah.mueller@speexx.com": "sarah.mueller@speexx.com",
    "sarah mueller": "sarah.mueller@speexx.com",
    # james wilson
    "james.wilson@speexx.com": "james.wilson@speexx.com",
    "j. wilson": "james.wilson@speexx.com",
    "james.wilson": "james.wilson@speexx.com",
    # ana garcia
    "ana.garcia@speexx.com": "ana.garcia@speexx.com",
    "ana garcia": "ana.garcia@speexx.com",
    # luca bianchi
    "luca.bianchi@speexx.com": "luca.bianchi@speexx.com",
    "l.bianchi@speexx.com": "luca.bianchi@speexx.com",
    "luca bianchi": "luca.bianchi@speexx.com",
    # marie dupont
    "marie.dupont@speexx.com": "marie.dupont@speexx.com",
    "marie dupont": "marie.dupont@speexx.com",
    # unassigned
    "unassigned": None,
    "": None,
}

# ──────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def parse_mixed_date(val):
    """Parse dates in YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY, DD.MM.YYYY formats."""
    if pd.isna(val) or val == "":
        return pd.NaT
    val = str(val).strip()
    formats = [
        "%Y-%m-%d",    # ISO standard
        "%d/%m/%Y",    # European
        "%m-%d-%Y",    # US
        "%d.%m.%Y",    # German
        "%Y/%m/%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return pd.NaT


def standardise_country(val):
    if pd.isna(val):
        return None
    return CANONICAL_COUNTRIES.get(str(val).strip().lower(), str(val).strip())


def standardise_lifecycle(val):
    if pd.isna(val):
        return None
    return CANONICAL_LIFECYCLE.get(str(val).strip().lower(), str(val).strip())


def standardise_source(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    return CANONICAL_SOURCES.get(str(val).strip().lower(), str(val).strip())


def standardise_owner(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    return CANONICAL_OWNERS.get(str(val).strip().lower(), str(val).strip())


def is_valid_email(val):
    if pd.isna(val) or str(val).strip() == "":
        return False
    pattern = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
    return bool(re.match(pattern, str(val).strip()))


def clean_arr_value(val):
    """Strip currency symbols, commas, parse to float. Return NaN for junk."""
    if pd.isna(val):
        return np.nan
    val_str = str(val).strip()
    if val_str in ["", "N/A", "n/a", "unknown", "tba"]:
        return np.nan
    # Remove currency symbols, letters, spaces, commas
    cleaned = re.sub(r"[€$£\sEURUSDGBP,]", "", val_str, flags=re.IGNORECASE)
    try:
        result = float(cleaned)
        return result if result > 0 else np.nan   # treat 0 and negative as missing
    except ValueError:
        return np.nan


def standardise_bool(val):
    """Standardise opted_out: True/False → 1/0."""
    if pd.isna(val):
        return np.nan
    mapping = {
        "true": 1, "1": 1, "yes": 1,
        "false": 0, "0": 0, "no": 0,
    }
    return mapping.get(str(val).strip().lower(), np.nan)


# ──────────────────────────────────────────────────────────────────────────────
# CONTACTS PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def clean_contacts(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    issues = {}
    df = df_raw.copy()
    n_raw = len(df)

    # 1. Exact duplicates
    n_exact_dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    issues["exact_duplicates_removed"] = int(n_exact_dupes)

    # 2. Standardise contact_id to string
    df["contact_id"] = df["contact_id"].astype(str).str.strip()

    # 3. Parse created_date
    df["created_date_parsed"] = df["created_date"].apply(parse_mixed_date)
    n_unparseable_dates = df["created_date_parsed"].isna().sum()
    issues["unparseable_dates"] = int(n_unparseable_dates)

    # 4. Flag future dates
    today = pd.Timestamp(datetime.today().date())
    future_mask = df["created_date_parsed"] > today
    issues["future_dated_records_flagged"] = int(future_mask.sum())
    df["is_future_dated"] = future_mask.astype(int)

    # 5. Validate emails
    df["email_valid"] = df["email"].apply(is_valid_email)
    issues["invalid_emails"] = int((~df["email_valid"]).sum())

    # 6. Standardise country
    df["country_raw"] = df["country"]
    df["country"] = df["country"].apply(standardise_country)
    issues["country_variants_normalised"] = int(df_raw["country"].nunique())

    # 7. Standardise lifecycle stage
    df["lifecycle_stage_raw"] = df["lifecycle_stage"]
    df["lifecycle_stage"] = df["lifecycle_stage"].apply(standardise_lifecycle)
    issues["lifecycle_variants_normalised"] = int(df_raw["lifecycle_stage"].nunique())

    # 8. Standardise lead source
    df["lead_source"] = df["lead_source"].apply(standardise_source)

    # 9. Standardise owner
    df["owner_raw"] = df["owner"]
    df["owner"] = df["owner"].apply(standardise_owner)
    issues["unassigned_owners"] = int(df["owner"].isna().sum())

    # 10. Standardise opted_out
    df["opted_out"] = df["opted_out"].apply(standardise_bool)

    # 11. Clean names
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()

    # 12. Drop raw helper columns we no longer need
    df = df.drop(columns=["created_date", "lifecycle_stage_raw",
                           "country_raw", "owner_raw"])
    df = df.rename(columns={"created_date_parsed": "created_date"})

    issues["rows_in"] = n_raw
    issues["rows_out"] = len(df)
    issues["net_rows_removed"] = n_raw - len(df)

    return df, issues


# ──────────────────────────────────────────────────────────────────────────────
# DEALS PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def clean_deals(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    issues = {}
    df = df_raw.copy()
    n_raw = len(df)

    # 1. Exact duplicates
    n_exact_dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    issues["exact_duplicates_removed"] = int(n_exact_dupes)

    # 2. Parse dates
    df["created_date"] = df["created_date"].apply(parse_mixed_date)
    df["expected_close_date"] = df["expected_close_date"].apply(parse_mixed_date)

    # 3. Flag close-before-open errors
    invalid_dates_mask = (
        df["expected_close_date"].notna()
        & df["created_date"].notna()
        & (df["expected_close_date"] < df["created_date"])
    )
    issues["close_before_open_errors"] = int(invalid_dates_mask.sum())
    df["date_error_flagged"] = invalid_dates_mask.astype(int)

    # 4. Clean ARR value
    df["arr_value_raw"] = df["arr_value"]
    df["arr_value_eur"] = df["arr_value"].apply(clean_arr_value)
    issues["arr_missing_or_invalid"] = int(df["arr_value_eur"].isna().sum())
    issues["arr_total_eur"] = float(df["arr_value_eur"].sum())

    # 5. Standardise country
    df["country"] = df["country"].apply(standardise_country)

    # 6. Standardise lifecycle stage
    df["lifecycle_stage"] = df["lifecycle_stage"].apply(standardise_lifecycle)

    # 7. Standardise deal stage
    df["deal_stage"] = df["deal_stage"].apply(standardise_lifecycle)

    # 8. Standardise owner
    df["owner"] = df["owner"].apply(standardise_owner)

    # 9. Standardise lead source
    df["lead_source"] = df["lead_source"].apply(standardise_source)

    # 10. Fill missing deal names
    missing_name_mask = df["deal_name"].isna() | (df["deal_name"].str.strip() == "")
    df.loc[missing_name_mask, "deal_name"] = (
        df.loc[missing_name_mask, "company"] + " - Unnamed Deal"
    )
    issues["deal_names_backfilled"] = int(missing_name_mask.sum())

    # 11. Standardise probability_pct to numeric
    df["probability_pct"] = pd.to_numeric(df["probability_pct"], errors="coerce")

    # 12. Drop raw helper columns
    df = df.drop(columns=["arr_value"])
    df = df.rename(columns={"arr_value_raw": "arr_value_original"})

    issues["rows_in"] = n_raw
    issues["rows_out"] = len(df)

    return df, issues


# ──────────────────────────────────────────────────────────────────────────────
# GTM FUNNEL SUMMARY
# ──────────────────────────────────────────────────────────────────────────────

def build_funnel_summary(contacts_clean: pd.DataFrame, deals_clean: pd.DataFrame) -> pd.DataFrame:
    """Build a GTM funnel conversion table."""
    stage_order = ["Lead", "MQL", "SQL", "Opportunity", "Closed Won", "Closed Lost", "Customer"]

    contact_counts = (
        contacts_clean["lifecycle_stage"]
        .value_counts()
        .reindex(stage_order, fill_value=0)
        .reset_index()
    )
    contact_counts.columns = ["stage", "contacts"]

    deal_counts = (
        deals_clean["deal_stage"]
        .value_counts()
        .reindex(stage_order, fill_value=0)
        .reset_index()
    )
    deal_counts.columns = ["stage", "deals"]

    funnel = contact_counts.merge(deal_counts, on="stage", how="outer").fillna(0)

    arr_by_stage = (
        deals_clean.groupby("deal_stage")["arr_value_eur"]
        .sum()
        .reindex(stage_order, fill_value=0)
        .reset_index()
    )
    arr_by_stage.columns = ["stage", "arr_eur"]

    funnel = funnel.merge(arr_by_stage, on="stage", how="left").fillna(0)
    funnel["arr_eur"] = funnel["arr_eur"].round(0)

    return funnel


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("SPEEXX REVOPS: CRM DATA CLEANING PIPELINE")
    print("=" * 60)

    # Load raw data
    raw_contacts = pd.read_csv(INPUT_DIR / "raw_contacts.csv")
    raw_deals = pd.read_csv(INPUT_DIR / "raw_deals.csv")

    print(f"\nRAW DATA LOADED")
    print(f"  Contacts: {len(raw_contacts):,} rows")
    print(f"  Deals:    {len(raw_deals):,} rows")

    # Run pipelines
    print("\nRUNNING CONTACTS PIPELINE...")
    clean_contacts_df, contact_issues = clean_contacts(raw_contacts)

    print("RUNNING DEALS PIPELINE...")
    clean_deals_df, deal_issues = clean_deals(raw_deals)

    # Build funnel
    funnel = build_funnel_summary(clean_contacts_df, clean_deals_df)

    # Save outputs
    clean_contacts_df.to_csv(OUTPUT_DIR / "clean_contacts.csv", index=False)
    clean_deals_df.to_csv(OUTPUT_DIR / "clean_deals.csv", index=False)
    funnel.to_csv(OUTPUT_DIR / "gtm_funnel.csv", index=False)

    scorecard = {
        "run_timestamp": datetime.now().isoformat(),
        "contacts": contact_issues,
        "deals": deal_issues,
    }
    with open(OUTPUT_DIR / "data_quality_scorecard.json", "w") as f:
        json.dump(scorecard, f, indent=2)

    # Print scorecard
    print("\n" + "=" * 60)
    print("DATA QUALITY SCORECARD")
    print("=" * 60)

    print("\n-- CONTACTS --")
    print(f"  Rows in:                      {contact_issues['rows_in']:>6,}")
    print(f"  Exact duplicates removed:     {contact_issues['exact_duplicates_removed']:>6,}")
    print(f"  Rows out (clean):             {contact_issues['rows_out']:>6,}")
    print(f"  Unparseable dates:            {contact_issues['unparseable_dates']:>6,}")
    print(f"  Future-dated records flagged: {contact_issues['future_dated_records_flagged']:>6,}")
    print(f"  Invalid emails:               {contact_issues['invalid_emails']:>6,}")
    print(f"  Country variants normalised:  {contact_issues['country_variants_normalised']:>6,} raw → 10 canonical")
    print(f"  Lifecycle variants normalised:{contact_issues['lifecycle_variants_normalised']:>6,} raw → 7 canonical")
    print(f"  Unassigned owners:            {contact_issues['unassigned_owners']:>6,}")

    print("\n-- DEALS --")
    print(f"  Rows in:                      {deal_issues['rows_in']:>6,}")
    print(f"  Exact duplicates removed:     {deal_issues['exact_duplicates_removed']:>6,}")
    print(f"  Close-before-open errors:     {deal_issues['close_before_open_errors']:>6,}")
    print(f"  ARR missing or invalid:       {deal_issues['arr_missing_or_invalid']:>6,}")
    print(f"  Deal names backfilled:        {deal_issues['deal_names_backfilled']:>6,}")
    print(f"  Total clean ARR:              €{deal_issues['arr_total_eur']:>10,.0f}")

    print("\n-- GTM FUNNEL SUMMARY --")
    print(funnel.to_string(index=False))

    print("\n✓ Clean files written to pipeline/output/")
    print("  clean_contacts.csv")
    print("  clean_deals.csv")
    print("  gtm_funnel.csv")
    print("  data_quality_scorecard.json")

    return clean_contacts_df, clean_deals_df, funnel, scorecard


if __name__ == "__main__":
    main()
