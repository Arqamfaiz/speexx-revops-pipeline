"""
Speexx RevOps Pipeline — Interactive Demo
=========================================
A Python + pandas ETL pipeline that takes a messy CRM export and cleans it.
The pipeline logic + dashboard live in this single file for deployment simplicity.
The standalone pipeline code is also available as pipeline/clean.py in the GitHub repo.

Author: Arqam Faiz Siddiqui
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import re
from datetime import datetime
from pathlib import Path

st.set_page_config(
    page_title="Speexx RevOps Pipeline",
    page_icon="R",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# STYLING
# ──────────────────────────────────────────────────────────────────────────────

SPEEXX_PURPLE = "#6B1FA2"
SPEEXX_LIGHT = "#F3E8FF"
CLEAN_GREEN = "#2E7D32"
ERROR_RED = "#C62828"
AMBER = "#ED6C02"
GREY = "#6B7280"

st.markdown(
    f"""
    <style>
    .header {{ color: {SPEEXX_PURPLE}; font-weight: 700; padding-bottom: 0.4rem;
              border-bottom: 3px solid {SPEEXX_PURPLE}; margin-bottom: 1.2rem; }}
    .insight {{ background-color: {SPEEXX_LIGHT}; border-left: 4px solid {SPEEXX_PURPLE};
               padding: 0.85rem 1rem; border-radius: 0.3rem; margin: 0.8rem 0;
               font-size: 0.95em; }}
    div[data-testid="metric-container"] {{
        background-color: #FAFAFA; border-radius: 0.5rem; padding: 0.7rem;
        border: 1px solid #E5E7EB; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE: CONFIGURATION (same as pipeline/clean.py)
# ──────────────────────────────────────────────────────────────────────────────

CANONICAL_COUNTRIES = {
    "germany": "Germany", "de": "Germany", "deutschland": "Germany", "german": "Germany",
    "france": "France", "fr": "France", "francia": "France",
    "spain": "Spain", "es": "Spain", "espana": "Spain",
    "italy": "Italy", "it": "Italy", "italia": "Italy",
    "netherlands": "Netherlands", "nl": "Netherlands",
    "the netherlands": "Netherlands", "holland": "Netherlands",
    "united kingdom": "United Kingdom", "uk": "United Kingdom",
    "gb": "United Kingdom", "england": "United Kingdom", "great britain": "United Kingdom",
    "united states": "United States", "usa": "United States",
    "us": "United States", "u.s.": "United States", "united states of america": "United States",
    "poland": "Poland", "pl": "Poland",
    "austria": "Austria", "at": "Austria",
    "switzerland": "Switzerland", "ch": "Switzerland",
}

CANONICAL_LIFECYCLE = {
    "mql": "MQL", "marketing qualified lead": "MQL", "marketing qualified": "MQL",
    "sql": "SQL", "sales qualified lead": "SQL", "sales qualified": "SQL",
    "opportunity": "Opportunity", "opp": "Opportunity",
    "lead": "Lead", "new lead": "Lead",
    "closed won": "Closed Won", "won": "Closed Won",
    "closed lost": "Closed Lost", "lost": "Closed Lost",
    "customer": "Customer",
}

CANONICAL_SOURCES = {
    "organic search": "Organic Search", "organic": "Organic Search",
    "paid search": "Paid Search", "ppc": "Paid Search", "google ads": "Paid Search",
    "linkedin": "LinkedIn", "linkedin ads": "LinkedIn",
    "referral": "Referral", "partner referral": "Referral",
    "direct": "Direct",
    "webinar": "Webinar", "event": "Webinar",
    "email": "Email", "email campaign": "Email",
    "n/a": None, "unknown": None, "": None,
}

CANONICAL_OWNERS = {
    "sarah.mueller@speexx.com": "sarah.mueller@speexx.com",
    "sarah mueller": "sarah.mueller@speexx.com",
    "james.wilson@speexx.com": "james.wilson@speexx.com",
    "j. wilson": "james.wilson@speexx.com",
    "james.wilson": "james.wilson@speexx.com",
    "ana.garcia@speexx.com": "ana.garcia@speexx.com",
    "ana garcia": "ana.garcia@speexx.com",
    "luca.bianchi@speexx.com": "luca.bianchi@speexx.com",
    "l.bianchi@speexx.com": "luca.bianchi@speexx.com",
    "luca bianchi": "luca.bianchi@speexx.com",
    "marie.dupont@speexx.com": "marie.dupont@speexx.com",
    "marie dupont": "marie.dupont@speexx.com",
    "unassigned": None, "": None,
}

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE: HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def parse_mixed_date(val):
    if pd.isna(val) or val == "":
        return pd.NaT
    val = str(val).strip()
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d.%m.%Y", "%Y/%m/%d"]:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return pd.NaT

def standardise_country(val):
    if pd.isna(val): return None
    return CANONICAL_COUNTRIES.get(str(val).strip().lower(), str(val).strip())

def standardise_lifecycle(val):
    if pd.isna(val): return None
    return CANONICAL_LIFECYCLE.get(str(val).strip().lower(), str(val).strip())

def standardise_source(val):
    if pd.isna(val) or str(val).strip() == "": return None
    return CANONICAL_SOURCES.get(str(val).strip().lower(), str(val).strip())

def standardise_owner(val):
    if pd.isna(val) or str(val).strip() == "": return None
    return CANONICAL_OWNERS.get(str(val).strip().lower(), str(val).strip())

def is_valid_email(val):
    if pd.isna(val) or str(val).strip() == "": return False
    return bool(re.match(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", str(val).strip()))

def clean_arr_value(val):
    if pd.isna(val): return np.nan
    val_str = str(val).strip()
    if val_str in ["", "N/A", "n/a", "unknown", "tba"]: return np.nan
    cleaned = re.sub(r"[€$£\sEURUSDGBP,]", "", val_str, flags=re.IGNORECASE)
    try:
        result = float(cleaned)
        return result if result > 0 else np.nan
    except ValueError:
        return np.nan

def standardise_bool(val):
    if pd.isna(val): return np.nan
    mapping = {"true": 1, "1": 1, "yes": 1, "false": 0, "0": 0, "no": 0}
    return mapping.get(str(val).strip().lower(), np.nan)

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE: CONTACTS
# ──────────────────────────────────────────────────────────────────────────────

def clean_contacts(df_raw):
    issues = {}
    df = df_raw.copy()
    n_raw = len(df)

    n_exact_dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    issues["exact_duplicates_removed"] = int(n_exact_dupes)

    df["contact_id"] = df["contact_id"].astype(str).str.strip()

    df["created_date_parsed"] = df["created_date"].apply(parse_mixed_date)
    issues["unparseable_dates"] = int(df["created_date_parsed"].isna().sum())

    today = pd.Timestamp(datetime.today().date())
    future_mask = df["created_date_parsed"] > today
    issues["future_dated_records_flagged"] = int(future_mask.sum())
    df["is_future_dated"] = future_mask.astype(int)

    df["email_valid"] = df["email"].apply(is_valid_email)
    issues["invalid_emails"] = int((~df["email_valid"]).sum())

    issues["country_variants_normalised"] = int(df_raw["country"].nunique())
    df["country"] = df["country"].apply(standardise_country)

    issues["lifecycle_variants_normalised"] = int(df_raw["lifecycle_stage"].nunique())
    df["lifecycle_stage"] = df["lifecycle_stage"].apply(standardise_lifecycle)

    df["lead_source"] = df["lead_source"].apply(standardise_source)

    df["owner"] = df["owner"].apply(standardise_owner)
    issues["unassigned_owners"] = int(df["owner"].isna().sum())

    df["opted_out"] = df["opted_out"].apply(standardise_bool)

    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()

    df = df.drop(columns=["created_date"])
    df = df.rename(columns={"created_date_parsed": "created_date"})

    issues["rows_in"] = n_raw
    issues["rows_out"] = len(df)
    issues["net_rows_removed"] = n_raw - len(df)

    return df, issues

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE: DEALS
# ──────────────────────────────────────────────────────────────────────────────

def clean_deals(df_raw):
    issues = {}
    df = df_raw.copy()
    n_raw = len(df)

    n_exact_dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    issues["exact_duplicates_removed"] = int(n_exact_dupes)

    df["created_date"] = df["created_date"].apply(parse_mixed_date)
    df["expected_close_date"] = df["expected_close_date"].apply(parse_mixed_date)

    invalid_dates_mask = (
        df["expected_close_date"].notna()
        & df["created_date"].notna()
        & (df["expected_close_date"] < df["created_date"])
    )
    issues["close_before_open_errors"] = int(invalid_dates_mask.sum())
    df["date_error_flagged"] = invalid_dates_mask.astype(int)

    df["arr_value_raw"] = df["arr_value"]
    df["arr_value_eur"] = df["arr_value"].apply(clean_arr_value)
    issues["arr_missing_or_invalid"] = int(df["arr_value_eur"].isna().sum())
    issues["arr_total_eur"] = float(df["arr_value_eur"].sum())

    df["country"] = df["country"].apply(standardise_country)
    df["lifecycle_stage"] = df["lifecycle_stage"].apply(standardise_lifecycle)
    df["deal_stage"] = df["deal_stage"].apply(standardise_lifecycle)
    df["owner"] = df["owner"].apply(standardise_owner)
    df["lead_source"] = df["lead_source"].apply(standardise_source)

    missing_name_mask = df["deal_name"].isna() | (df["deal_name"].str.strip() == "")
    df.loc[missing_name_mask, "deal_name"] = df.loc[missing_name_mask, "company"] + " - Unnamed Deal"
    issues["deal_names_backfilled"] = int(missing_name_mask.sum())

    df["probability_pct"] = pd.to_numeric(df["probability_pct"], errors="coerce")

    df = df.drop(columns=["arr_value"])
    df = df.rename(columns={"arr_value_raw": "arr_value_original"})

    issues["rows_in"] = n_raw
    issues["rows_out"] = len(df)

    return df, issues

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE: GTM FUNNEL
# ──────────────────────────────────────────────────────────────────────────────

def build_funnel_summary(contacts_clean, deals_clean):
    stage_order = ["Lead", "MQL", "SQL", "Opportunity", "Closed Won", "Closed Lost", "Customer"]

    contact_counts = (
        contacts_clean["lifecycle_stage"].value_counts()
        .reindex(stage_order, fill_value=0).reset_index()
    )
    contact_counts.columns = ["stage", "contacts"]

    deal_counts = (
        deals_clean["deal_stage"].value_counts()
        .reindex(stage_order, fill_value=0).reset_index()
    )
    deal_counts.columns = ["stage", "deals"]

    funnel = contact_counts.merge(deal_counts, on="stage", how="outer").fillna(0)

    arr_by_stage = (
        deals_clean.groupby("deal_stage")["arr_value_eur"].sum()
        .reindex(stage_order, fill_value=0).reset_index()
    )
    arr_by_stage.columns = ["stage", "arr_eur"]

    funnel = funnel.merge(arr_by_stage, on="stage", how="left").fillna(0)
    funnel["arr_eur"] = funnel["arr_eur"].round(0)

    return funnel

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA: Run the pipeline inline
# ──────────────────────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent / "data"

@st.cache_data
def load_and_clean():
    raw_contacts = pd.read_csv(DATA_DIR / "raw_contacts.csv")
    raw_deals = pd.read_csv(DATA_DIR / "raw_deals.csv")

    contacts, contact_issues = clean_contacts(raw_contacts)
    deals, deal_issues = clean_deals(raw_deals)
    funnel = build_funnel_summary(contacts, deals)

    scorecard = {
        "contacts": contact_issues,
        "deals": deal_issues,
    }

    return contacts, deals, funnel, scorecard

contacts, deals, funnel, scorecard = load_and_clean()

# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Navigation")
    section = st.radio(
        "Section",
        ["Data Quality Scorecard", "GTM Funnel", "Clean Contacts", "Clean Deals", "Pipeline Code"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption(
        "This dashboard is the visual companion to the cleaning pipeline. "
        "The real work is the ETL logic (see Pipeline Code section). "
        "Raw data was generated to mimic a realistic Dynamics 365 / HubSpot export."
    )
    st.caption("Built by Arqam Faiz Siddiqui for the Speexx RevOps application.")

# ──────────────────────────────────────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────────────────────────────────────

st.markdown(
    "<h1 class='header'>Speexx RevOps: CRM Data Cleaning Pipeline</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "A Python + pandas ETL pipeline that takes a realistic messy CRM export "
    "(contacts + deals from Dynamics 365 / HubSpot) and produces clean, "
    "analysis-ready tables. The dashboard shows what was found and what was fixed."
)

with st.expander("What this project demonstrates"):
    st.markdown(
        """
        The pipeline covers **13 cleaning operations** that mirror real RevOps data quality work:

        1. Remove exact duplicates
        2. Standardise contact IDs to consistent string format
        3. Parse and normalise mixed date formats (ISO, European DD/MM/YYYY, US MM-DD-YYYY, German DD.MM.YYYY)
        4. Flag and quarantine future-dated records (data entry errors)
        5. Validate email addresses with regex
        6. Standardise country names — 44 raw variants → 10 canonical ISO names
        7. Standardise lifecycle stage vocabulary — 29 raw variants → 7 canonical stages
        8. Standardise lead source vocabulary
        9. Resolve owner identity — name variants + email formats → canonical email
        10. Clean ARR / deal value — strip currency symbols, coerce to float, flag negatives
        11. Flag close-before-open date errors in deals
        12. Standardise boolean opted_out field
        13. Backfill missing deal names from company name

        The standalone `pipeline/clean.py` file is also in the GitHub repo
        with full documentation and RevOps reasoning for each step.
        The raw data generator (`data/generate_raw_data.py`) is provided so
        the full pipeline can be reproduced from scratch.
        """
    )

st.divider()

# ====================================================================
# SECTION 1: SCORECARD
# ====================================================================
if section == "Data Quality Scorecard":
    st.subheader("Data Quality Scorecard: Before vs After")
    st.caption("What the pipeline found and fixed across 670 raw records (420 contacts + 250 deals).")

    c = scorecard["contacts"]
    d = scorecard["deals"]

    st.markdown("#### Contacts")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows in", f"{c['rows_in']:,}")
    col2.metric("Duplicates removed", c['exact_duplicates_removed'], delta_color="inverse")
    col3.metric("Rows out (clean)", f"{c['rows_out']:,}")
    col4.metric("Net removed", f"{c['net_rows_removed']:,}", delta_color="inverse")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Invalid emails", c['invalid_emails'], delta_color="inverse")
    col6.metric("Future-dated records", c['future_dated_records_flagged'], delta_color="inverse")
    col7.metric("Country variants → canonical", f"{c['country_variants_normalised']} → 10")
    col8.metric("Lifecycle variants → canonical", f"{c['lifecycle_variants_normalised']} → 7")

    st.markdown("#### Deals")
    col9, col10, col11, col12 = st.columns(4)
    col9.metric("Rows in", f"{d['rows_in']:,}")
    col10.metric("Close-before-open errors", d['close_before_open_errors'], delta_color="inverse")
    col11.metric("ARR missing or invalid", d['arr_missing_or_invalid'], delta_color="inverse")
    col12.metric("Total clean ARR", f"€{d['arr_total_eur']:,.0f}")

    st.markdown("#### Issue resolution breakdown")
    issues_df = pd.DataFrame({
        "Issue": [
            "Exact duplicates", "Invalid emails", "Future-dated records",
            "Close-before-open", "ARR invalid", "Deal names missing",
            "Country variants", "Lifecycle variants"
        ],
        "Count found": [
            c['exact_duplicates_removed'], c['invalid_emails'],
            c['future_dated_records_flagged'], d['close_before_open_errors'],
            d['arr_missing_or_invalid'], d['deal_names_backfilled'],
            c['country_variants_normalised'], c['lifecycle_variants_normalised']
        ],
        "Action": [
            "Removed", "Flagged", "Flagged", "Flagged",
            "Set to null", "Backfilled", "Standardised", "Standardised"
        ]
    })
    fig = px.bar(
        issues_df,
        x="Count found", y="Issue", orientation="h",
        color="Action",
        color_discrete_map={
            "Removed": ERROR_RED, "Flagged": AMBER, "Set to null": GREY,
            "Backfilled": CLEAN_GREEN, "Standardised": SPEEXX_PURPLE,
        },
        template="plotly_white",
        labels={"Count found": "Records affected", "Issue": ""}
    )
    fig.update_layout(
        height=380, margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3)
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        f"""
        <div class='insight'>
        <b>Most impactful fix:</b> Lifecycle stage standardisation collapsed
        {c['lifecycle_variants_normalised']} raw variants down to 7 canonical stages.
        This is the single most common RevOps data problem — the same stage name
        entered in 4 different ways means no two reports agree on the funnel size.
        </div>
        """,
        unsafe_allow_html=True,
    )

# ====================================================================
# SECTION 2: GTM FUNNEL
# ====================================================================
elif section == "GTM Funnel":
    st.subheader("GTM Funnel: Post-Clean View")
    st.caption(
        "Lifecycle stage distribution after standardisation. "
        "This is what the downstream BI team can now reliably consume."
    )

    stage_order = ["Lead", "MQL", "SQL", "Opportunity", "Closed Won", "Closed Lost", "Customer"]
    funnel_ordered = funnel.set_index("stage").reindex(stage_order).reset_index().fillna(0)

    col_l, col_r = st.columns([3, 2])
    with col_l:
        fig_funnel = px.bar(
            funnel_ordered,
            x="stage", y="contacts",
            color_discrete_sequence=[SPEEXX_PURPLE],
            template="plotly_white",
            labels={"contacts": "Contacts", "stage": "Lifecycle Stage"},
            text="contacts",
        )
        fig_funnel.update_traces(textposition="outside")
        fig_funnel.update_layout(
            height=380, margin=dict(l=20, r=20, t=20, b=20), showlegend=False
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

    with col_r:
        st.markdown("##### ARR by deal stage (clean)")
        funnel_arr = funnel_ordered[funnel_ordered["arr_eur"] > 0]
        fig_arr = px.pie(
            funnel_arr, values="arr_eur", names="stage", hole=0.45,
            color_discrete_sequence=[SPEEXX_PURPLE, CLEAN_GREEN, AMBER, "#9333EA", "#C084FC"],
            template="plotly_white",
        )
        fig_arr.update_traces(textinfo="percent+label")
        fig_arr.update_layout(
            height=380, margin=dict(l=20, r=20, t=20, b=20), showlegend=False
        )
        st.plotly_chart(fig_arr, use_container_width=True)

    st.markdown(
        """
        <div class='insight'>
        <b>Why this matters:</b> Before cleaning, the same contact might appear as
        "MQL", "mql", "Marketing Qualified Lead", and "marketing qualified" — four
        rows that a COUNT query treats as four different stages. After standardisation,
        the funnel is accurate and the conversion rates become meaningful.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("##### Contact distribution by country (post-clean)")
    country_counts = contacts["country"].value_counts().reset_index()
    country_counts.columns = ["country", "count"]
    fig_country = px.bar(
        country_counts.head(10),
        x="count", y="country", orientation="h",
        color_discrete_sequence=[SPEEXX_PURPLE],
        template="plotly_white",
        labels={"count": "Contacts", "country": ""}
    )
    fig_country.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20))
    st.plotly_chart(fig_country, use_container_width=True)

# ====================================================================
# SECTION 3: CLEAN CONTACTS
# ====================================================================
elif section == "Clean Contacts":
    st.subheader("Clean Contacts Table")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        stage_filter = st.multiselect(
            "Lifecycle stage",
            options=sorted(contacts["lifecycle_stage"].dropna().unique()),
            default=None,
        )
    with col_f2:
        country_filter = st.multiselect(
            "Country",
            options=sorted(contacts["country"].dropna().unique()),
            default=None,
        )
    with col_f3:
        email_filter = st.selectbox(
            "Email valid",
            options=["All", "Valid only", "Invalid only"],
        )

    df_view = contacts.copy()
    if stage_filter:
        df_view = df_view[df_view["lifecycle_stage"].isin(stage_filter)]
    if country_filter:
        df_view = df_view[df_view["country"].isin(country_filter)]
    if email_filter == "Valid only":
        df_view = df_view[df_view["email_valid"] == True]
    elif email_filter == "Invalid only":
        df_view = df_view[df_view["email_valid"] == False]

    st.caption(f"Showing {len(df_view):,} of {len(contacts):,} contacts")
    st.dataframe(
        df_view[[
            "contact_id", "first_name", "last_name", "email",
            "email_valid", "company", "country", "lifecycle_stage",
            "lead_source", "owner", "created_date", "is_future_dated"
        ]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "contact_id": "ID", "first_name": "First", "last_name": "Last",
            "email": "Email",
            "email_valid": st.column_config.CheckboxColumn("Email Valid"),
            "company": "Company", "country": "Country",
            "lifecycle_stage": "Stage", "lead_source": "Source", "owner": "Owner",
            "created_date": st.column_config.DateColumn("Created"),
            "is_future_dated": st.column_config.CheckboxColumn("Future Dated"),
        },
    )

# ====================================================================
# SECTION 4: CLEAN DEALS
# ====================================================================
elif section == "Clean Deals":
    st.subheader("Clean Deals Table")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        stage_filter_d = st.multiselect(
            "Deal stage",
            options=sorted(deals["deal_stage"].dropna().unique()),
            default=None,
        )
    with col_f2:
        owner_filter_d = st.multiselect(
            "Owner",
            options=sorted(deals["owner"].dropna().unique()),
            default=None,
        )

    df_view_d = deals.copy()
    if stage_filter_d:
        df_view_d = df_view_d[df_view_d["deal_stage"].isin(stage_filter_d)]
    if owner_filter_d:
        df_view_d = df_view_d[df_view_d["owner"].isin(owner_filter_d)]

    total_arr = df_view_d["arr_value_eur"].sum()
    st.metric("Total clean ARR (filtered)", f"€{total_arr:,.0f}")
    st.caption(f"Showing {len(df_view_d):,} of {len(deals):,} deals")

    st.dataframe(
        df_view_d[[
            "deal_id", "deal_name", "company", "country",
            "deal_stage", "arr_value_eur", "owner",
            "created_date", "expected_close_date", "date_error_flagged"
        ]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "deal_id": "ID", "deal_name": "Deal", "company": "Company",
            "country": "Country", "deal_stage": "Stage",
            "arr_value_eur": st.column_config.NumberColumn("ARR (EUR)", format="%.0f"),
            "owner": "Owner",
            "created_date": st.column_config.DateColumn("Created"),
            "expected_close_date": st.column_config.DateColumn("Close Date"),
            "date_error_flagged": st.column_config.CheckboxColumn("Date Error"),
        },
    )

# ====================================================================
# SECTION 5: PIPELINE CODE
# ====================================================================
elif section == "Pipeline Code":
    st.subheader("Pipeline: clean.py (core logic)")
    st.caption(
        "The actual ETL code. The dashboard is just a wrapper. "
        "Full source at github.com/Arqamfaiz/speexx-revops-pipeline"
    )

    # Read the standalone pipeline file if it exists, otherwise show embedded code
    pipeline_path = Path(__file__).parent / "pipeline" / "clean.py"
    if pipeline_path.exists():
        with open(pipeline_path) as f:
            code = f.read()
    else:
        # Show the embedded pipeline logic from this file
        with open(__file__) as f:
            full_source = f.read()
        start_marker = "# PIPELINE: CONFIGURATION"
        end_marker = "# LOAD DATA: Run the pipeline inline"
        start_idx = full_source.find(start_marker)
        end_idx = full_source.find(end_marker)
        if start_idx != -1 and end_idx != -1:
            code = full_source[start_idx:end_idx].strip()
        else:
            code = "# Pipeline code is embedded in app.py. See the GitHub repo for the full source."

    st.code(code, language="python")

# ──────────────────────────────────────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    """
    <div style='text-align: center; color: #888; font-size: 0.85em; padding-top: 1em;'>
    Built by Arqam Faiz Siddiqui as a portfolio project for the
    Speexx Working Student Revenue Operations application. <br>
    Raw data is synthetically generated to mimic a real CRM export.
    No real customer data is used.
    </div>
    """,
    unsafe_allow_html=True,
)
