"""
Generate a realistic messy CRM export for the Speexx RevOps pipeline demo.
Simulates the kind of data quality issues a RevOps analyst actually sees
in Dynamics 365 / HubSpot exports from a B2B SaaS GTM team.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# --- Realistic B2B SaaS company names (mix of real-sounding orgs) ---
COMPANIES = [
    "Siemens AG", "SIEMENS AG", "Siemens ag", "siemens",             # duplicates
    "Allianz SE", "Allianz Se", "ALLIANZ",
    "Bosch GmbH", "Robert Bosch GmbH", "BOSCH",
    "SAP SE", "SAP", "sap se",
    "Deutsche Telekom", "Deutsche Telekom AG", "DT AG",
    "BMW Group", "BMW AG", "bmw",
    "Volkswagen AG", "VW AG", "volkswagen",
    "Lufthansa Group", "Deutsche Lufthansa AG",
    "BASF SE", "basf",
    "Henkel AG", "Henkel",
    "Continental AG", "Continental",
    "Thyssenkrupp AG", "ThyssenKrupp",
    "E.ON SE", "E.ON", "eon",
    "Merck KGaA", "Merck", "MERCK KGAA",
    "Fresenius SE", "Fresenius",
    "Zalando SE", "Zalando",
    "HelloFresh SE", "HelloFresh",
    "Delivery Hero SE", "Delivery Hero",
]

COUNTRIES_DIRTY = [
    "Germany", "GERMANY", "germany", "DE", "Deutschland",             # Germany variants
    "France", "FRANCE", "france", "FR", "Francia",
    "Spain", "SPAIN", "spain", "ES", "Espana",
    "Italy", "ITALY", "italy", "IT", "Italia",
    "Netherlands", "NETHERLANDS", "NL", "The Netherlands", "Holland",
    "United Kingdom", "UK", "GB", "United kingdom", "england",
    "United States", "USA", "US", "United states", "u.s.",
    "Poland", "POLAND", "PL",
    "Austria", "AUSTRIA", "AT",
    "Switzerland", "CH", "SWITZERLAND",
]

LIFECYCLE_STAGES_DIRTY = [
    "MQL", "mql", "Marketing Qualified Lead", "marketing qualified lead",
    "SQL", "sql", "Sales Qualified Lead", "Sales qualified",
    "Opportunity", "opportunity", "OPPORTUNITY", "Opp",
    "Closed Won", "closed won", "CLOSED WON", "Won", "won",
    "Closed Lost", "closed lost", "CLOSED LOST", "Lost", "lost",
    "Lead", "lead", "LEAD", "New Lead",
    "Customer", "customer", "CUSTOMER",
]

SOURCES_DIRTY = [
    "Organic Search", "organic search", "ORGANIC", "organic",
    "Paid Search", "paid search", "PPC", "Google Ads", "google ads",
    "LinkedIn", "linkedin", "LinkedIn Ads", "LINKEDIN",
    "Referral", "referral", "REFERRAL", "Partner Referral",
    "Direct", "direct", "DIRECT",
    "Webinar", "webinar", "WEBINAR", "Event",
    "Email", "email", "EMAIL", "Email Campaign",
    None, "N/A", "n/a", "Unknown", "unknown", "",
]

OWNERS = [
    "sarah.mueller@speexx.com", "Sarah Mueller",                      # same person, 2 formats
    "james.wilson@speexx.com", "J. Wilson", "james.wilson",
    "ana.garcia@speexx.com", "Ana Garcia",
    "luca.bianchi@speexx.com", "Luca Bianchi", "l.bianchi@speexx.com",
    "marie.dupont@speexx.com", "Marie Dupont",
    None, "Unassigned", "",
]

DEAL_STAGES_DIRTY = [
    "Prospecting", "prospecting",
    "Discovery", "discovery", "DISCOVERY",
    "Demo Scheduled", "Demo", "demo scheduled", "demo",
    "Proposal Sent", "Proposal", "proposal sent",
    "Negotiation", "negotiation", "NEGOTIATION",
    "Closed Won", "closed won", "Won",
    "Closed Lost", "closed lost", "Lost",
]

def random_date(start_year=2023, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_email(name_str, domain="company.com"):
    """Generate realistic email, sometimes with errors."""
    if not name_str or pd.isna(name_str):
        return None
    name = name_str.lower().replace(" ", ".").replace("-", "")
    variants = [
        f"{name}@{domain}",
        f"{name}@{domain}",
        f"{name}@{domain}",
        f"{name.split('.')[0]}@{domain}",           # first name only
        f"info@{domain}",
        f"contact@{domain}",
        name,                                         # missing @domain
        f"{name}@",                                   # malformed
        None,
    ]
    return random.choices(variants, weights=[50, 50, 50, 10, 5, 5, 5, 3, 8])[0]

def random_phone():
    variants = [
        f"+49 {random.randint(100,999)} {random.randint(1000000,9999999)}",
        f"0{random.randint(100,999)}-{random.randint(1000000,9999999)}",
        f"+49{random.randint(1000000000,9999999999)}",
        f"004{random.randint(910000000,999999999)}",
        f"{random.randint(100,999)} {random.randint(1000,9999)} {random.randint(1000,9999)}",
        None, "", "N/A", "tba",
    ]
    return random.choice(variants)

def random_revenue():
    """ARR / deal value, sometimes as number, string, or garbage."""
    value = random.choices(
        [5000, 10000, 25000, 50000, 75000, 100000, 150000, 250000],
        weights=[20, 25, 20, 15, 8, 6, 4, 2]
    )[0]
    # Add noise
    value += random.randint(-500, 500)
    variants = [
        value,
        float(value),
        f"€{value:,}",
        f"{value} EUR",
        f"EUR {value}",
        str(value),
        None,
        0,
        -1,                                           # placeholder for "unknown"
    ]
    return random.choices(variants, weights=[40, 15, 8, 5, 5, 8, 8, 5, 6])[0]

def create_messy_contacts(n=400):
    rows = []
    contact_id = 1000
    for _ in range(n):
        first = random.choice(["Anna","Thomas","Julia","Michael","Sarah","Peter","Maria","David","Laura","Stefan"])
        last = random.choice(["Mueller","Schmidt","Schneider","Fischer","Weber","Meyer","Wagner","Becker","Richter","Hoffmann"])
        company = random.choice(COMPANIES)
        country = random.choice(COUNTRIES_DIRTY)
        created = random_date(2023, 2024)

        # Introduce future dates (data error)
        if random.random() < 0.04:
            created = datetime(2026, random.randint(1,12), random.randint(1,28))

        rows.append({
            "contact_id": f"CTX-{contact_id}" if random.random() > 0.03 else contact_id,
            "first_name": first if random.random() > 0.05 else first.upper(),
            "last_name": last if random.random() > 0.05 else None,
            "email": random_email(f"{first} {last}", domain=f"{company.split()[0].lower().replace(',','').replace('.','')}.com"),
            "phone": random_phone(),
            "company": company,
            "country": country,
            "lifecycle_stage": random.choice(LIFECYCLE_STAGES_DIRTY),
            "lead_source": random.choice(SOURCES_DIRTY),
            "owner": random.choice(OWNERS),
            "created_date": created.strftime("%Y-%m-%d") if random.random() > 0.06 else
                            created.strftime("%d/%m/%Y") if random.random() > 0.5 else
                            created.strftime("%m-%d-%Y"),
            "last_activity_date": (created + timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d"),
            "opted_out": random.choice(["True","False","true","false","1","0","yes","no",None]),
        })
        contact_id += 1

    # Add exact duplicates
    for _ in range(20):
        rows.append(random.choice(rows[:200]).copy())

    return pd.DataFrame(rows)


def create_messy_deals(n=250):
    rows = []
    deal_id = 5000
    for _ in range(n):
        company = random.choice(COMPANIES)
        created = random_date(2023, 2024)
        close = created + timedelta(days=random.randint(14, 180))

        # Sometimes close date before create date (error)
        if random.random() < 0.05:
            close = created - timedelta(days=random.randint(1, 30))

        stage = random.choice(DEAL_STAGES_DIRTY)
        arr = random_revenue()

        rows.append({
            "deal_id": f"DL-{deal_id}",
            "deal_name": f"{company.split()[0]} - Enterprise License" if random.random() > 0.1 else None,
            "company": company,
            "country": random.choice(COUNTRIES_DIRTY),
            "deal_stage": stage,
            "lifecycle_stage": random.choice(LIFECYCLE_STAGES_DIRTY),
            "arr_value": arr,
            "currency": random.choice(["EUR","EUR","EUR","eur","€","USD","GBP",None]),
            "owner": random.choice(OWNERS),
            "created_date": created.strftime("%Y-%m-%d"),
            "expected_close_date": close.strftime("%Y-%m-%d") if random.random() > 0.08 else
                                   close.strftime("%d.%m.%Y"),
            "lead_source": random.choice(SOURCES_DIRTY),
            "probability_pct": random.choice([10,20,30,50,70,80,90,100,None,"","unknown"]),
            "notes": random.choice([
                "Follow up next week","FOLLOW UP","follow up",
                "Sent proposal","sent proposal",
                "Awaiting legal review","awaiting legal",
                "Champion identified","no champion","Champion?",
                None, "", "tbd","TBD",
            ]),
        })
        deal_id += 1

    return pd.DataFrame(rows)


if __name__ == "__main__":
    contacts = create_messy_contacts(400)
    deals = create_messy_deals(250)

    contacts.to_csv("/home/claude/speexx_revops/data/raw_contacts.csv", index=False)
    deals.to_csv("/home/claude/speexx_revops/data/raw_deals.csv", index=False)

    print(f"Contacts: {len(contacts)} rows, {contacts.duplicated().sum()} exact duplicates")
    print(f"Deals: {len(deals)} rows")
    print(f"\nContact nulls:\n{contacts.isnull().sum()}")
    print(f"\nDeal nulls:\n{deals.isnull().sum()}")
    print(f"\nDate format sample (contacts):\n{contacts['created_date'].head(10).tolist()}")
    print(f"\nCountry variants: {contacts['country'].nunique()} unique values for {contacts['country'].notna().sum()} non-null")
    print(f"\nLifecycle variants: {contacts['lifecycle_stage'].unique()}")
