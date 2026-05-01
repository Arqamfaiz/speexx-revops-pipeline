# Speexx RevOps: CRM Data Cleaning Pipeline

A Python ETL pipeline that cleans a realistic messy CRM export
(contacts + deals from Dynamics 365 / HubSpot) and produces
clean, analysis-ready tables with a data quality scorecard.

Built as a portfolio project for the **Working Student Revenue Operations**
role at Speexx.

**Live demo:** [paste Streamlit link here after deployment]

**GitHub:** https://github.com/Arqamfaiz/speexx-revops-pipeline

---

## What it does

The pipeline processes 670 raw records (420 contacts + 250 deals) through
13 cleaning operations and outputs:

- `clean_contacts.csv` — 400 rows, fully standardised
- `clean_deals.csv` — 250 rows, with clean ARR values and flagged date errors
- `gtm_funnel.csv` — lifecycle stage distribution with ARR by stage
- `data_quality_scorecard.json` — machine-readable record of every issue found

### The 13 cleaning operations

| # | Operation | What it fixes |
|---|-----------|---------------|
| 1 | Exact duplicate removal | 20 exact-duplicate contact rows |
| 2 | Contact ID standardisation | Mixed int/string IDs → consistent CTX-XXXX format |
| 3 | Mixed date parsing | YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY, DD.MM.YYYY all normalised |
| 4 | Future-date flagging | Records with created_date in the future (data entry error) |
| 5 | Email validation | Regex check; malformed emails flagged, not silently dropped |
| 6 | Country standardisation | 44 raw variants (DE, Deutschland, GERMANY…) → 10 canonical names |
| 7 | Lifecycle stage normalisation | 29 raw variants → 7 canonical stages (Lead, MQL, SQL, Opportunity, Closed Won, Closed Lost, Customer) |
| 8 | Lead source normalisation | Free-text variants → controlled vocabulary |
| 9 | Owner identity resolution | Name variants + email formats → canonical owner email |
| 10 | ARR value cleaning | Strips €/$, commas, text suffixes; coerces to float; flags negatives |
| 11 | Close-before-open flagging | 16 deals where expected_close_date < created_date |
| 12 | Boolean standardisation | opted_out: "True"/"1"/"yes"/"true" → 1; "False"/"0"/"no" → 0 |
| 13 | Deal name backfill | 22 missing deal names filled as "CompanyName - Unnamed Deal" |

---

## Why each operation exists (the RevOps reasoning)

**Operations 6 and 7 (country + lifecycle normalisation) are the highest
impact.** These are the two most common RevOps data problems. When lifecycle
stages exist in 29 variants, a COUNT by stage in Power BI returns 29 rows
instead of 7. The funnel looks broken. Dashboards disagree with each other.
No one trusts the data. Fixing this at the ETL layer is the right solution,
not patching it in every downstream query.

**Operation 9 (owner identity resolution) matters for quota attribution.**
If "Sarah Mueller", "sarah.mueller@speexx.com", and "Sarah mueller" are
treated as three different owners, rep-level revenue reports are wrong.
Commissions and quota tracking depend on this being clean.

**Operation 11 (close-before-open flagging) is a CRM hygiene signal.**
Deals where the close date is before the create date usually indicate a
data entry mistake or a CRM migration artefact. They should not be silently
dropped but flagged so the sales ops team can review and correct.

---

## How to run it locally

```bash
git clone https://github.com/Arqamfaiz/speexx-revops-pipeline.git
cd speexx-revops-pipeline

pip install -r requirements.txt

# Generate fresh raw data
python data/generate_raw_data.py

# Run the cleaning pipeline
python pipeline/clean.py

# Launch the interactive dashboard
streamlit run app.py
```

---

## Project structure

```
speexx-revops-pipeline/
├── app.py                         # Streamlit dashboard (visual companion)
├── requirements.txt
├── README.md
├── data/
│   ├── generate_raw_data.py       # Generates realistic messy CRM export
│   ├── raw_contacts.csv           # 420 dirty contact rows
│   └── raw_deals.csv              # 250 dirty deal rows
└── pipeline/
    ├── clean.py                   # Main ETL pipeline (the core deliverable)
    └── output/                    # Created on first run
        ├── clean_contacts.csv
        ├── clean_deals.csv
        ├── gtm_funnel.csv
        └── data_quality_scorecard.json
```

---

## Data quality scorecard (sample output)

```
-- CONTACTS --
  Rows in:                         420
  Exact duplicates removed:         20
  Rows out (clean):                400
  Unparseable dates:                 0
  Future-dated records flagged:      5
  Invalid emails:                   30
  Country variants normalised:      44  →  10 canonical
  Lifecycle variants normalised:    29  →  7 canonical
  Unassigned owners:                79

-- DEALS --
  Rows in:                         250
  Close-before-open errors:         16
  ARR missing or invalid:           45
  Deal names backfilled:            22
  Total clean ARR:              €7,820,317
```

---

## Stack

- **Python 3.10+** with pandas and numpy for ETL
- **Streamlit** for the interactive demo
- **Plotly** for charts

No external data sources, no API keys, no authentication. The pipeline
runs from scratch with a single command.

---

## What to say in the interview

> "The job description mentioned ETL, data cleaning, and downstream data
> quality for BI teams. Those are exactly the three things this project
> covers. I generated a messy CRM export that mimics what you'd actually
> see in a Dynamics 365 or HubSpot export, and built a pipeline to fix it.
> The most important parts are the lifecycle stage normalisation and the
> owner identity resolution — those are the two issues that most commonly
> break RevOps dashboards. Everything is documented with the reasoning,
> not just the code."

---

## About the author

Arqam Faiz Siddiqui — M.Sc. International Information Systems candidate
at FAU Erlangen-Nürnberg, with prior experience in data analysis at GSK
(55,000+ pharmacy outlets) and Excel-based operational dashboards at
T.M. Enterprises.

Not affiliated with or endorsed by Speexx.
