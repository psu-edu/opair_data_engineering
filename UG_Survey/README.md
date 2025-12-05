# UG_Survey

Lightweight ETL for loading Undergraduate Survey data (CSV â†’ SQL Server staging).

## Layout
- \src/ug_survey\ â€“ Python package (loader, config, logging).
- \config\ â€“ environment settings, field mappings, and table field specs.
- \logs\ â€“ rotating run logs.
- \data\ â€“ incoming/archive/quarantine CSV files.
- \scripts\ â€“ helper scripts.

## Quick start
1. Create venv: \python -m venv .venv\
2. Activate: \.venv\Scripts\Activate.ps1\
3. Install deps: \pip install -r requirements.txt\
4. Copy \.env.example\ to \.env\ and set connection values.
5. Run a test load: \python -m ug_survey.load_raw --file .\data\incoming\sample.csv\
