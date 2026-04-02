# Client Bulk Import Web App

A simple Flask UI around the uploaded client import script.

## Features
- Upload `.csv`, `.xlsx`, or `.xls`
- Map spreadsheet headers like `Particulars` to `company_name`
- Dry-run preview before writing to PostgreSQL
- Insert or update existing clients by `(company_id, company_name)`
- Download a CSV report of inserted, updated, skipped, and error rows

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python3 app.py
```

Open:

```text
http://127.0.0.1:5001
```

## Optional environment variables

```bash
export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
export COMPANY_ID='cmp_main_babanamak'
export FLASK_SECRET_KEY='change-me'
```

## Notes
- For live imports, the `Client` table must already exist.
- If your database password contains `@`, encode it as `%40` inside the URL.
- The app limits uploads to 16 MB.


## Important
- Uploads and reports are now written to a runtime folder outside the project directory by default, so Flask will not restart during uploads.
- The app starts with the auto-reloader disabled.
# client_uploader_webapp
