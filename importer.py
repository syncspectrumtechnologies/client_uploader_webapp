from __future__ import annotations

import csv
import json
import math
import os
import re
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PINCODE_RE = re.compile(r'(?<!\d)(\d{6})(?!\d)')

TARGET_FIELDS = [
    "company_name",
    "gstin",
    "registration_type",
    "pan_it_no",
    "phone",
    "mobile_no",
    "email",
    "address",
    "city",
    "state",
    "country",
    "pincode",
]

HEADER_ALIASES = {
    "company_name": [
        "company_name",
        "company name",
        "particulars",
        "client",
        "client name",
        "name",
        "party name",
        "customer",
        "customer name",
    ],
    "gstin": ["gstin", "gstin/uin", "gstin uin", "gst no", "gstin no", "gst number"],
    "registration_type": [
        "registration_type",
        "registration type",
        "reg type",
        "type of registration",
        "registration",
    ],
    "pan_it_no": [
        "pan_it_no",
        "pan/it no.",
        "pan/it no",
        "pan it no",
        "pan",
        "pan no",
        "it no",
    ],
    "phone": ["phone", "phone no", "phone no.", "phone number", "telephone", "landline"],
    "mobile_no": ["mobile_no", "mobile no", "mobile nos", "mobile nos.", "mobile number", "mobile", "mobile nos/whatsapp"],
    "email": ["email", "e-mail", "e-mail id", "email id", "mail id"],
    "address": ["address", "billing address", "location"],
    "city": ["city", "town"],
    "state": ["state", "province", "region"],
    "country": ["country", "nation"],
    "pincode": ["pincode", "pin code", "postal code", "zipcode", "zip code"],
}

FIELD_ORDER_SQL = [
    "id",
    "company_id",
    "company_name",
    "gstin",
    "registration_type",
    "pan_it_no",
    "phone",
    "mobile_no",
    "email",
    "address",
    "city",
    "state",
    "country",
    "pincode",
    "is_active",
    "created_at",
    "updated_at",
]

CREATE_SQL_TEMPLATE = """
INSERT INTO "Client" (
  "id", "company_id", "company_name", "gstin", "registration_type",
  "pan_it_no", "phone", "mobile_no", "email", "address",
  "city", "state", "country", "pincode", "is_active", "created_at", "updated_at"
) VALUES (
  %(id)s, %(company_id)s, %(company_name)s, %(gstin)s, %(registration_type)s,
  %(pan_it_no)s, %(phone)s, %(mobile_no)s, %(email)s, %(address)s,
  %(city)s, %(state)s, %(country)s, %(pincode)s, %(is_active)s, %(created_at)s, %(updated_at)s
)
{conflict_clause}
"""


@dataclass
class RowResult:
    source_row: int
    action: str
    company_name: Optional[str]
    message: str


class ImportErrorWithContext(Exception):
    pass


def cuid_like() -> str:
    ts = base36(int(time.time() * 1000))
    rnd = uuid.uuid4().hex[:16]
    core = f"{ts}{rnd}"
    core = re.sub(r"[^a-z0-9]", "", core.lower())
    return "c" + core[:24].ljust(24, "0")



def base36(num: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if num == 0:
        return "0"
    out = []
    while num:
        num, rem = divmod(num, 36)
        out.append(chars[rem])
    return "".join(reversed(out))



def normalize_header(value: Any) -> str:
    s = "" if value is None else str(value)
    s = s.strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"[^\w\s/]+", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def empty_to_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s



def extract_pincode(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    m = PINCODE_RE.search(address)
    return m.group(1) if m else None



def build_column_map(df: pd.DataFrame) -> Dict[str, str]:
    normalized_to_original = {normalize_header(col): str(col) for col in df.columns}
    result: Dict[str, str] = {}

    for target, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            norm = normalize_header(alias)
            if norm in normalized_to_original:
                result[target] = normalized_to_original[norm]
                break
    return result



def load_dataframe(path: Path, sheet_name: Optional[str], delimiter: str) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name or 0, dtype=object)
    if suffix == ".csv":
        return pd.read_csv(path, dtype=object, keep_default_na=False, sep=delimiter)
    raise ImportErrorWithContext(f"Unsupported file type: {suffix}. Use .xlsx, .xls, or .csv")



def transform_rows(df: pd.DataFrame, company_id: str, strict: bool) -> Tuple[List[Dict[str, Any]], List[RowResult], Dict[str, str]]:
    col_map = build_column_map(df)
    notices: List[RowResult] = []
    rows: List[Dict[str, Any]] = []

    if "company_name" not in col_map:
        raise ImportErrorWithContext("Missing required column: company_name / particulars")

    for idx, rec in enumerate(df.to_dict(orient="records"), start=2):
        out: Dict[str, Any] = {field: None for field in TARGET_FIELDS}
        for target, source_col in col_map.items():
            out[target] = empty_to_none(rec.get(source_col))

        if out["email"]:
            out["email"] = out["email"].lower()
        if not out["pincode"]:
            out["pincode"] = extract_pincode(out["address"])

        company_name = out["company_name"]
        if company_name:
            company_name = re.sub(r"\s+", " ", company_name).strip()
            out["company_name"] = company_name

        if not company_name:
            notices.append(RowResult(idx, "error", None, "Missing company name"))
            continue

        current_time = datetime.utcnow()
        row = {
            "id": cuid_like(),
            "company_id": company_id,
            **out,
            "is_active": True,
            "created_at": current_time,
            "updated_at": current_time,
            "_source_row": idx,
        }
        rows.append(row)

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = row["company_name"].strip().lower()
        if strict and key in deduped:
            notices.append(RowResult(row["_source_row"], "warning", row["company_name"], "Duplicate in file; later row kept"))
        deduped[key] = row
    rows = list(deduped.values())

    return rows, notices, col_map



def connect_db(database_url: str):
    if not database_url:
        raise ImportErrorWithContext("Database URL is required")

    last_err = None
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(database_url)
        return conn, "psycopg2"
    except Exception as e:
        last_err = e

    try:
        import psycopg  # type: ignore
        conn = psycopg.connect(database_url)
        return conn, "psycopg"
    except Exception as e:
        last_err = e

    raise ImportErrorWithContext(
        "Could not connect because neither psycopg2 nor psycopg is available or connection failed. "
        "Install one of these first: pip install psycopg2-binary OR pip install psycopg[binary]. "
        f"Last error: {last_err}"
    )



def build_sql(conflict_mode: str) -> str:
    if conflict_mode == "skip":
        conflict_clause = 'ON CONFLICT ("company_id", "company_name") DO NOTHING'
    elif conflict_mode == "update":
        conflict_clause = """
ON CONFLICT ("company_id", "company_name") DO UPDATE SET
  "gstin" = EXCLUDED."gstin",
  "registration_type" = EXCLUDED."registration_type",
  "pan_it_no" = EXCLUDED."pan_it_no",
  "phone" = EXCLUDED."phone",
  "mobile_no" = EXCLUDED."mobile_no",
  "email" = EXCLUDED."email",
  "address" = EXCLUDED."address",
  "city" = EXCLUDED."city",
  "state" = EXCLUDED."state",
  "country" = EXCLUDED."country",
  "pincode" = EXCLUDED."pincode",
  "is_active" = TRUE,
  "updated_at" = NOW()
"""
    else:
        raise ImportErrorWithContext("conflict_mode must be 'skip' or 'update'")
    return CREATE_SQL_TEMPLATE.format(conflict_clause=conflict_clause)



def write_report(report_path: Path, results: List[RowResult]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_row", "action", "company_name", "message"])
        for r in results:
            writer.writerow([r.source_row, r.action, r.company_name or "", r.message])



def existing_client_names(conn, company_id: str) -> set[str]:
    cur = conn.cursor()
    cur.execute('SELECT "company_name" FROM "Client" WHERE "company_id" = %s', (company_id,))
    vals = {str(row[0]).strip().lower() for row in cur.fetchall()}
    cur.close()
    return vals



def import_rows(conn, rows: List[Dict[str, Any]], company_id: str, conflict_mode: str) -> List[RowResult]:
    sql = build_sql(conflict_mode)
    preexisting = existing_client_names(conn, company_id)
    results: List[RowResult] = []

    cur = conn.cursor()
    try:
        for row in rows:
            try:
                cur.execute(sql, {k: row.get(k) for k in FIELD_ORDER_SQL})
                company_key = row["company_name"].strip().lower()
                if conflict_mode == "skip":
                    if company_key in preexisting:
                        results.append(RowResult(row["_source_row"], "skipped", row["company_name"], "Client already exists for this company"))
                    else:
                        results.append(RowResult(row["_source_row"], "inserted", row["company_name"], "Inserted"))
                        preexisting.add(company_key)
                else:
                    if company_key in preexisting:
                        results.append(RowResult(row["_source_row"], "updated", row["company_name"], "Updated existing client"))
                    else:
                        results.append(RowResult(row["_source_row"], "inserted", row["company_name"], "Inserted"))
                        preexisting.add(company_key)
            except Exception as exc:
                results.append(RowResult(row["_source_row"], "error", row["company_name"], str(exc)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return results



def run_import(
    file_path: Path,
    database_url: str,
    company_id: str,
    sheet_name: Optional[str] = None,
    delimiter: str = ",",
    on_conflict: str = "update",
    dry_run: bool = False,
    strict: bool = False,
    report_path: Optional[Path] = None,
) -> Dict[str, Any]:
    if not file_path.exists():
        raise ImportErrorWithContext(f"File not found: {file_path}")

    df = load_dataframe(file_path, sheet_name, delimiter)
    rows, precheck_results, col_map = transform_rows(df, company_id, strict)

    preview = [{k: r.get(k) for k in TARGET_FIELDS} for r in rows[:5]]
    all_results = list(precheck_results)

    driver = None
    if not dry_run:
        conn, driver = connect_db(database_url)
        try:
            db_results = import_rows(conn, rows, company_id, on_conflict)
            all_results.extend(db_results)
        finally:
            conn.close()

    if report_path is not None:
        write_report(report_path, all_results)

    summary = {
        "inserted": sum(1 for r in all_results if r.action == "inserted"),
        "updated": sum(1 for r in all_results if r.action == "updated"),
        "skipped": sum(1 for r in all_results if r.action == "skipped"),
        "warnings": sum(1 for r in all_results if r.action == "warning"),
        "errors": sum(1 for r in all_results if r.action == "error"),
    }

    return {
        "detected_column_mapping": col_map,
        "parsed_valid_rows": len(rows),
        "preview": preview,
        "results": [asdict(r) for r in all_results],
        "summary": summary,
        "driver": driver,
        "dry_run": dry_run,
        "report_path": str(report_path) if report_path else None,
    }



def _cli() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Bulk import clients from CSV/XLSX into PostgreSQL.")
    parser.add_argument("--file", required=True, help="Path to CSV/XLSX input file")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="PostgreSQL connection URL")
    parser.add_argument("--company-id", required=True, help="company_id value to write into Client rows")
    parser.add_argument("--sheet-name", default=None, help="Excel sheet name (optional)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    parser.add_argument("--on-conflict", choices=["skip", "update"], default="update", help="How to handle existing rows")
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview without writing to DB")
    parser.add_argument("--strict", action="store_true", help="Emit warnings for duplicate company names inside the file")
    parser.add_argument("--report-file", default="client_import_report.csv", help="CSV report output path")
    args = parser.parse_args()

    try:
        result = run_import(
            file_path=Path(args.file),
            database_url=args.database_url,
            company_id=args.company_id,
            sheet_name=args.sheet_name,
            delimiter=args.delimiter,
            on_conflict=args.on_conflict,
            dry_run=args.dry_run,
            strict=args.strict,
            report_path=Path(args.report_file),
        )
        print("Detected column mapping:")
        print(json.dumps(result["detected_column_mapping"], indent=2))
        print(f"\nParsed valid rows: {result['parsed_valid_rows']}")
        if result["preview"]:
            print("\nPreview (first 5 rows):")
            print(json.dumps(result["preview"], indent=2))
        print("\nImport summary:")
        print(json.dumps({**result["summary"], "report_file": result["report_path"]}, indent=2))
        return 0 if result["summary"]["errors"] == 0 else 1
    except Exception as exc:
        print(str(exc), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
