from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from flask import Flask, flash, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from importer import ImportErrorWithContext, run_import

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = Path(os.environ.get("CLIENT_IMPORT_RUNTIME_DIR", Path(tempfile.gettempdir()) / "client_import_webapp_runtime"))
UPLOAD_DIR = RUNTIME_DIR / "uploads"
REPORT_DIR = RUNTIME_DIR / "reports"
ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "client-import-secret")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


def allowed_file(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


@app.route("/", methods=["GET"])
def index():
    defaults = {
        "database_url": os.environ.get("DATABASE_URL", ""),
        "company_id": os.environ.get("COMPANY_ID", ""),
        "sheet_name": "",
        "delimiter": ",",
        "on_conflict": "update",
        "dry_run": False,
        "strict": False,
    }
    return render_template("index.html", defaults=defaults)


@app.route("/upload", methods=["POST"])
def upload():
    upload_file = request.files.get("file")
    database_url = (request.form.get("database_url") or os.environ.get("DATABASE_URL", "")).strip()
    company_id = (request.form.get("company_id") or os.environ.get("COMPANY_ID", "")).strip()
    sheet_name = (request.form.get("sheet_name") or "").strip() or None
    delimiter = (request.form.get("delimiter") or ",").strip() or ","
    on_conflict = (request.form.get("on_conflict") or "update").strip()
    dry_run = request.form.get("dry_run") == "on"
    strict = request.form.get("strict") == "on"

    form_values = {
        "database_url": database_url,
        "company_id": company_id,
        "sheet_name": sheet_name or "",
        "delimiter": delimiter,
        "on_conflict": on_conflict,
        "dry_run": dry_run,
        "strict": strict,
    }

    if not upload_file or not upload_file.filename:
        flash("Please choose a CSV or Excel file.", "error")
        return render_template("index.html", defaults=form_values)

    if not allowed_file(upload_file.filename):
        flash("Unsupported file type. Please upload .csv, .xlsx, or .xls.", "error")
        return render_template("index.html", defaults=form_values)

    if not company_id:
        flash("Company ID is required.", "error")
        return render_template("index.html", defaults=form_values)

    if not dry_run and not database_url:
        flash("Database URL is required unless you are doing a dry run.", "error")
        return render_template("index.html", defaults=form_values)

    original_name = secure_filename(upload_file.filename)
    unique_prefix = uuid.uuid4().hex
    upload_path = UPLOAD_DIR / f"{unique_prefix}_{original_name}"
    report_path = REPORT_DIR / f"report_{unique_prefix}.csv"
    upload_file.save(upload_path)

    try:
        result = run_import(
            file_path=upload_path,
            database_url=database_url,
            company_id=company_id,
            sheet_name=sheet_name,
            delimiter=delimiter,
            on_conflict=on_conflict,
            dry_run=dry_run,
            strict=strict,
            report_path=report_path,
        )
        return render_template(
            "result.html",
            filename=original_name,
            result=result,
            report_name=report_path.name,
            form_values=form_values,
        )
    except Exception as exc:
        message = str(exc)
        flash(message, "error")
        return render_template("index.html", defaults=form_values)


@app.route("/reports/<path:report_name>", methods=["GET"])
def download_report(report_name: str):
    report_path = REPORT_DIR / secure_filename(report_name)
    if not report_path.exists():
        flash("Report file not found.", "error")
        return redirect(url_for("index"))
    return send_file(report_path, as_attachment=True, download_name=report_path.name)


@app.errorhandler(413)
def too_large(_error):
    flash("File is too large. Maximum upload size is 16 MB.", "error")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", 5001)),
        debug=os.environ.get("FLASK_DEBUG", "0") == "1",
        use_reloader=False,
    )
