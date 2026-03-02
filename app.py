import os
import tempfile
from typing import List, Optional

from flask import Flask, render_template, request, redirect, url_for, session, flash, abort
from pymarc import MARCReader, Record

import marc_store

APP_SECRET = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "100"))
MAX_MB = int(os.environ.get("MAX_UPLOAD_MB", "10"))

ALLOWED_EXTS = {".mrc"}  # Day 1: only .mrc. We'll add .mrk later.

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * 1024 * 1024  # upload cap


def _safe_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower()


def parse_mrc_path(path: str, max_records: int = MAX_RECORDS) -> List[Record]:
    records: List[Record] = []
    with open(path, "rb") as fh:
        reader = MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling="ignore")
        for rec in reader:
            if rec is None:
                continue
            records.append(rec)
            if len(records) >= max_records:
                break
    return records


def first_subfield(rec: Record, tag: str, code: str) -> Optional[str]:
    f = rec.get_fields(tag)
    if not f:
        return None
    # tag is a data field here; pymarc fields have get_subfields
    vals = f[0].get_subfields(code)
    return vals[0] if vals else None


def control(rec: Record, tag: str) -> Optional[str]:
    cf = rec[tag]
    return str(cf.value()) if cf is not None else None


def best_title(rec: Record) -> str:
    a = first_subfield(rec, "245", "a")
    b = first_subfield(rec, "245", "b")
    if a and b:
        return f"{a} {b}".strip()
    return (a or b or "[no 245]")[:200]


def best_main_entry(rec: Record) -> str:
    a100 = first_subfield(rec, "100", "a")
    if a100:
        return a100[:200]
    a110 = first_subfield(rec, "110", "a")
    if a110:
        return a110[:200]
    return ""


def best_pub_year(rec: Record) -> str:
    # super rough Day 1 approach
    for tag in ("264", "260"):
        c = first_subfield(rec, tag, "c")
        if c:
            return c[:40]
    # fallback: 008 positions 7-10 (date1)
    f008 = control(rec, "008")
    if f008 and len(f008) >= 11:
        return f008[7:11]
    return ""


@app.get("/")
def index():
    marc_store.purge_older_than(3600)
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    return render_template(
        "index.html",
        stored=stored,
        max_records=MAX_RECORDS,
        max_mb=MAX_MB,
        allowed_exts=", ".join(sorted(ALLOWED_EXTS)),
    )


@app.post("/upload")
def upload():
    marc_store.purge_older_than(3600)

    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please choose a .mrc file to upload.", "error")
        return redirect(url_for("index"))

    ext = _safe_ext(f.filename)
    if ext not in ALLOWED_EXTS:
        flash(f"Unsupported file type: {ext}. Day 1 supports: {', '.join(sorted(ALLOWED_EXTS))}", "error")
        return redirect(url_for("index"))

    # Write to a temp file then parse (don’t keep uploads around)
    fd, tmp_path = tempfile.mkstemp(prefix="upload_", suffix=ext)
    os.close(fd)
    try:
        f.save(tmp_path)
        records = parse_mrc_path(tmp_path, max_records=MAX_RECORDS)
    except Exception as e:
        flash(f"Could not parse MARC: {e}", "error")
        return redirect(url_for("index"))
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    if not records:
        flash("Parsed 0 records. Is this a valid MARC (.mrc) file?", "error")
        return redirect(url_for("index"))

    sid = session.get("sid")
    if not sid:
        sid = marc_store.new_session_id()
        session["sid"] = sid

    marc_store.put(sid, filename=f.filename, records=records)
    flash(f"Loaded {len(records)} record(s) from {f.filename}", "ok")
    return redirect(url_for("records"))


@app.get("/records")
def records():
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))

    rows = []
    for i, rec in enumerate(stored.records):
        rows.append(
            {
                "i": i,
                "001": control(rec, "001") or "",
                "title": best_title(rec),
                "main": best_main_entry(rec),
                "year": best_pub_year(rec),
            }
        )

    return render_template("records.html", filename=stored.filename, count=len(rows), rows=rows)


@app.get("/record/<int:i>")
def record_detail(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))

    if i < 0 or i >= len(stored.records):
        abort(404)

    rec = stored.records[i]

    # Build a simple display list:
    # - control fields first
    # - then data fields with indicators + subfields
    control_fields = []
    data_fields = []

    for field in rec.get_fields():
        if field.is_control_field():
            control_fields.append((field.tag, field.value()))
        else:
            subs = []
            for code, val in field.subfields:
                subs.append((code, val))
            data_fields.append((field.tag, field.indicators[0], field.indicators[1], subs))

    return render_template(
        "record.html",
        i=i,
        filename=stored.filename,
        count=len(stored.records),
        control_fields=control_fields,
        data_fields=data_fields,
    )


if __name__ == "__main__":
    app.run(debug=True)
