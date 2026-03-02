import os
import tempfile
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Tuple

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from pymarc import Field, MARCReader, MARCWriter, Record, Subfield

import marc_store

APP_SECRET = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "100"))
MAX_MB = int(os.environ.get("MAX_UPLOAD_MB", "10"))

ALLOWED_EXTS = {".mrc"}  # We'll add .mrk later.

app = Flask(__name__)
app.secret_key = APP_SECRET
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * 1024 * 1024  # upload cap


def _safe_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower()


def parse_mrc_path(path: str, max_records: int = MAX_RECORDS) -> List[Record]:
    records: List[Record] = []
    with open(path, "rb") as fh:
        reader = MARCReader(
            fh,
            to_unicode=True,
            force_utf8=True,
            utf8_handling="ignore",
        )
        for rec in reader:
            if rec is None:
                continue
            records.append(rec)
            if len(records) >= max_records:
                break
    return records


def _iter_subfields(field: Field) -> List[Tuple[str, str]]:
    """
    Yield (code, value) pairs from a pymarc Field in a version-tolerant way.

    Supports:
    - [Subfield(code='a', value='Title'), ...]
    - [('a','Title'), ...]
    - ['a','Title','b','Subtitle', ...]
    """
    subs = getattr(field, "subfields", None)
    if not subs:
        return []

    first = subs[0]

    # Subfield objects
    if hasattr(first, "code") and hasattr(first, "value"):
        return [(sf.code, sf.value) for sf in subs]  # type: ignore[attr-defined]

    # tuples
    if isinstance(first, tuple) and len(first) == 2:
        return [(c, v) for (c, v) in subs]

    # flat list
    if isinstance(first, str):
        out: List[Tuple[str, str]] = []
        for i in range(0, len(subs), 2):
            if i + 1 < len(subs):
                out.append((subs[i], subs[i + 1]))
        return out

    return []


def first_subfield(rec: Record, tag: str, code: str) -> Optional[str]:
    f = rec.get_fields(tag)
    if not f:
        return None
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
    for tag in ("264", "260"):
        c = first_subfield(rec, tag, "c")
        if c:
            return c[:40]
    f008 = control(rec, "008")
    if f008 and len(f008) >= 11:
        return f008[7:11]
    return ""


def record_to_mrk(rec: Record) -> str:
    """
    Serialize a Record to strict mrk-like lines.
    """
    lines: List[str] = []

    leader = str(rec.leader) if rec.leader is not None else ""
    leader = (leader + " " * 24)[:24]
    lines.append(f"=LDR  {leader}")

    # Control fields
    for f in rec.get_fields():
        if f.is_control_field():
            lines.append(f"={f.tag}  {f.value()}")

    # Data fields
    for f in rec.get_fields():
        if f.is_control_field():
            continue

        ind1 = f.indicators[0] if f.indicators and len(f.indicators) > 0 else " "
        ind2 = f.indicators[1] if f.indicators and len(f.indicators) > 1 else " "
        ind1 = (ind1 or " ")[:1]
        ind2 = (ind2 or " ")[:1]

        parts: List[str] = []
        for code, val in _iter_subfields(f):
            val = (val or "").replace("$", r"\$")
            parts.append(f"${code}{val}")

        lines.append(f"={f.tag}  {ind1}{ind2}{''.join(parts)}")

    return "\n".join(lines) + "\n"


def mrk_to_record(text: str) -> Record:
    """
    Parse the strict mrk-like format produced by record_to_mrk().
    """
    rec = Record(force_utf8=True)

    lines = [ln.rstrip("\n") for ln in (text or "").splitlines()]
    for ln in lines:
        ln = ln.rstrip()
        if not ln:
            continue
        if not ln.startswith("=") or len(ln) < 6:
            raise ValueError(f"Bad line (expected '=TAG  ...'): {ln}")

        tag = ln[1:4]

        if tag == "LDR":
            if ln[4:6] != "  ":
                raise ValueError(f"Bad spacing after tag in line: {ln}")
            body = ln[6:]
            body = (body + " " * 24)[:24]
            rec.leader = body
            continue

        if not tag.isdigit():
            raise ValueError(f"Bad tag: {tag}")

        if ln[4:6] != "  ":
            raise ValueError(f"Bad spacing after tag in line: {ln}")

        body = ln[6:]

        if int(tag) < 10:
            rec.add_field(Field(tag=tag, data=body))
            continue

        ind1 = body[0:1] if len(body) >= 1 else " "
        ind2 = body[1:2] if len(body) >= 2 else " "
        rest = body[2:] if len(body) > 2 else ""

        subfields: List[Subfield] = []
        if rest:
            i = 0
            while i < len(rest):
                if rest[i] != "$":
                    raise ValueError(f"Expected '$' starting subfield in line: {ln}")
                if i + 1 >= len(rest):
                    raise ValueError(f"Dangling '$' at end of line: {ln}")
                code = rest[i + 1]
                i += 2

                buf: List[str] = []
                while i < len(rest):
                    if rest[i] == "\\" and i + 1 < len(rest) and rest[i + 1] == "$":
                        buf.append("$")
                        i += 2
                        continue
                    if rest[i] == "$":
                        break
                    buf.append(rest[i])
                    i += 1

                subfields.append(Subfield(code=code, value="".join(buf)))

        rec.add_field(Field(tag=tag, indicators=[ind1, ind2], subfields=subfields))

    return rec


def export_records_as_mrc(records: List[Record]) -> bytes:
    bio = BytesIO()
    writer = MARCWriter(bio)
    for r in records:
        writer.write(r)

    # Read bytes BEFORE closing (some pymarc versions close the underlying file-like object)
    data = bio.getvalue()

    try:
        writer.close()
    except Exception:
        pass

    return data


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


@app.get("/clear")
def clear():
    sid = session.get("sid")
    if sid:
        marc_store.delete(sid)
        session.pop("sid", None)
    flash("Cleared current session.", "ok")
    return redirect(url_for("index"))


@app.post("/upload")
def upload():
    marc_store.purge_older_than(3600)

    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please choose a .mrc file to upload.", "error")
        return redirect(url_for("index"))

    ext = _safe_ext(f.filename)
    if ext not in ALLOWED_EXTS:
        flash(
            f"Unsupported file type: {ext}. Supported: {', '.join(sorted(ALLOWED_EXTS))}",
            "error",
        )
        return redirect(url_for("index"))

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
    included_count = 0
    for i, rec in enumerate(stored.records):
        included = bool(stored.included[i])
        if included:
            included_count += 1
        rows.append(
            {
                "i": i,
                "included": included,
                "001": control(rec, "001") or "",
                "title": best_title(rec),
                "main": best_main_entry(rec),
                "year": best_pub_year(rec),
            }
        )

    return render_template(
        "records.html",
        filename=stored.filename,
        count=len(rows),
        included_count=included_count,
        rows=rows,
    )


@app.post("/record/<int:i>/exclude")
def record_exclude(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    stored.included[i] = False
    flash(f"Excluded record {i} from exports.", "ok")
    return redirect(url_for("records"))


@app.post("/record/<int:i>/include")
def record_include(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    stored.included[i] = True
    flash(f"Included record {i} in exports.", "ok")
    return redirect(url_for("records"))


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
    included = bool(stored.included[i])

    control_fields = []
    data_fields = []

    for field in rec.get_fields():
        if field.is_control_field():
            control_fields.append((field.tag, field.value()))
        else:
            subs = _iter_subfields(field)
            ind1 = field.indicators[0] if field.indicators and len(field.indicators) > 0 else " "
            ind2 = field.indicators[1] if field.indicators and len(field.indicators) > 1 else " "
            data_fields.append((field.tag, ind1, ind2, subs))

    return render_template(
        "record.html",
        i=i,
        filename=stored.filename,
        count=len(stored.records),
        included=included,
        control_fields=control_fields,
        data_fields=data_fields,
    )


@app.get("/record/<int:i>/raw")
def record_raw(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    rec = stored.records[i]
    raw = record_to_mrk(rec)
    return render_template("record_raw.html", i=i, filename=stored.filename, count=len(stored.records), raw=raw)


@app.post("/record/<int:i>/raw")
def record_raw_save(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    raw = request.form.get("raw", "")
    try:
        new_rec = mrk_to_record(raw)
    except Exception as e:
        flash(f"Could not parse your changes: {e}", "error")
        return render_template("record_raw.html", i=i, filename=stored.filename, count=len(stored.records), raw=raw)

    stored.records[i] = new_rec
    flash(f"Saved changes to record {i}.", "ok")
    return redirect(url_for("record_detail", i=i))


@app.get("/record/<int:i>/download.mrk")
def download_record_mrk(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    text = record_to_mrk(stored.records[i])
    base = os.path.splitext(stored.filename)[0] or "record"
    outname = f"{base}_record_{i}.mrk"
    return send_file(
        BytesIO(text.encode("utf-8")),
        mimetype="text/plain; charset=utf-8",
        as_attachment=True,
        download_name=outname,
    )


@app.get("/record/<int:i>/download.mrc")
def download_record_mrc(i: int):
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))
    if i < 0 or i >= len(stored.records):
        abort(404)

    data = export_records_as_mrc([stored.records[i]])
    base = os.path.splitext(stored.filename)[0] or "record"
    outname = f"{base}_record_{i}.mrc"
    return send_file(
        BytesIO(data),
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name=outname,
    )


@app.get("/export.mrc")
def export_mrc():
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))

    records_out = [r for r, inc in zip(stored.records, stored.included) if inc]
    data = export_records_as_mrc(records_out)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(stored.filename)[0] or "records"
    outname = f"{base}_edited_{ts}.mrc"

    return send_file(
        BytesIO(data),
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name=outname,
    )


@app.get("/export.mrk")
def export_mrk():
    sid = session.get("sid")
    stored = marc_store.get(sid) if sid else None
    if not stored:
        flash("No record set loaded yet.", "error")
        return redirect(url_for("index"))

    parts: List[str] = []
    for rec, inc in zip(stored.records, stored.included):
        if not inc:
            continue
        parts.append(record_to_mrk(rec).rstrip("\n"))

    text = "\n\n".join(parts) + "\n"

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(stored.filename)[0] or "records"
    outname = f"{base}_edited_{ts}.mrk"

    return send_file(
        BytesIO(text.encode("utf-8")),
        mimetype="text/plain; charset=utf-8",
        as_attachment=True,
        download_name=outname,
    )


if __name__ == "__main__":
    app.run(debug=True)