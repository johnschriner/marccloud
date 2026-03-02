# MARCCloud

A lightweight, browser-based MARC record viewer/editor built with Flask and pymarc.

## What it does

- Upload a `.mrc` file (loads up to a configurable record cap)
- Browse a record list with quick “at a glance” fields (001, 245, 100/110, date)
- View an individual record’s control + data fields
- Edit a record using a raw, mrk-like text editor
- Export the edited record set back to `.mrc`

## Raw editor format

Each line starts with `=TAG␠␠` (tag + two spaces).

Notes:
- Data fields include two indicators (use spaces if blank).
- Escape a literal dollar sign inside a value as `\$`.

## Configuration (environment variables)

- `FLASK_SECRET_KEY` (required for sessions)
- `MAX_RECORDS` (default: 100)
- `MAX_UPLOAD_MB` (default: 10)

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export FLASK_SECRET_KEY="change-me"
flask --app app run --debug
