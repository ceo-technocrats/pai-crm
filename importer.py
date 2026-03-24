"""
importer.py — CSV → PostgreSQL contact importer

Reads CSV files produced by the council crawler (UTF-8 or EUC-KR).
Upserts into contacts table on docid (unique key).
Skips rows missing required fields (region, council, name).
Returns (inserted, updated, skipped) counts.
"""

import csv
import io

import db


REQUIRED = ("region", "council", "name")

# Map CSV column names → DB column names
COLUMN_MAP = {
    "지역": "region",
    "region": "region",
    "의회": "council",
    "council": "council",
    "이름": "name",
    "name": "name",
    "정당": "party",
    "party": "party",
    "선거구": "district",
    "district": "district",
    "기수": "term",
    "term": "term",
    "이메일": "email",
    "email": "email",
    "사무실전화": "phone_office",
    "phone_office": "phone_office",
    "휴대폰": "phone_mobile",
    "phone_mobile": "phone_mobile",
    "팩스": "fax",
    "fax": "fax",
    "docid": "docid",
}


def import_csv(file_bytes: bytes) -> tuple[int, int, int]:
    """
    Parse CSV bytes (UTF-8 or EUC-KR fallback), upsert contacts.
    Returns (inserted, updated, skipped).
    """
    text = _decode(file_bytes)
    reader = csv.DictReader(io.StringIO(text))

    inserted = updated = skipped = 0

    with db.db_conn() as conn:
        with conn.cursor() as cur:
            for row in reader:
                mapped = _map_row(row)
                if not mapped:
                    skipped += 1
                    continue

                docid = mapped.get("docid")

                if docid:
                    # Check if exists
                    cur.execute("SELECT id FROM contacts WHERE docid = %s", (docid,))
                    existing = cur.fetchone()
                else:
                    existing = None

                if existing:
                    # Update non-status fields only (preserve CRM state)
                    cur.execute("""
                        UPDATE contacts SET
                            region       = %s,
                            council      = %s,
                            name         = %s,
                            party        = %s,
                            district     = %s,
                            term         = %s,
                            email        = %s,
                            phone_office = %s,
                            phone_mobile = %s,
                            fax          = %s
                        WHERE docid = %s
                    """, (
                        mapped["region"], mapped["council"], mapped["name"],
                        mapped.get("party"), mapped.get("district"),
                        mapped.get("term"), mapped.get("email"),
                        mapped.get("phone_office"), mapped.get("phone_mobile"),
                        mapped.get("fax"), docid,
                    ))
                    updated += 1
                else:
                    cur.execute("""
                        INSERT INTO contacts
                            (region, council, name, party, district, term,
                             email, phone_office, phone_mobile, fax, docid)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (docid) DO NOTHING
                    """, (
                        mapped["region"], mapped["council"], mapped["name"],
                        mapped.get("party"), mapped.get("district"),
                        mapped.get("term"), mapped.get("email"),
                        mapped.get("phone_office"), mapped.get("phone_mobile"),
                        mapped.get("fax"), docid,
                    ))
                    if cur.rowcount:
                        inserted += 1
                    else:
                        skipped += 1

    return inserted, updated, skipped


def _decode(file_bytes: bytes) -> str:
    """Try UTF-8 first, fall back to EUC-KR."""
    try:
        return file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return file_bytes.decode("euc-kr", errors="replace")


def _map_row(row: dict) -> dict | None:
    """Map CSV headers to DB columns. Returns None if required fields missing."""
    mapped = {}
    for csv_col, value in row.items():
        db_col = COLUMN_MAP.get(csv_col.strip())
        if db_col:
            mapped[db_col] = value.strip() if value else None

    # Validate required fields
    for field in REQUIRED:
        if not mapped.get(field):
            return None

    # term must be integer or None
    if mapped.get("term"):
        try:
            mapped["term"] = int(mapped["term"])
        except (ValueError, TypeError):
            mapped["term"] = None

    return mapped
