"""
db.py — PostgreSQL connection + query helpers + state machine

State machine:
  미연락 ──────────────────────► 연락함
                                     │
              ┌──────────────────────┤
              │                      │
              ▼                      ▼
           답변옴           (follow_up_date set,
              │              status stays 연락함)
              │
              ▼
          데모예약
              │
              ▼
   클로즈 (close_outcome = 'won' | 'lost')

  Any → 미연락  (reset)
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# ── Connection ─────────────────────────────────────────────────────────────────

def get_conn():
    """Open a psycopg2 connection from DATABASE_URL env var."""
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

@contextmanager
def db_conn():
    """Context manager: auto-commit or rollback."""
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ── State machine ──────────────────────────────────────────────────────────────

STATUSES = ["미연락", "연락함", "답변옴", "데모예약", "클로즈"]

VALID_TRANSITIONS = {
    "미연락":  {"연락함", "미연락"},
    "연락함":  {"답변옴", "연락함", "미연락"},
    "답변옴":  {"데모예약", "미연락"},
    "데모예약": {"클로즈", "미연락"},
    "클로즈":  {"미연락"},
}

def validate_transition(current: str, target: str, close_outcome: str = None) -> None:
    """Raise ValueError if the transition is invalid."""
    allowed = VALID_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError("유효하지 않은 상태 전환입니다")
    if target == "클로즈" and close_outcome not in ("won", "lost"):
        raise ValueError("클로즈 결과를 선택해주세요 (성공/실패)")

# ── Settings ───────────────────────────────────────────────────────────────────

def get_setting(key: str) -> str | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cur.fetchone()
            return row["value"] if row else None

def set_setting(key: str, value: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO settings (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                (key, value),
            )

# ── Contacts ───────────────────────────────────────────────────────────────────

def get_contact(contact_id: int) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM contacts WHERE id = %s", (contact_id,))
            return cur.fetchone()

def list_contacts(
    page: int = 1,
    per_page: int = 50,
    region: str = None,
    party: str = None,
    council: str = None,
    status: str = None,
    tag: str = None,
    search: str = None,
    today_targets: bool = False,
) -> tuple[list, int]:
    """Returns (rows, total_count)."""
    conditions = []
    params = []

    if today_targets:
        conditions.append("c.status = '미연락' AND c.email IS NOT NULL")
    else:
        if region:
            conditions.append("c.region = %s")
            params.append(region)
        if party:
            conditions.append("c.party = %s")
            params.append(party)
        if council:
            conditions.append("c.council = %s")
            params.append(council)
        if status:
            conditions.append("c.status = %s")
            params.append(status)
        if search:
            conditions.append("(c.name ILIKE %s OR c.council ILIKE %s OR c.email ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    tag_join = ""
    if tag:
        tag_join = """
            JOIN contact_tags ct ON ct.contact_id = c.id
            JOIN tags t ON t.id = ct.tag_id AND t.name = %s
        """
        params.insert(0, tag)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    base_query = f"""
        SELECT DISTINCT c.*,
               (SELECT STRING_AGG(t2.name, ',' ORDER BY t2.name)
                FROM contact_tags ct2 JOIN tags t2 ON t2.id = ct2.tag_id
                WHERE ct2.contact_id = c.id) AS tag_names
        FROM contacts c
        {tag_join}
        {where}
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            count_params = ([tag] if tag else []) + params
            cur.execute(
                f"SELECT COUNT(*) AS n FROM contacts c {tag_join} {where}",
                count_params,
            )
            total = cur.fetchone()["n"]

            offset = (page - 1) * per_page
            all_params = count_params + [per_page, offset]
            cur.execute(
                base_query + " ORDER BY c.region, c.council, c.name LIMIT %s OFFSET %s",
                all_params,
            )
            rows = cur.fetchall()

    return rows, total

def update_contact_status(
    contact_id: int,
    target: str,
    close_outcome: str = None,
    follow_up_date: str = None,
) -> None:
    contact = get_contact(contact_id)
    if not contact:
        raise KeyError("연락처를 찾을 수 없습니다")
    validate_transition(contact["status"], target, close_outcome)

    with db_conn() as conn:
        with conn.cursor() as cur:
            if target == "미연락":
                cur.execute(
                    "UPDATE contacts SET status=%s, close_outcome=NULL, follow_up_date=NULL WHERE id=%s",
                    (target, contact_id),
                )
            elif target == "클로즈":
                cur.execute(
                    "UPDATE contacts SET status=%s, close_outcome=%s WHERE id=%s",
                    (target, close_outcome, contact_id),
                )
            else:
                cur.execute(
                    "UPDATE contacts SET status=%s WHERE id=%s",
                    (target, contact_id),
                )
            if follow_up_date is not None:
                cur.execute(
                    "UPDATE contacts SET follow_up_date=%s WHERE id=%s",
                    (follow_up_date or None, contact_id),
                )

def update_contact_notes(contact_id: int, notes: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE contacts SET notes=%s WHERE id=%s",
                (notes, contact_id),
            )

# ── Outreach log ───────────────────────────────────────────────────────────────

def log_outreach(
    contact_id: int,
    channel: str,
    direction: str,
    subject: str = None,
    body: str = None,
    gmail_message_id: str = None,
    notes: str = None,
    logged_at: str = None,
) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            if logged_at:
                cur.execute(
                    """INSERT INTO outreach_log
                       (contact_id, channel, direction, subject, body, gmail_message_id, notes, logged_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (contact_id, channel, direction, subject, body, gmail_message_id, notes, logged_at),
                )
            else:
                cur.execute(
                    """INSERT INTO outreach_log
                       (contact_id, channel, direction, subject, body, gmail_message_id, notes)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (contact_id, channel, direction, subject, body, gmail_message_id, notes),
                )

def get_outreach_log(contact_id: int) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM outreach_log WHERE contact_id=%s ORDER BY logged_at DESC",
                (contact_id,),
            )
            return cur.fetchall()

def sends_today() -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) AS n FROM outreach_log
                   WHERE channel='email' AND direction='outbound'
                   AND logged_at::date = CURRENT_DATE""",
            )
            return cur.fetchone()["n"]

# ── Tags ───────────────────────────────────────────────────────────────────────

def get_contact_tags(contact_id: int) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT t.* FROM tags t
                   JOIN contact_tags ct ON ct.tag_id = t.id
                   WHERE ct.contact_id = %s ORDER BY t.name""",
                (contact_id,),
            )
            return cur.fetchall()

def all_tags() -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tags ORDER BY name")
            return cur.fetchall()

def add_tag_to_contact(contact_id: int, tag_name: str) -> None:
    tag_name = tag_name[:30].strip()
    if not tag_name:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tags (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (tag_name,),
            )
            cur.execute("SELECT id FROM tags WHERE name = %s", (tag_name,))
            tag_id = cur.fetchone()["id"]
            cur.execute(
                "INSERT INTO contact_tags (contact_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (contact_id, tag_id),
            )

def remove_tag_from_contact(contact_id: int, tag_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM contact_tags WHERE contact_id=%s AND tag_id=%s",
                (contact_id, tag_id),
            )

# ── Templates ──────────────────────────────────────────────────────────────────

def get_default_template() -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM templates WHERE is_default=TRUE LIMIT 1")
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT * FROM templates ORDER BY id LIMIT 1")
                row = cur.fetchone()
            return row

def save_template(name: str, subject: str, body: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM templates WHERE is_default=TRUE LIMIT 1")
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE templates SET name=%s, subject=%s, body=%s WHERE id=%s",
                    (name, subject, body, row["id"]),
                )
            else:
                cur.execute(
                    "INSERT INTO templates (name, subject, body, is_default) VALUES (%s,%s,%s,TRUE)",
                    (name, subject, body),
                )

def fill_template_vars(template_text: str, contact: dict) -> str:
    term = contact.get("term")
    term_str = f"제{term}대" if term else "미확인"
    replacements = {
        "{의원명}": contact.get("name") or "미확인",
        "{의회명}": contact.get("council") or "미확인",
        "{선거구}": contact.get("district") or "미확인",
        "{정당}": contact.get("party") or "미확인",
        "{대수}": term_str,
    }
    for var, val in replacements.items():
        template_text = template_text.replace(var, val)
    return template_text

# ── Dashboard ──────────────────────────────────────────────────────────────────

def dashboard_data() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM contacts
                   WHERE follow_up_date < CURRENT_DATE
                   AND status IN ('연락함','답변옴')
                   ORDER BY follow_up_date ASC"""
            )
            overdue = cur.fetchall()

            cur.execute(
                """SELECT * FROM contacts
                   WHERE follow_up_date = CURRENT_DATE
                   ORDER BY name"""
            )
            due_today = cur.fetchall()

            cur.execute(
                """SELECT status, COUNT(*) AS n FROM contacts GROUP BY status"""
            )
            pipeline = {r["status"]: r["n"] for r in cur.fetchall()}

            cur.execute(
                """SELECT COUNT(*) AS n FROM outreach_log
                   WHERE channel='email' AND direction='outbound'
                   AND logged_at::date = CURRENT_DATE"""
            )
            sends = cur.fetchone()["n"]

    return {
        "overdue": overdue,
        "due_today": due_today,
        "pipeline": pipeline,
        "sends_today": sends,
    }

# ── Analytics ──────────────────────────────────────────────────────────────────

def analytics_data() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) AS n FROM contacts GROUP BY status")
            by_status = {r["status"]: r["n"] for r in cur.fetchall()}

            cur.execute("SELECT COUNT(*) AS n FROM contacts WHERE status != '미연락'")
            denom = cur.fetchone()["n"]

            cur.execute(
                "SELECT COUNT(*) AS n FROM contacts WHERE status IN ('답변옴','데모예약','클로즈')"
            )
            numer = cur.fetchone()["n"]

            cur.execute(
                """SELECT region,
                          COUNT(*) FILTER (WHERE status != '미연락') AS contacted,
                          COUNT(*) FILTER (WHERE status IN ('답변옴','데모예약','클로즈')) AS responded
                   FROM contacts GROUP BY region ORDER BY responded::float / NULLIF(contacted,0) DESC NULLS LAST"""
            )
            regions = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) AS n FROM contacts WHERE close_outcome='won'"
            )
            won = cur.fetchone()["n"]
            cur.execute(
                "SELECT COUNT(*) AS n FROM contacts WHERE close_outcome='lost'"
            )
            lost = cur.fetchone()["n"]

    return {
        "by_status": by_status,
        "denominator": denom,
        "numerator": numer,
        "response_rate": round(numer / denom * 100, 1) if denom else 0,
        "regions": regions,
        "demo_count": by_status.get("데모예약", 0),
        "won": won,
        "lost": lost,
        "total": sum(by_status.values()),
    }

# ── Pipeline (Kanban) ──────────────────────────────────────────────────────────

def pipeline_data() -> dict:
    result = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for status in STATUSES:
                cur.execute(
                    """SELECT c.*,
                              (SELECT STRING_AGG(t.name, ',' ORDER BY t.name)
                               FROM contact_tags ct JOIN tags t ON t.id=ct.tag_id
                               WHERE ct.contact_id=c.id) AS tag_names
                       FROM contacts c WHERE c.status=%s
                       ORDER BY c.follow_up_date ASC NULLS LAST, c.name
                       LIMIT 101""",
                    (status,),
                )
                rows = cur.fetchall()
                result[status] = {
                    "rows": rows[:100],
                    "overflow": max(0, len(rows) - 100),
                }
    return result

# ── CSV export ─────────────────────────────────────────────────────────────────

def export_contacts(status_filter: str = None, region_filter: str = None):
    """Generator yielding dicts for CSV export."""
    conditions = []
    params = []
    if status_filter and status_filter != "all":
        conditions.append("c.status = %s")
        params.append(status_filter)
    if region_filter and region_filter != "all":
        conditions.append("c.region = %s")
        params.append(region_filter)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT c.region, c.council, c.name, c.party, c.district, c.email,
                           c.status, c.follow_up_date,
                           MAX(ol.logged_at) AS last_contact,
                           COUNT(ol.id) FILTER (WHERE ol.direction='outbound') AS contact_count
                    FROM contacts c
                    LEFT JOIN outreach_log ol ON ol.contact_id = c.id
                    {where}
                    GROUP BY c.id
                    ORDER BY c.region, c.council, c.name""",
                params,
            )
            for row in cur:
                yield dict(row)

# ── Regions list ───────────────────────────────────────────────────────────────

def distinct_regions() -> list[str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT region FROM contacts ORDER BY region")
            return [r["region"] for r in cur.fetchall()]

def distinct_parties() -> list[str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT party FROM contacts WHERE party IS NOT NULL ORDER BY party")
            return [r["party"] for r in cur.fetchall()]

# ── Bulk send queue ────────────────────────────────────────────────────────────

def enqueue_bulk_send(
    job_id: str,
    template_id: int,
    region: str = None,
    party: str = None,
    status: str = None,
    tag: str = None,
) -> tuple[int, int]:
    """Insert matching contacts into send_queue. Returns (inserted, skipped_dedup)."""
    conditions = ["c.email IS NOT NULL"]
    params = [job_id, template_id]
    tag_join = ""

    if region:
        conditions.append("c.region = %s")
        params.append(region)
    if party:
        conditions.append("c.party = %s")
        params.append(party)
    if status:
        conditions.append("c.status = %s")
        params.append(status)
    if tag:
        tag_join = "JOIN contact_tags ct ON ct.contact_id=c.id JOIN tags t ON t.id=ct.tag_id AND t.name=%s"
        params.insert(2, tag)

    where = "WHERE " + " AND ".join(conditions + [
        "NOT EXISTS (SELECT 1 FROM send_queue sq WHERE sq.contact_id=c.id AND sq.status IN ('pending','sent'))"
    ])

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Count total matching (before dedup)
            count_params = ([tag] if tag else []) + params[2:]
            cur.execute(
                f"SELECT COUNT(*) AS n FROM contacts c {tag_join} {'WHERE ' + ' AND '.join(conditions) if conditions else ''}",
                count_params,
            )
            total_matching = cur.fetchone()["n"]

            cur.execute(
                f"""INSERT INTO send_queue (contact_id, job_id, template_id, status)
                    SELECT DISTINCT c.id, %s, %s, 'pending'
                    FROM contacts c {tag_join}
                    {where}""",
                params,
            )
            inserted = cur.rowcount

    return inserted, total_matching - inserted

def get_queue_status(job_id: str) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT status, COUNT(*) AS n FROM send_queue WHERE job_id=%s GROUP BY status""",
                (job_id,),
            )
            counts = {r["status"]: r["n"] for r in cur.fetchall()}

            cur.execute(
                """SELECT sq.*, c.name, c.council, c.email
                   FROM send_queue sq JOIN contacts c ON c.id=sq.contact_id
                   WHERE sq.job_id=%s ORDER BY sq.queued_at""",
                (job_id,),
            )
            rows = cur.fetchall()

    return {"counts": counts, "rows": rows}

def retry_failed_queue(job_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE send_queue SET status='pending', error=NULL, sent_at=NULL WHERE job_id=%s AND status='failed'",
                (job_id,),
            )
            return cur.rowcount

def get_pending_queue_batch(limit: int = 100) -> list:
    """SELECT FOR UPDATE SKIP LOCKED — safe for concurrent cron runs."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT sq.*, c.*, t.subject AS tmpl_subject, t.body AS tmpl_body
                   FROM send_queue sq
                   JOIN contacts c ON c.id = sq.contact_id
                   LEFT JOIN templates t ON t.id = sq.template_id
                   WHERE sq.status = 'pending'
                   ORDER BY sq.queued_at
                   LIMIT %s
                   FOR UPDATE OF sq SKIP LOCKED""",
                (limit,),
            )
            return cur.fetchall()

def mark_queue_sent(queue_id: int, gmail_message_id: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE send_queue SET status='sent', sent_at=NOW() WHERE id=%s",
                (queue_id,),
            )

def mark_queue_failed(queue_id: int, error: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE send_queue SET status='failed', error=%s WHERE id=%s",
                (error, queue_id),
            )

def mark_remaining_skipped(job_id: str, reason: str = "daily_limit") -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE send_queue SET status='skipped', error=%s WHERE job_id=%s AND status='pending'",
                (reason, job_id),
            )
            return cur.rowcount
