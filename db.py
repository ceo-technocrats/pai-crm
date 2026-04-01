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

# Fallback if DB is empty
_DEFAULT_STATUSES = ["미연락", "연락함", "답변옴", "데모예약", "클로즈"]


def get_statuses() -> list[str]:
    """Load pipeline stages from DB, ordered by position. No cache — serverless has no shared memory."""
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM pipeline_stages ORDER BY position")
                rows = cur.fetchall()
                return [r["name"] for r in rows] if rows else _DEFAULT_STATUSES
    except Exception:
        return _DEFAULT_STATUSES


# Keep STATUSES as a property-like accessor for backward compatibility
class _StatusesAccessor(list):
    """List that refreshes from DB on access."""
    def __iter__(self):
        return iter(get_statuses())
    def __len__(self):
        return len(get_statuses())
    def __getitem__(self, i):
        return get_statuses()[i]
    def __contains__(self, item):
        return item in get_statuses()

STATUSES = _StatusesAccessor()


def validate_transition(current: str, target: str, close_outcome: str = None) -> None:
    """Raise ValueError if the transition is invalid."""
    statuses = get_statuses()
    # Allow moving to any stage, or reset to first stage
    if target not in statuses:
        raise ValueError("유효하지 않은 상태입니다")


# ── Pipeline stage management ─────────────────────────────────────────────────

def get_pipeline_stages() -> list[dict]:
    """Return all stages ordered by position."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pipeline_stages ORDER BY position")
            return cur.fetchall()


def add_pipeline_stage(name: str) -> int:
    """Add a new stage at the end. Returns new stage ID."""

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(position), -1) + 1 AS next_pos FROM pipeline_stages")
            next_pos = cur.fetchone()["next_pos"]
            cur.execute(
                "INSERT INTO pipeline_stages (name, position) VALUES (%s, %s) RETURNING id",
                (name, next_pos),
            )
            return cur.fetchone()["id"]


def delete_pipeline_stage(stage_id: int) -> None:
    """Delete a stage. Contacts with this status keep their status string."""

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pipeline_stages WHERE id = %s", (stage_id,))


def reorder_pipeline_stages(stage_ids: list[int]) -> None:
    """Reorder stages by the given list of IDs."""

    with db_conn() as conn:
        with conn.cursor() as cur:
            for pos, sid in enumerate(stage_ids):
                cur.execute(
                    "UPDATE pipeline_stages SET position = %s WHERE id = %s",
                    (pos, sid),
                )


def rename_pipeline_stage(stage_id: int, new_name: str) -> None:
    """Rename a stage. Also updates all contacts with the old name."""

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM pipeline_stages WHERE id = %s", (stage_id,))
            row = cur.fetchone()
            if not row:
                return
            old_name = row["name"]
            cur.execute("UPDATE pipeline_stages SET name = %s WHERE id = %s", (new_name, stage_id))
            cur.execute("UPDATE contacts SET status = %s WHERE status = %s", (new_name, old_name))

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
    tag_params = []
    if tag:
        tag_join = """
            JOIN contact_tags ct ON ct.contact_id = c.id
            JOIN tags t ON t.id = ct.tag_id AND t.name = %s
        """
        tag_params = [tag]

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
            query_params = tag_params + params
            cur.execute(
                f"SELECT COUNT(*) AS n FROM contacts c {tag_join} {where}",
                query_params,
            )
            total = cur.fetchone()["n"]

            offset = (page - 1) * per_page
            all_params = query_params + [per_page, offset]
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


def create_contact(data: dict) -> int:
    """Insert a new contact. Returns the new contact ID."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO contacts
                   (region, council, name, party, district, term,
                    email, phone_office, phone_mobile, fax, notes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   RETURNING id""",
                (
                    data["region"], data["council"], data["name"],
                    data.get("party"), data.get("district"),
                    data.get("term") or None,
                    data.get("email"), data.get("phone_office"),
                    data.get("phone_mobile"), data.get("fax"),
                    data.get("notes"),
                ),
            )
            return cur.fetchone()["id"]


def update_contact(contact_id: int, data: dict) -> None:
    """Update all editable fields of a contact."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE contacts SET
                    region=%s, council=%s, name=%s, party=%s, district=%s,
                    term=%s, email=%s, phone_office=%s, phone_mobile=%s,
                    fax=%s, notes=%s
                   WHERE id=%s""",
                (
                    data["region"], data["council"], data["name"],
                    data.get("party"), data.get("district"),
                    data.get("term") or None,
                    data.get("email"), data.get("phone_office"),
                    data.get("phone_mobile"), data.get("fax"),
                    data.get("notes"),
                    contact_id,
                ),
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

# ── Gmail sync helpers ─────────────────────────────────────────────────────────

def get_all_contact_emails() -> dict:
    """Returns {email_lower: contact_id} for all contacts with email."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, LOWER(email) AS email FROM contacts WHERE email IS NOT NULL AND email != ''"
            )
            return {r["email"]: r["id"] for r in cur.fetchall()}


def bulk_check_gmail_ids(gmail_ids: list) -> set:
    """Returns set of gmail_message_ids that already exist in outreach_log."""
    if not gmail_ids:
        return set()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT gmail_message_id FROM outreach_log WHERE gmail_message_id = ANY(%s)",
                (gmail_ids,),
            )
            return {r["gmail_message_id"] for r in cur.fetchall()}


def log_outreach_if_new(gmail_message_id: str, **kwargs) -> bool:
    """Insert outreach log only if gmail_message_id doesn't already exist. Returns True if inserted."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO outreach_log
                   (contact_id, channel, direction, subject, body, gmail_message_id, notes, logged_at)
                   SELECT %s, %s, %s, %s, %s, %s, %s, %s
                   WHERE NOT EXISTS (
                     SELECT 1 FROM outreach_log WHERE gmail_message_id = %s
                   )""",
                (
                    kwargs["contact_id"], kwargs["channel"], kwargs["direction"],
                    kwargs.get("subject"), kwargs.get("body"), gmail_message_id,
                    kwargs.get("notes"), kwargs.get("logged_at"),
                    gmail_message_id,
                ),
            )
            return cur.rowcount > 0


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

def list_templates() -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM templates ORDER BY is_default DESC, name")
            return cur.fetchall()


def get_default_template() -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM templates WHERE is_default=TRUE LIMIT 1")
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT * FROM templates ORDER BY id LIMIT 1")
                row = cur.fetchone()
            return row

def save_template(name: str, subject: str, body: str, template_id: int = None) -> int:
    """Create or update a template. Returns template ID."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            if template_id:
                cur.execute(
                    "UPDATE templates SET name=%s, subject=%s, body=%s WHERE id=%s",
                    (name, subject, body, template_id),
                )
                return template_id
            else:
                cur.execute(
                    "INSERT INTO templates (name, subject, body) VALUES (%s,%s,%s) RETURNING id",
                    (name, subject, body),
                )
                return cur.fetchone()["id"]


def delete_template(template_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM templates WHERE id = %s AND is_default = FALSE", (template_id,))


def add_template_attachment(template_id: int, filename: str, mimetype: str, data_b64: str, size_bytes: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO template_attachments (template_id, filename, mimetype, data, size_bytes)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (template_id, filename, mimetype, data_b64, size_bytes),
            )
            return cur.fetchone()["id"]


def get_template_attachments(template_id: int) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, filename, mimetype, size_bytes FROM template_attachments WHERE template_id = %s ORDER BY id",
                (template_id,),
            )
            return cur.fetchall()


def get_template_attachment_data(attachment_id: int) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM template_attachments WHERE id = %s", (attachment_id,))
            return cur.fetchone()


def delete_template_attachment(attachment_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM template_attachments WHERE id = %s", (attachment_id,))



def set_default_template(template_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE templates SET is_default = FALSE WHERE is_default = TRUE")
            cur.execute("UPDATE templates SET is_default = TRUE WHERE id = %s", (template_id,))

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

            cur.execute(
                """SELECT o.direction, o.subject, o.logged_at, o.notes,
                          c.name AS contact_name, c.id AS contact_id
                   FROM outreach_log o
                   JOIN contacts c ON c.id = o.contact_id
                   WHERE o.channel = 'email'
                   ORDER BY o.logged_at DESC NULLS LAST
                   LIMIT 20"""
            )
            recent_emails = cur.fetchall()

    return {
        "overdue": overdue,
        "due_today": due_today,
        "pipeline": pipeline,
        "sends_today": sends,
        "recent_emails": recent_emails,
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


def pipeline_map_data() -> dict:
    """Regional pipeline data for map view. Returns {region: {status: count}}."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT region, status, COUNT(*) AS n
                FROM contacts
                WHERE region IS NOT NULL AND region != ''
                GROUP BY region, status
                ORDER BY region
            """)
            rows = cur.fetchall()

            cur.execute("SELECT COUNT(*) AS n FROM contacts WHERE region IS NOT NULL AND region != ''")
            total = cur.fetchone()["n"]

    result = {}
    for r in rows:
        if r["region"] not in result:
            result[r["region"]] = {"total": 0}
        result[r["region"]][r["status"]] = r["n"]
        result[r["region"]]["total"] += r["n"]

    return {"regions": result, "total": total}


# ── Campaigns ──────────────────────────────────────────────────────────────────

def create_campaign(name: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO campaigns (name) VALUES (%s) RETURNING id", (name,))
            return cur.fetchone()["id"]

def get_campaign(campaign_id: int) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
            return cur.fetchone()

def list_campaigns() -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.*,
                       (SELECT COUNT(*) FROM campaign_steps WHERE campaign_id = c.id) AS step_count,
                       (SELECT COUNT(*) FROM campaign_enrollments WHERE campaign_id = c.id) AS enrolled,
                       (SELECT COUNT(*) FROM campaign_enrollments WHERE campaign_id = c.id AND status = 'replied') AS replied,
                       (SELECT COUNT(*) FROM campaign_enrollments WHERE campaign_id = c.id AND status = 'completed') AS completed_count
                FROM campaigns c ORDER BY c.created_at DESC
            """)
            return cur.fetchall()

def update_campaign_status(campaign_id: int, status: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE campaigns SET status = %s, updated_at = NOW() WHERE id = %s", (status, campaign_id))

def delete_campaign(campaign_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM campaigns WHERE id = %s", (campaign_id,))

def get_campaign_steps(campaign_id: int) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cs.*, t.name AS template_name, t.subject AS template_subject
                FROM campaign_steps cs JOIN templates t ON t.id = cs.template_id
                WHERE cs.campaign_id = %s ORDER BY cs.position
            """, (campaign_id,))
            return cur.fetchall()

def add_campaign_step(campaign_id: int, position: int, template_id: int, delay_days: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO campaign_steps (campaign_id, position, template_id, delay_days) VALUES (%s, %s, %s, %s) RETURNING id",
                (campaign_id, position, template_id, delay_days))
            return cur.fetchone()["id"]

def delete_campaign_step(step_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM campaign_steps WHERE id = %s", (step_id,))

def launch_campaign(campaign_id: int, region: str = None, party: str = None,
                    status_filter: str = None, tag: str = None) -> tuple[int, int]:
    conditions = ["c.email IS NOT NULL", "c.email != ''"]
    params = [campaign_id]
    tag_join = ""
    if region:
        conditions.append("c.region = %s"); params.append(region)
    if party:
        conditions.append("c.party = %s"); params.append(party)
    if status_filter:
        conditions.append("c.status = %s"); params.append(status_filter)
    if tag:
        tag_join = "JOIN contact_tags ct ON ct.contact_id = c.id JOIN tags tg ON tg.id = ct.tag_id AND tg.name = %s"
        params.append(tag)
    where = " AND ".join(conditions)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO campaign_enrollments (campaign_id, contact_id)
                SELECT %s, c.id FROM contacts c {tag_join} WHERE {where}
                ON CONFLICT (campaign_id, contact_id) DO NOTHING
            """, params)
            enrolled = cur.rowcount
            cur.execute(f"SELECT COUNT(*) AS n FROM contacts c {tag_join} WHERE {where}", params[1:])
            total = cur.fetchone()["n"]
    return enrolled, total - enrolled

def campaign_stats(campaign_id: int) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) AS n FROM campaign_enrollments WHERE campaign_id = %s GROUP BY status", (campaign_id,))
            counts = {r["status"]: r["n"] for r in cur.fetchall()}
            cur.execute("SELECT current_step, COUNT(*) AS n FROM campaign_enrollments WHERE campaign_id = %s GROUP BY current_step ORDER BY current_step", (campaign_id,))
            steps = cur.fetchall()
            total = sum(counts.values())
            return {
                "total": total, "active": counts.get("active", 0),
                "replied": counts.get("replied", 0), "completed": counts.get("completed", 0),
                "paused": counts.get("paused", 0),
                "reply_rate": round(counts.get("replied", 0) / total * 100) if total else 0,
                "steps": steps,
            }

def campaign_enrollments_list(campaign_id: int, limit: int = 100) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ce.*, c.name AS contact_name, c.council, c.email, c.status AS contact_status
                FROM campaign_enrollments ce JOIN contacts c ON c.id = ce.contact_id
                WHERE ce.campaign_id = %s ORDER BY ce.status, ce.enrolled_at DESC LIMIT %s
            """, (campaign_id, limit))
            return cur.fetchall()

def get_due_campaign_steps(limit: int = 100) -> list:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ce.id AS enrollment_id, ce.campaign_id, ce.contact_id,
                       ce.current_step, ce.enrolled_at, ce.last_step_sent_at, ce.retry_count,
                       cs.template_id, cs.delay_days, cs.position AS step_position,
                       cam.name AS campaign_name
                FROM campaign_enrollments ce
                JOIN campaign_steps cs ON cs.campaign_id = ce.campaign_id AND cs.position = ce.current_step
                JOIN campaigns cam ON cam.id = ce.campaign_id
                WHERE ce.status = 'active' AND cam.status = 'active'
                  AND (
                    (ce.last_step_sent_at IS NULL AND cs.position = 0)
                    OR (ce.last_step_sent_at IS NOT NULL
                        AND ce.last_step_sent_at + (cs.delay_days || ' days')::interval <= NOW())
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM outreach_log ol
                    WHERE ol.contact_id = ce.contact_id AND ol.direction = 'inbound'
                      AND ol.channel = 'email'
                      AND ol.logged_at > COALESCE(ce.last_step_sent_at, ce.enrolled_at)
                  )
                LIMIT %s FOR UPDATE OF ce SKIP LOCKED
            """, (limit,))
            return cur.fetchall()

def advance_enrollment(enrollment_id: int, campaign_id: int) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(position) AS max_pos FROM campaign_steps WHERE campaign_id = %s", (campaign_id,))
            max_pos = cur.fetchone()["max_pos"] or 0
            cur.execute("SELECT current_step FROM campaign_enrollments WHERE id = %s", (enrollment_id,))
            row = cur.fetchone()
            if not row:
                return
            next_step = row["current_step"] + 1
            if next_step > max_pos:
                cur.execute("UPDATE campaign_enrollments SET status = 'completed', current_step = %s, last_step_sent_at = NOW() WHERE id = %s", (next_step, enrollment_id))
            else:
                cur.execute("UPDATE campaign_enrollments SET current_step = %s, last_step_sent_at = NOW(), retry_count = 0 WHERE id = %s", (next_step, enrollment_id))

def mark_enrollment_retry(enrollment_id: int) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE campaign_enrollments SET retry_count = retry_count + 1 WHERE id = %s RETURNING retry_count", (enrollment_id,))
            row = cur.fetchone()
            if row and row["retry_count"] >= 3:
                cur.execute("UPDATE campaign_enrollments SET status = 'paused' WHERE id = %s", (enrollment_id,))
                return True
            return False

def mark_contact_enrollments_replied(contact_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE campaign_enrollments SET status = 'replied' WHERE contact_id = %s AND status = 'active'", (contact_id,))
            return cur.rowcount

def clone_campaign(campaign_id: int) -> int | None:
    campaign = get_campaign(campaign_id)
    if not campaign:
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO campaigns (name, status) VALUES (%s, 'draft') RETURNING id", (f"{campaign['name']} (복사)",))
            new_id = cur.fetchone()["id"]
            cur.execute("""
                INSERT INTO campaign_steps (campaign_id, position, template_id, delay_days)
                SELECT %s, position, template_id, delay_days FROM campaign_steps WHERE campaign_id = %s ORDER BY position
            """, (new_id, campaign_id))
    return new_id

def fill_campaign_template_vars(text: str, contact: dict, campaign_context: dict = None) -> str:
    result = fill_template_vars(text, contact)
    if campaign_context:
        result = result.replace("{step_number}", str(campaign_context.get("step_number", "")))
        result = result.replace("{campaign_name}", str(campaign_context.get("campaign_name", "")))
        result = result.replace("{days_since_first_email}", str(campaign_context.get("days_since_first_email", "")))
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
