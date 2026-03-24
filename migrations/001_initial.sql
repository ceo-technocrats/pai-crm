-- PAI CRM — Initial Schema
-- Apply via Supabase SQL editor (NOT at app startup)
-- Idempotent: safe to re-run

-- ── Contacts ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contacts (
  id            SERIAL PRIMARY KEY,
  region        TEXT NOT NULL,
  council       TEXT NOT NULL,
  name          TEXT NOT NULL,
  party         TEXT,
  district      TEXT,
  term          INTEGER,
  email         TEXT,
  phone_office  TEXT,
  phone_mobile  TEXT,
  fax           TEXT,
  status        TEXT NOT NULL DEFAULT '미연락',
  close_outcome TEXT,           -- NULL | 'won' | 'lost'
  follow_up_date DATE,
  notes         TEXT,
  docid         TEXT UNIQUE,    -- nanet docid, upsert key
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS contacts_updated_at ON contacts;
CREATE TRIGGER contacts_updated_at
  BEFORE UPDATE ON contacts
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── Email templates ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS templates (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  subject    TEXT NOT NULL,
  body       TEXT NOT NULL,
  is_default BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default template (only if table is empty)
INSERT INTO templates (name, subject, body, is_default)
SELECT
  '기본 첫 연락',
  '[PAI] {의원명} 의원님께 — 의정활동 AI 지원 서비스 소개',
  '{의원명} 의원님,

안녕하십니까. AI 기반 의정활동 지원 서비스 PAI를 운영하는 테크노크라츠입니다.

PAI는 {의회명} 의원님들의 조례 검색, 회의록 분석, 문서 자동 생성을 AI로 지원하는
서비스입니다. 현재 여러 의회에서 시범 운영 중입니다.

30분 온라인 데모를 통해 {선거구} 선거구의 의정활동에 어떻게 도움이 될 수 있는지
직접 보여드리고 싶습니다.

편하신 시간을 알려주시면 감사하겠습니다.

테크노크라츠 드림',
  TRUE
WHERE NOT EXISTS (SELECT 1 FROM templates);

-- ── Outreach log ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outreach_log (
  id               SERIAL PRIMARY KEY,
  contact_id       INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  channel          TEXT NOT NULL,  -- 'email' | 'phone' | 'meeting' | 'kakao' | 'other'
  direction        TEXT NOT NULL,  -- 'outbound' | 'inbound'
  subject          TEXT,
  body             TEXT,
  gmail_message_id TEXT,
  notes            TEXT,
  logged_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ── Tags ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tags (
  id   SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS contact_tags (
  contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  PRIMARY KEY (contact_id, tag_id)
);

-- ── Bulk send queue ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS send_queue (
  id          SERIAL PRIMARY KEY,
  contact_id  INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  job_id      UUID NOT NULL,
  template_id INTEGER REFERENCES templates(id),
  status      TEXT NOT NULL DEFAULT 'pending',  -- 'pending'|'sent'|'failed'|'skipped'
  error       TEXT,
  queued_at   TIMESTAMPTZ DEFAULT NOW(),
  sent_at     TIMESTAMPTZ
);

-- ── App settings (key/value) ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS settings (
  key        TEXT PRIMARY KEY,
  value      TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_contacts_status     ON contacts(status);
CREATE INDEX IF NOT EXISTS idx_contacts_region     ON contacts(region);
CREATE INDEX IF NOT EXISTS idx_contacts_follow_up  ON contacts(follow_up_date);
CREATE INDEX IF NOT EXISTS idx_outreach_contact    ON outreach_log(contact_id);
CREATE INDEX IF NOT EXISTS idx_outreach_logged_at  ON outreach_log(logged_at);
CREATE INDEX IF NOT EXISTS idx_contact_tags_tag    ON contact_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_send_queue_job      ON send_queue(job_id, status);
CREATE INDEX IF NOT EXISTS idx_send_queue_pending  ON send_queue(status) WHERE status = 'pending';
