-- 002: Gmail sync indexes
-- Partial unique index on gmail_message_id for dedup (NULLs allowed for manual logs)
CREATE UNIQUE INDEX IF NOT EXISTS idx_outreach_gmail_msg_id
  ON outreach_log(gmail_message_id)
  WHERE gmail_message_id IS NOT NULL;

-- Index on contacts.email for fast lookup during sync
CREATE INDEX IF NOT EXISTS idx_contacts_email
  ON contacts(email)
  WHERE email IS NOT NULL;
