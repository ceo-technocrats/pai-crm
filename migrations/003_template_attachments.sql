CREATE TABLE IF NOT EXISTS template_attachments (
  id          SERIAL PRIMARY KEY,
  template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
  filename    TEXT NOT NULL,
  mimetype    TEXT NOT NULL,
  data        TEXT NOT NULL,  -- base64 encoded file content
  size_bytes  INTEGER NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ta_template ON template_attachments(template_id);
