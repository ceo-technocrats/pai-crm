-- 003: Custom pipeline stages
CREATE TABLE IF NOT EXISTS pipeline_stages (
  id       SERIAL PRIMARY KEY,
  name     TEXT NOT NULL UNIQUE,
  position INTEGER NOT NULL DEFAULT 0
);

INSERT INTO pipeline_stages (name, position) VALUES
  ('미연락', 0),
  ('연락함', 1),
  ('답변옴', 2),
  ('데모예약', 3),
  ('클로즈', 4)
ON CONFLICT (name) DO NOTHING;
