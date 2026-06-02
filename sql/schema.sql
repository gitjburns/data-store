CREATE TABLE IF NOT EXISTS documents (
  document_id TEXT PRIMARY KEY,
  source_path TEXT NOT NULL UNIQUE,
  source_sha256 TEXT,
  markdown_path TEXT NOT NULL,
  markdown_sha256 TEXT,
  pdf_backend TEXT NOT NULL,
  ocr_mode TEXT NOT NULL,
  page_batch_size INTEGER,
  units_ingested INTEGER NOT NULL,
  status TEXT NOT NULL,
  diagnostics_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_source_path
ON documents(source_path);

CREATE TABLE IF NOT EXISTS units (
  unit_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  source_path TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  heading_path_json TEXT NOT NULL,
  page_numbers_json TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  content TEXT NOT NULL,
  content_chars INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_units_document_sequence
ON units(document_id, sequence);

CREATE TABLE IF NOT EXISTS dense_vectors (
  unit_id TEXT PRIMARY KEY REFERENCES units(unit_id) ON DELETE CASCADE,
  dimension INTEGER NOT NULL,
  vector_blob BLOB NOT NULL,
  vector_norm REAL NOT NULL,
  model_path TEXT NOT NULL,
  model_dimension INTEGER NOT NULL,
  pooling TEXT NOT NULL,
  format TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS colbert_document_vectors (
  unit_id TEXT PRIMARY KEY REFERENCES units(unit_id) ON DELETE CASCADE,
  token_count INTEGER NOT NULL,
  dimension INTEGER NOT NULL,
  vector_blob BLOB NOT NULL,
  model_path TEXT NOT NULL,
  model_dimension INTEGER NOT NULL,
  format TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS units_fts
USING fts5(content, content='units', content_rowid='rowid');

PRAGMA user_version = 2;
