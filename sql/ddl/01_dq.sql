-- =====================================================================
-- 01_dq.sql — schema dq: log batch, log kualitas, karantina, manifest ingestion
-- Idempotent (IF NOT EXISTS): aman dijalankan ulang oleh task init_schema.
-- =====================================================================

-- Status setiap task pipeline per batch (monitoring sukses/gagal).
CREATE TABLE IF NOT EXISTS dq.batch_log (
    log_id         bigserial   PRIMARY KEY,
    batch_id       text        NOT NULL,             -- run_id Airflow atau manual__YYYYMMDDTHHMMSS
    task_name      text        NOT NULL,
    tahap          text        NOT NULL,             -- ingestion | staging | transform | quality | load | ml
    status         text        NOT NULL CHECK (status IN ('BERJALAN', 'SUKSES', 'GAGAL', 'DILEWATI')),
    waktu_mulai    timestamptz NOT NULL DEFAULT now(),
    waktu_selesai  timestamptz,
    jumlah_baris   bigint,
    pesan          text,                             -- ringkasan hasil atau jenis+pesan error
    percobaan_ke   integer                           -- try_number Airflow (retry)
);
CREATE INDEX IF NOT EXISTS ix_batch_log_batch ON dq.batch_log (batch_id);

-- Temuan kualitas data per aturan (T.., Q.., STG..). Tidak boleh memuat nama/alamat kader.
CREATE TABLE IF NOT EXISTS dq.dq_log (
    dq_id          bigserial   PRIMARY KEY,
    batch_id       text        NOT NULL,
    tahap          text        NOT NULL,
    kode_aturan    text        NOT NULL,
    tingkat        text        NOT NULL CHECK (tingkat IN ('INFO', 'PERINGATAN', 'KRITIS')),
    objek          text,                             -- sheet/tabel/sumber yang diperiksa
    baris_sumber   integer,                          -- nomor baris di berkas sumber (lineage)
    kolom          text,
    nilai          text,
    keterangan     text,
    dicatat_pada   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_log_batch ON dq.dq_log (batch_id, kode_aturan);

-- Data/berkas yang ditolak (korup, gagal validasi) disimpan di sini, tidak masuk DWH.
CREATE TABLE IF NOT EXISTS dq.quarantine (
    q_id           bigserial   PRIMARY KEY,
    batch_id       text        NOT NULL,
    tahap          text        NOT NULL,
    sumber         text        NOT NULL,
    objek          text,
    baris_sumber   integer,
    alasan         text        NOT NULL,
    data           jsonb,
    dicatat_pada   timestamptz NOT NULL DEFAULT now()
);

-- Lineage raw zone: berkas apa, dari mana, kapan, checksum, dan letaknya di data lake.
CREATE TABLE IF NOT EXISTS dq.ingest_manifest (
    manifest_id    bigserial   PRIMARY KEY,
    batch_id       text        NOT NULL,
    sumber         text        NOT NULL,             -- kode sumber, mis. internal_laporan_posyandu
    cara_ambil     text        NOT NULL CHECK (cara_ambil IN ('lokal', 'web', 'fallback_manual')),
    lokasi_asal    text        NOT NULL,             -- nama berkas lokal atau URL
    bucket         text        NOT NULL,
    object_key     text        NOT NULL,
    sha256         char(64)    NOT NULL,
    ukuran_byte    bigint      NOT NULL,
    content_type   text,
    http_status    integer,
    status_unggah  text        NOT NULL CHECK (status_unggah IN ('diunggah', 'sudah_ada')),
    waktu_ambil    timestamptz NOT NULL,
    UNIQUE (batch_id, sumber)
);
