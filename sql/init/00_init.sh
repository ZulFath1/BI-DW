#!/bin/bash
# =====================================================================
# Inisialisasi PostgreSQL (dijalankan otomatis oleh image postgres HANYA saat
# volume pg_data masih kosong / pertama kali dibuat).
#
# Membuat:
#   - role airflow_user  + database metadata Airflow
#   - role etl_user      + database DWH dengan schema stg, curated, dq, dw, mart
#   - role dashboard_ro  : hanya SELECT pada schema dw dan mart (untuk Streamlit)
#
# Ditulis sebagai .sh (bukan .sql) agar nama & kata sandi role dibaca dari
# environment (.env), bukan ditulis di kode.
# =====================================================================
# Tanpa "-u": bila berkas ini di-source (bukan dieksekusi) oleh entrypoint postgres,
# "-u" bisa ikut memengaruhi skrip entrypoint.
set -eo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v dw_db="$DW_DB" \
  -v etl_user="$ETL_USER" -v etl_pass="$ETL_PASSWORD" \
  -v dash_user="$DASHBOARD_USER" -v dash_pass="$DASHBOARD_PASSWORD" \
  -v af_db="$AIRFLOW_DB" -v af_user="$AIRFLOW_DB_USER" -v af_pass="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
-- ---------- Role ----------
CREATE ROLE :"af_user"   LOGIN PASSWORD :'af_pass';
CREATE ROLE :"etl_user"  LOGIN PASSWORD :'etl_pass';
CREATE ROLE :"dash_user" LOGIN PASSWORD :'dash_pass';
-- Lapis pengaman tambahan: setiap transaksi dashboard bersifat read-only
ALTER ROLE :"dash_user" SET default_transaction_read_only = on;

-- ---------- Database ----------
CREATE DATABASE :"af_db" OWNER :"af_user";
CREATE DATABASE :"dw_db" OWNER :"etl_user";

-- Hanya role yang berhak yang boleh terhubung
REVOKE ALL ON DATABASE :"af_db" FROM PUBLIC;
REVOKE ALL ON DATABASE :"dw_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dw_db" TO :"dash_user";
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DW_DB" \
  -v etl_user="$ETL_USER" -v dash_user="$DASHBOARD_USER" <<'EOSQL'
-- ---------- Schema DWH (pemilik: etl_user) ----------
CREATE SCHEMA stg     AUTHORIZATION :"etl_user";   -- staging: nilai asli + nomor baris sumber
CREATE SCHEMA curated AUTHORIZATION :"etl_user";   -- hasil transform yang lolos quality gate
CREATE SCHEMA dq      AUTHORIZATION :"etl_user";   -- batch_log, dq_log, quarantine
CREATE SCHEMA dw      AUTHORIZATION :"etl_user";   -- galaxy schema (fakta & dimensi)
CREATE SCHEMA mart    AUTHORIZATION :"etl_user";   -- data mart untuk dashboard & ML

-- Schema public tidak dipakai
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ---------- Hak baca dashboard: hanya dw dan mart ----------
GRANT USAGE ON SCHEMA dw, mart TO :"dash_user";
GRANT SELECT ON ALL TABLES IN SCHEMA dw, mart TO :"dash_user";
-- Tabel/view yang nanti dibuat etl_user di dw & mart otomatis bisa dibaca dashboard
ALTER DEFAULT PRIVILEGES FOR ROLE :"etl_user" IN SCHEMA dw, mart
  GRANT SELECT ON TABLES TO :"dash_user";
EOSQL

echo "Inisialisasi PostgreSQL selesai: database, schema, dan role dibuat."
