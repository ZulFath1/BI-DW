# sql/

| Folder/berkas | Fungsi |
|---|---|
| `init/00_init.sh` | Dijalankan otomatis saat volume PostgreSQL pertama kali dibuat: database DWH & Airflow, schema `stg`, `curated`, `dq`, `dw`, `mart`, role `etl_user`, `dashboard_ro` (read-only), `airflow_user` |
| `ddl/01_dq.sql` | `dq.batch_log`, `dq.dq_log`, `dq.quarantine`, `dq.ingest_manifest` |
| `ddl/02_stg.sql` | Tabel staging (nilai asli + nomor baris sumber) |
| `ddl/03_dw.sql` | Galaxy schema: 7 dimensi, 5 fakta, PK/FK/surrogate key |
| `ddl/04_dw_seed.sql` | Isi dimensi konstanta: `dim_waktu` (48), `dim_kelompok_sasaran` (5), `dim_status_data` (4), `dim_alat` (11), `dim_kelompok_umur` (16) |
| `ddl/05_dw_view.sql` | View `dw.v_rasio_stunting_balita` (T19) |
| `validasi/01_staging.sql` | Query validasi hasil staging (jumlah baris, rekonsiliasi, privasi) |
| `validasi/02_uji_view_rasio.sql` | Uji logika view rasio dengan data staging di dalam transaksi yang di-ROLLBACK |

Berkas `ddl/` dijalankan berurutan oleh task `init_schema` (`etl/schema.py`) dalam satu
transaksi. Semuanya idempotent (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, `CREATE OR REPLACE VIEW`)
dan tidak pernah menghapus tabel atau data.

## Galaxy schema (dw)

Matriks bus (X = FK langsung, (X) = melalui `dim_posyandu.kelurahan_key`):

| Fakta | posyandu | kelurahan | waktu | kelompok sasaran | status data | alat | kelompok umur |
|---|---|---|---|---|---|---|---|
| `fact_kunjungan_bulanan` | X | (X) | X | X | X | | |
| `fact_alat_snapshot` | X | (X) | X | | | X | |
| `fact_kader_snapshot` | X | (X) | X | | | | |
| `fact_penduduk_kelurahan` | | X | X | | | | X |
| `fact_stunting_kelurahan` | | X | X | | | | |

`dim_kelurahan` adalah dimensi bersama yang menghubungkan data posyandu (internal) dengan data
penduduk dan stunting (eksternal). `dim_kelompok_umur` memetakan kelompok umur ke `dim_kelompok_sasaran`.

Grain: lihat komentar tabel di `ddl/03_dw.sql` (`\d+ dw.<tabel>` di psql).

Menjalankan query validasi:
```bash
docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/01_staging.sql
```
