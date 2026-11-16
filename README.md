# Data Warehouse & Analitik Kunjungan Posyandu — Kota Banjarmasin

Nama : Muhammad Dzul Fathi Ahyan
NIM : 2410817210011

Proyek UAS Kecerdasan Bisnis (bidang Big Data). Membangun pipeline data dari laporan
kunjungan posyandu dan data terbuka kependudukan/stunting menjadi data lake, data
warehouse (galaxy schema), data mart, model clustering, dan dashboard.

> **Status (16 Nov 2026):** tahap 1–10 selesai — infrastruktur; ingestion ke data lake; staging;
> schema DWH; transform T01–T19 ke `curated`; quality gate Q01–Q09; DWH terisi (106.020 baris
> fakta kunjungan); data mart (11 tabel); clustering posyandu (45 model dibandingkan, daftar prioritas per
> posyandu); dashboard Streamlit dengan filter tahun/kecamatan/puskesmas/cluster; DAG `posyandu_pipeline`
> (16 task, tanggal 10 setiap bulan) dan `posyandu_backup` (harian); uji kegagalan, uji end-to-end dari
> kondisi bersih, dan uji pemulihan bencana. Belum: laporan PDF & PPT.
> Rincian per tahap: [docs/laporan_tahap/](docs/laporan_tahap/).

## Arsitektur

```
Sumber (Excel, PDF, web)
   │  ingestion (Python)
   ▼
Data lake / raw zone ── Silo (object storage kompatibel S3, fork MinIO oleh Pigsty)
   │
   ▼
PostgreSQL 16: stg → curated → dw (galaxy schema) → mart      dq: batch_log, dq_log, quarantine
   │                                    ▲
   │        orkestrasi: Apache Airflow 3.3.2 (LocalExecutor)
   ▼
ML (scikit-learn) → mart  ──►  Dashboard Streamlit (user read-only)
```

| Layanan | Image (di-pin ke digest) | Alamat |
|---|---|---|
| PostgreSQL | `postgres:16.13` | `127.0.0.1:5432` |
| Data lake — S3 API | `pgsty/silo:RELEASE.2026-11-12T00-00-00Z` | `http://127.0.0.1:9000` |
| Data lake — konsol web | (image yang sama) | `http://127.0.0.1:9001` |
| Airflow (UI & API) | `apache/airflow:3.3.2-python3.12` + `docker/airflow/Dockerfile` | `http://127.0.0.1:8081` |
| Dashboard | `python:3.12-slim` + `dashboard/Dockerfile` (Streamlit) | `http://127.0.0.1:8501` |

Semua port hanya di-bind ke `127.0.0.1` (tidak terbuka ke jaringan).

### Catatan pemilihan versi

- **Silo, bukan image MinIO resmi:** image community MinIO tidak lagi tersedia
  (per 15 Nov 2026 `minio/minio` di Docker Hub menolak akses). Silo adalah fork MinIO
  yang dipelihara Pigsty (AGPL-3.0, <https://github.com/pgsty/silo>) dan kompatibel
  dengan API S3. Bucket dibuat dengan boto3 (`ingestion/lake_setup.py`).
- **Airflow 3.3.2:** cabang 3.3 sudah berakhir dukungannya pada 7 Nov 2026
  (endoflife.date). Tetap dipakai karena patch matang, sistem hanya berjalan lokal
  untuk demo, dan 3.4.0 masih terlalu baru. Upgrade ke 3.4.x adalah pengembangan lanjutan.
- **Python 3.12** di semua container (didukung Airflow 3.3.2 dan semua pustaka).
- **Streamlit** dipakai untuk dashboard agar source code dashboard ada di repository.

## Struktur repository

```
├── docker-compose.yml      # semua layanan
├── .env.example            # contoh konfigurasi (salin ke .env)
├── requirements.txt        # pustaka pipeline (dipasang di image Airflow)
├── docker/airflow/         # Dockerfile Airflow
├── common/                 # modul bersama: koneksi DB & data lake, batch_log/dq_log, pseudonim
├── ingestion/              # extract & unggah ke data lake
├── etl/                    # staging, transform, load
├── quality/                # quality gate Q01–Q09
├── dags/                   # DAG Airflow
├── sql/                    # init database, schema DWH, query validasi
├── ml/                     # feature engineering, training, evaluasi
├── dashboard/              # aplikasi Streamlit
├── backup/                 # backup & restore
├── tests/                  # uji error handling, uji E2E bersih, uji pemulihan, ukur performa
├── data/                   # TIDAK di-commit (lihat data/README.md)
└── docs/                   # keputusan, laporan per tahap, bukti eksekusi
```

## Menjalankan

Prasyarat: Docker Desktop (WSL 2) dengan RAM minimal ±6 GB untuk Docker.

1. Letakkan berkas sumber sesuai [data/README.md](data/README.md).
2. Buat konfigurasi: `cp .env.example .env`, lalu isi semua nilai kosong
   (petunjuk pembuat nilai acak ada di dalam berkas).
3. Nyalakan semua layanan:
   ```bash
   docker compose up -d --build
   docker compose ps
   ```
   Tunggu sampai semua layanan berstatus `healthy`. `airflow-init` dan
   `datalake-init` adalah layanan sekali jalan dan berakhir dengan `Exited (0)`.
4. Buka:
   - Airflow: <http://127.0.0.1:8081> (login: `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`)
   - Konsol data lake: <http://127.0.0.1:9001> (login: `LAKE_ROOT_USER` / `LAKE_ROOT_PASSWORD`)
   - Dashboard: <http://127.0.0.1:8501>

Mematikan layanan (data tetap tersimpan di volume): `docker compose down`.

### Menjalankan pipeline

Cara utama: DAG Airflow **`posyandu_pipeline`** (UI <http://127.0.0.1:8081>). DAG berjalan otomatis
tanggal 10 setiap bulan pukul 02.00 WITA; untuk menjalankan sekarang:

```bash
docker compose exec airflow-scheduler airflow dags unpause posyandu_pipeline   # sekali saja
docker compose exec airflow-scheduler airflow dags trigger posyandu_pipeline
```

Alur DAG: `init_schema → ingest_excel/ingest_pdf/ingest_web → load_staging (5 task) → transform →
quality_gate → load_dw → build_mart → train_ml → refresh_dashboard` (atau `quarantine` bila pemeriksaan
kritis gagal). Lihat [dags/README.md](dags/README.md).

Uji kegagalan di DAG run nyata (bukti di `docs/bukti/22–24_*.txt`):

```bash
# data buruk: salinan uji (bukan data asli) -> quality gate -> karantina, DWH tidak berubah
docker compose exec airflow-scheduler python -m tests.buat_data_uji
docker compose exec airflow-scheduler airflow dags trigger posyandu_pipeline --conf '{"skenario_uji": "data_buruk"}'
# status task satu run (state, percobaan ke-)
docker compose exec airflow-scheduler python -m tests.status_run <run_id>
```

Cara alternatif (pengembangan/uji), fungsi yang sama lewat CLI:

```bash
docker compose exec airflow-scheduler python -m etl.cli semua          # schema → ... → refresh_dashboard
# validasi hasil staging, DWH, dan mart (dibandingkan dengan angka acuan)
docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/01_staging.sql
docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/03_dwh.sql
docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/04_mart.sql
# uji error handling: DB terputus, web gagal, data korup, struktur berubah (E1–E5)
docker compose exec airflow-scheduler python -m tests.uji_error_handling
# uji quality gate: rollback Q06, quarantine Q02, idempotensi load (E6–E8)
docker compose exec airflow-scheduler python -m tests.uji_quality_gate
```

Status setiap task tercatat di `dq.batch_log`; temuan kualitas di `dq.dq_log`; berkas/data yang
ditolak di `dq.quarantine`; lineage berkas mentah di `dq.ingest_manifest`; hasil Q01–Q09 di
`dq.quality_result`. Callback Airflow (retry/gagal) tercatat di `dq.batch_log` dengan tahap `orkestrasi`;
bila database DWH sedang tidak tersedia, catatan ditunda di outbox dan dikirim run berikutnya.

### Data mart, machine learning, dashboard

- **Data mart** (schema `mart`, dibangun `etl/build_mart.py`): KPI cakupan per kecamatan × bulan × kelompok,
  kelengkapan laporan, profil per posyandu (fitur ML), KPI kelurahan (stunting), ringkasan kualitas, hasil
  quality gate terakhir. KPI cakupan = Σ cakupan ÷ Σ sasaran, baris Terlapor, bukan A1, sasaran > 0 (K38).
- **ML** (`ml/clustering.py`, lihat [ml/README.md](ml/README.md)): baseline aturan → K-Means & Agglomerative
  K=2–8 pada tiga set fitur → model terpilih (`kmeans_v2_internal_k6`). Hasil di `mart.hasil_cluster`,
  `mart.cluster_profil`, `mart.ml_evaluasi`.
- **Dashboard** (`dashboard/app.py`, <http://127.0.0.1:8501>): lima tab — ringkasan & insight, tren cakupan,
  posyandu prioritas (hasil ML), stunting kelurahan, kualitas data — dengan filter tahun, kecamatan,
  puskesmas, cluster, kelompok. Insight berpola Data → Temuan → Interpretasi → Impact → Rekomendasi,
  dihitung ulang sesuai filter. Hanya membaca `mart`, dengan user read-only.
- **Laporan evaluasi ML:** `docker compose exec airflow-scheduler python -m ml.laporan`.

### Backup, pemulihan, uji end-to-end

```bash
docker compose exec airflow-scheduler airflow dags trigger posyandu_backup   # juga terjadwal 01.00 WITA
bash tests/uji_e2e_bersih.sh     # stack terpisah dari volume kosong -> run terjadwal -> validasi identik
bash tests/uji_pemulihan.sh      # stack kosong tanpa berkas sumber -> restore -> pipeline dari raw zone
docker compose exec airflow-scheduler python -m tests.ukur_performa          # durasi task & ukuran layer
```

Rincian kebijakan backup & langkah pemulihan: [backup/README.md](backup/README.md).

### Screenshot bukti

Screenshot diambil di container Playwright (tidak ada instalasi global):

```bash
docker compose --profile bukti run --rm screenshot airflow     # juga: lake, dashboard
```

### Database

`sql/init/00_init.sh` dijalankan otomatis **hanya saat volume PostgreSQL pertama kali dibuat**:

| Database | Pemilik | Isi |
|---|---|---|
| `posyandu_dw` | `etl_user` | schema `stg`, `curated`, `dq`, `dw`, `mart` |
| `airflow` | `airflow_user` | metadata Airflow |

`dashboard_ro` hanya boleh `SELECT` pada schema `dw` dan `mart`, dan setiap
transaksinya read-only.

## Privasi data

Dataset (termasuk data pribadi kader) tidak pernah di-commit. Nama kader
dipseudonimkan dengan kunci di `.env`, alamat kader tidak dimuat, dan screenshot
bukti di `docs/bukti/` tidak memuat data pribadi.

Berkas turunan dataset juga diperlakukan sebagai **data privat** dan tidak di-commit:
`etl/pemetaan_posyandu_manual.csv` (pemetaan manual nama posyandu Antrop/Kader, berisi nama posyandu
persis seperti di dataset) dan salinan uji di `data/uji/`. Pipeline tetap berjalan tanpa berkas pemetaan
(hasilnya sedikit berbeda: fakta alat 4.890, bukan 4.900). Rinciannya di [data/README.md](data/README.md).
