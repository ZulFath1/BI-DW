# Perubahan dari Rancangan UTS (BI-DW-001 Rev. 1.3)

Dokumen ini mencatat setiap perbedaan antara rancangan UTS dan implementasi UAS,
beserta alasan dan sumbernya. Keputusan desain lain dari UTS (aturan T01–T19,
quality check Q01–Q09, galaxy schema) **tetap dipakai** kecuali tercatat di sini.

| No | Komponen | Rancangan UTS | Implementasi UAS | Tanggal |
|---|---|---|---|---|
| P01 | Dashboard | Metabase | Streamlit | 15 Nov 2026 |
| P02 | Data lake | MinIO (image community) | PGSTY Silo (fork MinIO oleh Pigsty) | 15 Nov 2026 |
| P03 | Orkestrasi | Apache Airflow (versi belum ditetapkan) | Apache Airflow 3.3.2, LocalExecutor | 15 Nov 2026 |

## P01 — Metabase → Streamlit

- **Alasan:** instruksi UAS mewajibkan *source code dashboard* ada di repository.
  Dashboard Metabase tersimpan di database aplikasi Metabase, bukan sebagai kode,
  sehingga sulit dibuktikan dan direproduksi dari repository.
- **Dampak:** dashboard ditulis sebagai `dashboard/app.py` (Python), berjalan di
  container sendiri, dan membaca schema `dw` dan `mart` dengan user read-only
  `dashboard_ro`.
- **Kelemahan yang diakui:** Streamlit butuh lebih banyak kode dibanding membuat
  dashboard lewat antarmuka Metabase; fitur seperti penjadwalan laporan email tidak ada.

## P02 — MinIO → PGSTY Silo

- **Alasan:** image community MinIO tidak lagi tersedia. Diperiksa 15 Nov 2026:
  `minio/minio` di Docker Hub menolak akses dan `quay.io/minio/minio` tidak ada.
  Menurut artikel Pinggy (sumber pihak ketiga,
  <https://pinggy.io/amp/blog/minio_archived_self_hosted_s3_alternatives/>), MinIO
  community berhenti merilis image pada Okt 2025 dan repositorinya diarsipkan 25 Apr 2026.
- **Pengganti:** `pgsty/silo:RELEASE.2026-11-12T00-00-00Z`, di-pin ke digest
  `sha256:c20d112390b9a9ce8c11b8af95ad35bcaa62f4759663c09c936cd8d2b511f329`.
  Fork `minio/minio` (AGPL-3.0) yang dipelihara Pigsty; kompatibel API S3 dan
  variabel `MINIO_*`.
- **Dampak:** kode data lake memakai boto3 (API S3 standar), bukan klien khusus MinIO,
  sehingga bisa dipindah ke object storage S3 lain tanpa mengubah kode.
- **Risiko:** fork komunitas bergantung pada satu pemelihara; dimitigasi dengan pin
  digest dan pemakaian API S3 standar.

## P03 — Apache Airflow 3.3.2

- **Alasan:** menurut endoflife.date/apache-airflow, Airflow 2 berakhir 22 Apr 2026
  dan 3.1 berakhir 7 Apr 2026.
- **Diakui jujur:** cabang 3.3 juga sudah berakhir dukungannya pada 7 Nov 2026 setelah
  3.4.0 rilis (sumber yang sama). Tetap dipakai karena patch 3.3.2 sudah matang,
  sistem hanya berjalan lokal (port di 127.0.0.1), dan 3.4.0 masih terlalu baru untuk
  tenggat ±4 minggu.
- **Dampak:** Airflow 3 memisahkan `api-server`, `scheduler`, dan `dag-processor`
  menjadi layanan terpisah; login memakai Simple Auth Manager.
- **Pengembangan lanjutan:** upgrade ke 3.4.x.
