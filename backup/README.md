# backup/ — backup & disaster recovery

| Berkas | Isi |
|---|---|
| `backup.py` | `backup_database` (pg_dump DWH + metadata Airflow + manifest), `mirror_lake` (salinan raw zone), `retensi` |
| `restore.py` | `python -m backup.restore db | lake | verifikasi | semua [--stempel ...]` |
| `../dags/posyandu_backup.py` | DAG harian `backup_database → mirror_lake → retensi` |
| `../tests/uji_pemulihan.sh` | uji pemulihan bencana otomatis (stack terpisah, berkas sumber dihapus) |
| `output/` | **hasil backup — tidak di-commit** (berisi data privat) |

## Kebijakan

| Aspek | Isi |
|---|---|
| Yang di-backup | (1) database DWH `posyandu_dw` (stg, curated, dq, dw, mart); (2) database metadata Airflow (riwayat run, status DAG); (3) raw zone data lake (bucket `raw`, berkas sumber asli + metadata sha256). Kode & konfigurasi ada di Git; `.env` disimpan pemilik terpisah |
| Cara | `pg_dump -Fc` klien PostgreSQL 16 (sama dengan server), sebagai pemilik database; mirror objek dengan boto3, SHA-256 dicek tiap unduhan |
| Lokasi | `backup/output/db/<stempel>/` (dump + `manifest.json`) dan `backup/output/lake/raw/` di disk laptop (di luar volume Docker); salinan dump di bucket `backup` data lake |
| Frekuensi | harian 01.00 WITA (DAG `posyandu_backup`), di luar jendela pipeline (02.00 tanggal 10). Raw zone praktis berubah bulanan, mirror inkremental hanya menyalin objek baru |
| Retensi | 7 dump harian terbaru + dump terbaru dari 4 minggu ISO terakhir (lokal & bucket). Mirror raw zone tidak dirotasi (append-only) |
| Verifikasi | saat backup: `pg_restore --list`, SHA-256 berkas; saat restore: SHA-256 dump, jumlah baris 44 tabel, md5 isi 23 tabel dw/mart, SHA-256 17 objek raw zone |
| Pemantauan | status tiap langkah di `dq.batch_log` (tahap `backup`) dan UI Airflow; retry 2×, callback gagal ke `dq.dq_log` |

**Batasan (jujur):** semua salinan masih di satu laptop (volume Docker dan folder `backup/output` sama-sama di
drive D:). Bucket `backup` ada di data lake yang sama, jadi bukan perlindungan bila data lake rusak. Untuk
produksi, `backup/output/` perlu disalin otomatis ke lokasi lain (drive eksternal / object storage lain /
cloud) — **butuh tindakan pemilik**, karena di luar folder project.

## Cara memulihkan (laptop/server baru)

```bash
# 0. clone repo, salin .env, letakkan folder backup/output/ hasil backup
docker compose build
docker compose up -d postgres datalake datalake-init        # volume baru: role & DB kosong dibuat init
docker compose run --rm --no-deps airflow-scheduler python -m backup.restore semua
docker compose up -d                                         # Airflow & dashboard membaca data hasil restore
# 1. menjalankan ulang pipeline: trigger DAG posyandu_pipeline. Bila data/source kosong, ingestion
#    otomatis memakai objek raw zone (dq_log ING02), sehingga DWH dapat dibangun ulang dari raw zone.
```

Uji otomatis seluruh langkah di atas (tanpa menyentuh stack utama): `bash tests/uji_pemulihan.sh`
→ `docs/bukti/30_uji_pemulihan.txt`. Hasil 16 Nov 2026: RTO 18 detik (stack kosong → data terverifikasi),
172 detik termasuk pipeline ulang; hasil validasi identik dengan stack utama. RPO = maksimal 24 jam
(backup harian); dalam praktik data hanya berubah saat pipeline bulanan.

## Skenario kegagalan → tindakan sistem

| Kejadian | Yang terjadi |
|---|---|
| Volume PostgreSQL rusak/hilang | restore dump terbaru (`restore db`), lalu verifikasi |
| Data lake hilang | `restore lake` dari mirror; objek & metadata sha256 kembali sama |
| Berkas sumber di `data/source` hilang | pipeline tetap jalan dari raw zone (ING02) |
| Dump rusak | `restore` menolak (SHA-256 beda dari manifest), tidak ada perubahan; pakai dump sebelumnya (`--stempel`) |
| Restore gagal di tengah | `pg_restore --single-transaction --exit-on-error`: database tidak berubah |
| Backup gagal | task GAGAL di batch_log, retry 2×, callback ke dq_log; backup setengah jadi dihapus |
