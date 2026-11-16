# Laporan Tahap 8 — Tindak lanjut hasil minggu 23–29 Nov dan uji kegagalan di DAG run nyata

- **Tanggal:** 16 Nov 2026 · **Mode:** otonom · **Dasar:** arahan pemilik poin 1–8 (16 Nov 2026)
- **Status:** semua poin selesai dan dites. Tahap 9 (mart, ML, dashboard) dilaporkan di
  `tahap_09_mart_ml_dashboard.md`.

## 1. Ringkasan per poin arahan

| Poin | Hasil | Rujukan |
|---|---|---|
| 1 Ibu hamil 56,70% vs 56,6% | Bukan pembulatan. Filter UTS (`sasaran > 0`) memberi **10.564 ÷ 18.669 = 56,59%**; implementasi lama 10.577 ÷ 18.655 ikut menghitung 2 baris Sasaran 0 (Cakupan 9 dan 1) dan 2 baris Sasaran negatif (−2, −12). Filter UTS dinyatakan benar dan dipakai di mart/dashboard | K38, P10 |
| 2 974 baris "Tidak berlaku" | 496 baris hanya persen/Dirujuk = 0, 19 baris Cakupan = 0 → dianggap kosong (tetap Tidak berlaku/Tidak terisi). **459 baris Cakupan > 0** → status baru "Cakupan tanpa sasaran" (1 / 5 / 382 / 71 untuk 2023–2026). Dirujuk tidak pernah > 0 | K37, P08, P11 |
| 3 Kader < 5: 31/482 | Dicatat di P07 bahwa angka UTS 32/483 keliru (baris berposyandu "N/A" terhitung posyandu) | P07 |
| 4 92 posyandu baru 2025 | 16 bernama sama persis dengan posyandu tahun sebelumnya di kelurahan lain, 16 bernama mirip, 60 tanpa padanan. Pada semua 32 pasangan, posyandu lama masih melapor di 2025 dan tidak ada posyandu 2024 yang hilang → **tidak ada perpindahan terdeteksi**; 503 versi = 13 perubahan puskesmas | P13 |
| 5 Uji kegagalan di DAG run nyata | (a) DB mati → retry → gagal → callback tercatat; (b) data uji buruk → karantina, DWH tidak berubah; (c) rerun setelah pulih → sukses | bagian 2 |
| 6 Screenshot | Container Playwright (profil compose `bukti`, tidak ada instalasi global): Airflow Graph, Grid sukses, run gagal + retry, detail jadwal, konsol data lake, dashboard | K46, bukti 30–44 |
| 7 Jadwal | DAG **aktif**, `0 2 10 * *` Asia/Makassar; next run 2026-12-09 18:00 UTC = **10 Des 2026 02.00 WITA** (bukti 36) | — |
| 8 Pemetaan manual | Penjelasan "bagian dari data privat" di `README.md` dan `data/README.md` (juga `data/uji/`) | K36 |

## 2. Uji kegagalan di DAG run nyata

Sebelum uji diambil "sidik" DWH: 106.020 baris fakta, Σ sasaran 6.283.900, md5 isi fakta `8fc975a9…`.

| Uji | Run | Cara | Hasil | Bukti |
|---|---|---|---|---|
| (b) data buruk | `uji_b_data_buruk__20261116` | parameter `skenario_uji=data_buruk`: salinan uji dengan baris posyandu sheet 2025 digandakan + sel persen berisi teks | transform mencatat T05 KRITIS; quality gate: Q02 (kritis) GAGAL, Q01 & Q03 peringatan → cabang `quarantine` (tercatat di `dq.quarantine` beserta baris 5 & 492), `load_dw`/mart/ML/dashboard dilewati, run **failed**, callback KRITIS. Sidik DWH **identik** | `22_*.txt`, `34_*karantina.png` |
| (a) DB mati | `uji_a_db_mati__20261116` | `ALTER DATABASE posyandu_dw WITH ALLOW_CONNECTIONS false` (metadata Airflow tetap hidup) | `init_schema`: retry di kode 5× (jeda 2–16 dtk) per percobaan; Airflow 4 percobaan (jeda 2 → 4 → 8 mnt); task **failed**, 15 task hilir `upstream_failed`. 3 callback retry + 1 callback gagal tertulis ke **outbox** (DB tidak tersedia) | `23_*.txt`, `34_*db_retry.png`, `35_*.png` |
| (c) pulih | `uji_c_pulih__20261116` | `ALLOW_CONNECTIONS true`, trigger ulang | 16 task sukses (quarantine dilewati); `init_schema` mengirim 4 catatan outbox ke `dq.batch_log` (tahap `orkestrasi`, percobaan 1–4) dan `dq.dq_log` (3 PERINGATAN + 1 KRITIS); isi DWH sama (md5 sama) | `24_*.txt`, `31_*.png`, `32_*.png` |

**Perbaikan yang ditemukan karena uji ini (K43):** sebelumnya baris posyandu ganda membuat `transform` crash
(PK curated) sehingga bagian "kunci ganda" Q02 tidak pernah bisa terpicu. Kini transform memuat baris pertama,
Q02 memeriksa `curated.pemetaan_posyandu`, dan batch masuk karantina.

**Callback + outbox (K44):** callback yang ditulis langsung ke DB akan hilang justru ketika DB mati. Callback kini
menulis ke DB bila bisa, selain itu ke berkas JSON di volume log Airflow, yang dikirim run berikutnya.

## 3. Berkas yang dibuat / diubah (tahap 8)

| Berkas | Perubahan |
|---|---|
| `etl/transform.py` | K37 status 5; K43 kunci ganda tidak crash |
| `sql/ddl/04_dw_seed.sql` | `dim_status_data` 5 baris (DO UPDATE) |
| `quality/quality_gate.py` | Q02 + pemeriksaan pemetaan; rincian kunci ganda di karantina |
| `common/batch.py` | `catat_kejadian_orkestrasi`, outbox, `kirim_outbox` |
| `etl/schema.py` | init_schema mengirim outbox |
| `common/config.py`, `ingestion/ingest_files.py` | skenario uji (`data/uji/`, awalan raw `uji/`) |
| `dags/posyandu_pipeline.py` | `on_retry_callback`, param `skenario_uji` (+ task tahap 9) |
| `docker-compose.yml` | mount `data/uji`; layanan `screenshot` (profil `bukti`) |
| `docker/playwright/Dockerfile` (baru) | image Playwright 1.52.0 di-pin digest |
| `tests/buat_data_uji.py`, `tests/status_run.py` (baru) | pembuat salinan uji; status task per run |
| `docs/bukti/ambil_screenshot.py` (baru) | skrip screenshot |
| `docs/bukti/22–25_*.txt`, `30–44_*.png`, `docs/bukti/README.md` | bukti |
| `docs/KEPUTUSAN.md` (K37–K47), `docs/PERUBAHAN_DARI_UTS.md` (P07, P08, P10 diperbarui; P11–P14) | catatan |
| `README.md`, `data/README.md`, `dags/README.md`, `sql/validasi/03_dwh.sql` | dokumentasi, D05 filter K38 |

## 4. Penjelasan kode untuk demo

- **Status data (transform_kunjungan):** `bermakna = sasaran kosong & (cakupan > 0 | dirujuk > 0)` → status 5.
- **Quality gate → karantina:** `quality_gate` adalah `@task.branch`; hasil kritis gagal → `quarantine` yang
  menulis `dq.quarantine` lalu sengaja `raise` agar run gagal; `load_dw` dilewati sehingga DWH tidak tersentuh.
- **Retry dua lapis:** `common/db.buka_koneksi` mencoba 5× (2, 4, 8, 16 dtk) untuk gangguan sesaat; bila tetap
  gagal, Airflow mengulang task 3× (2, 4, 8 menit).
- **Outbox:** `catat_kejadian_orkestrasi` → coba DB (2 percobaan) → gagal → `dq_outbox/<waktu>_<task>_<try>.json`;
  `kirim_outbox` di `init_schema` menulis ulang lalu mengganti akhiran menjadi `.terkirim`.

**Pertanyaan dosen yang mungkin muncul**

1. *Kalau database mati, bagaimana kegagalannya tercatat?* — Di log Airflow, dan callback menyimpan ke outbox lalu
   memasukkannya ke `dq.batch_log` saat DB pulih (bukti 23–24).
2. *Apakah data buruk bisa masuk DWH?* — Tidak: pemeriksaan kritis sebelum load mengarahkan ke `quarantine`;
   pemeriksaan kritis saat load memakai ROLLBACK. Bukti 22: md5 isi fakta identik.
3. *Kenapa tidak mematikan container PostgreSQL saja?* — Metadata Airflow ada di container yang sama; mematikan
   container berarti mematikan Airflow juga. `ALLOW_CONNECTIONS false` mensimulasikan database DWH tidak tersedia
   sementara orkestrator tetap hidup (keterbatasan uji ini dicatat).
4. *Kenapa ada status "Cakupan tanpa sasaran"?* — Ada pelayanan nyata tetapi sasaran tidak diisi; tidak bisa
   dihitung persennya dan tidak boleh disembunyikan sebagai "tidak berlaku".
5. *Apakah posyandu pindah kecamatan?* — Tidak terdeteksi di data 2023–2026 (P13); tanpa kode posyandu resmi,
   pindah sekaligus ganti nama tidak bisa dibedakan dari posyandu baru.

## 5. Risiko / kendala / butuh tindakan pemilik

- Uji (a) memakai `ALLOW_CONNECTIONS false`, bukan mematikan server; skenario server mati total belum diuji di DAG
  (sudah diuji di level fungsi, E1).
- Raw zone kini berisi objek uji di `raw/uji/…` (tercatat jelas sebagai uji; dari data privat, tidak di-commit).
- `data/uji/Laporan_uji_data_buruk.xlsx` turunan data privat; ada di path yang diabaikan (`data/*`).
- Image Playwright ±2 GB di Docker (drive D:).
- **Butuh tindakan pemilik:** hanya git (bagian 6).

## 6. Untuk pemilik (git) — satu commit untuk tahap 8 dan 9

Claude **tidak** menjalankan git. Usulan:

```
git add README.md data/README.md docker-compose.yml docker/playwright/Dockerfile \
        common/batch.py common/config.py ingestion/ingest_files.py \
        etl/transform.py etl/schema.py etl/cli.py etl/build_mart.py etl/refresh_dashboard.py \
        quality/quality_gate.py dags/posyandu_pipeline.py dags/README.md \
        ml/__init__.py ml/clustering.py ml/README.md \
        dashboard/app.py dashboard/.streamlit/config.toml \
        sql/ddl/04_dw_seed.sql sql/ddl/07_mart.sql sql/validasi/03_dwh.sql sql/validasi/04_mart.sql \
        tests/buat_data_uji.py tests/status_run.py \
        docs/KEPUTUSAN.md docs/PERUBAHAN_DARI_UTS.md docs/bukti/README.md docs/bukti/ambil_screenshot.py \
        docs/bukti/22_uji_dag_data_buruk_karantina.txt docs/bukti/23_uji_dag_db_mati_retry_callback.txt \
        docs/bukti/24_uji_dag_pulih_rerun_sukses.txt docs/bukti/25_validasi_mart.txt \
        docs/bukti/3*.png docs/bukti/4*.png \
        docs/laporan_tahap/tahap_08_tindak_lanjut_uji_kegagalan.md docs/laporan_tahap/tahap_09_mart_ml_dashboard.md
git status            # yang tersisa seharusnya hanya berkas yang diabaikan
git diff --cached     # periksa sekilas: tidak ada .env, .csv, data/, CLAUDE.md
git commit -m "Status cakupan tanpa sasaran, filter KPI UTS, uji kegagalan DAG, data mart, clustering, dan dashboard Streamlit"
git push              # tanpa --force
```

Pemeriksaan berkas sensitif (tanpa git, 16 Nov 2026):
- 10 nilai rahasia `.env` (kata sandi, kunci) dicari di seluruh folder kecuali `.git/`, `data/`, `.env`:
  **tidak ditemukan**. Pola `KDR-` (kode kader): **tidak ditemukan**.
- Berkas data di luar `data/`: hanya `etl/pemetaan_posyandu_manual.csv` (diabaikan `*.csv`, sengaja privat) dan
  `.env` (diabaikan). `data/uji/` diabaikan oleh `data/*`.
- Bukti teks 23–24 memuat IP internal jaringan Docker (172.19.x.x) dan pesan error koneksi; bukan rahasia.
- Screenshot 30–44 diperiksa satu per satu: tidak ada data pribadi kader, kata sandi, atau token.
