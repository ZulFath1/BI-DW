# Laporan Tahap 2 — Ingestion ke Data Lake (Raw Zone)

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 16–22 Nov (CLAUDE.md 5.6) · **Mode:** otonom
- **Status:** selesai dan dites

## 1. Tujuan

Mengambil ketiga sumber (Excel internal, PDF E1, web E2) dan menyimpannya **apa adanya** di raw
zone data lake, dengan catatan lineage (checksum, ukuran, waktu, asal) dan error handling.

## 2. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `common/config.py`, `common/errors.py` | Konfigurasi dari `.env`; jenis error (`StrukturSumberError`, `DataKorupError`, `SumberTidakTersediaError`) |
| `common/db.py` | Koneksi PostgreSQL dengan retry (jeda 2, 4, 8, 16 detik) |
| `common/batch.py` | `catat_task` → `dq.batch_log` (BERJALAN → SUKSES/GAGAL); `tulis_dq_log`; `catat_quarantine` |
| `common/lake.py` | Klien S3 (boto3), simpan ke raw tanpa menimpa, manifest, baca objek + verifikasi checksum |
| `ingestion/ingest_files.py` | Ingest Excel & PDF (cek tanda tangan berkas, checksum) |
| `ingestion/ingest_web.py` | Ingest halaman E2 2024 & 2025 (retry 3×, fallback berkas manual) |
| `ingestion/lake_setup.py` | Kini memakai `common/lake.py` dan mengaktifkan versioning bucket |
| `sql/ddl/01_dq.sql` | `dq.batch_log`, `dq.dq_log`, `dq.quarantine`, `dq.ingest_manifest` |
| `etl/schema.py`, `etl/cli.py` | Task `init_schema`; CLI untuk menjalankan task manual |
| `docker-compose.yml` | Mount `./common` (dan `./tests`, tahap 3) ke container Airflow |
| `docs/KEPUTUSAN.md`, `docs/PERUBAHAN_DARI_UTS.md` | Dibuat (P01–P03, K01–K21) |

## 3. Cara kerja (untuk dijelaskan saat demo)

1. `ingest_excel` membaca `data/source/...xlsx` (folder di-mount **read-only**), memeriksa bahwa
   berkas diawali `PK` (xlsx = zip), menghitung SHA-256, lalu mengunggah ke
   `raw/internal_laporan_posyandu/2026-11-15/Laporan_Kunjungan_Posyandu.xlsx`.
2. Bila objek dengan kunci sama sudah ada dan checksum sama → tidak diunggah ulang (`sudah_ada`).
   Bila checksum berbeda → disimpan dengan awalan jam, objek lama tidak disentuh.
3. Setiap pengambilan dicatat di `dq.ingest_manifest`; tahap staging hanya membaca objek yang
   tercatat di manifest batch yang sama (lineage: baris staging → manifest → objek data lake).
4. `ingest_web` mengambil halaman HTML portal Satu Data. Gagal → coba lagi 3× → fallback
   `data/source/eksternal/stunting_<tahun>.html` → bila tetap tidak ada, task GAGAL.

## 4. Hasil & bukti

| Sumber | Objek raw | Ukuran | Cocok acuan |
|---|---|---|---|
| Excel internal | `internal_laporan_posyandu/2026-11-15/Laporan_Kunjungan_Posyandu.xlsx` | 2.524.269 byte | SHA-256 `f21c45b8…` ✔ |
| PDF E1 | `e1_disdukcapil_agregat_2025s2/2026-11-15/Buku_Data_...pdf` | 9.108.359 byte | SHA-256 `2382f1bc…` ✔ |
| E2 2024 / 2025 | `e2_stunting_<tahun>/2026-11-15/*.html` | 224.296 / 224.325 byte | isi tabel diverifikasi di tahap 3 |

- Versioning bucket `raw` dan `backup`: **Enabled**.
- Idempotensi: jalankan ulang → Excel & PDF `sudah_ada` (tidak terduplikasi).
- Bukti: `docs/bukti/10_run_pipeline_minggu1.txt`, `docs/bukti/15_isi_raw_zone.txt`,
  `docs/bukti/13_uji_error_handling.txt` (E1–E3).

## 5. Kelemahan, risiko, batasan (jujur)

- **Raw zone memuat data pribadi kader** (berkas Excel asli, sesuai prinsip "apa adanya").
  Akses hanya lewat kredensial root data lake di `.env`; port hanya 127.0.0.1. Pengembangan
  lanjutan: user data lake terpisah dengan hak baca saja untuk pipeline, enkripsi bucket.
- **Halaman web E2 selalu tersimpan sebagai objek baru** karena HTML memuat token formulir
  (`_token`) yang berubah setiap diakses. Raw zone kini berisi 10 objek HTML dari uji berulang
  hari ini (±220 KB per objek; kecil, tetapi menumpuk bila dijalankan sering).
- Ketergantungan pada **struktur HTML portal**: bila portal berubah tampilan, staging gagal dengan
  `StrukturSumberError` (disengaja; lebih baik gagal daripada salah baca).
- Fallback hanya mendukung **HTML simpanan browser**; format CSV/Excel unduhan portal belum
  diverifikasi strukturnya, jadi belum didukung.
- Waktu `LastModified` di data lake tercatat dalam UTC; tanggal di kunci objek memakai WITA.
- Ingestion masih dijalankan lewat CLI; DAG Airflow dijadwalkan minggu 23–29 Nov.

## 6. Untuk pemilik (git)

Claude **tidak** menjalankan perintah git apa pun. Saran commit (setelah tahap 3 dan 4 juga
dibaca, karena `etl/cli.py` sudah mereferensikan modul staging secara lazy):

```
git add common/__init__.py common/config.py common/errors.py common/db.py common/batch.py common/lake.py \
        ingestion/__init__.py ingestion/ingest_files.py ingestion/ingest_web.py ingestion/lake_setup.py ingestion/README.md \
        etl/__init__.py etl/schema.py etl/cli.py sql/ddl/01_dq.sql docker-compose.yml \
        docs/KEPUTUSAN.md docs/PERUBAHAN_DARI_UTS.md docs/bukti/15_isi_raw_zone.txt \
        docs/laporan_tahap/tahap_02_ingestion.md
git status
git diff --cached          # periksa: tidak ada .env, data/, CLAUDE.md, isi dataset, kredensial
git commit -m "Ingestion Excel, PDF, dan web ke data lake beserta manifest dan error handling"
```

Yang **tidak boleh** ikut: `.env`, folder `data/`, `CLAUDE.md`, berkas dari scratchpad.
`docs/bukti/15_isi_raw_zone.txt` hanya berisi nama objek dan ukuran, tanpa isi berkas.
