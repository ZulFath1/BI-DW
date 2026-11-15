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

## Perubahan minggu 23–29 Nov (dicatat 15 Nov 2026)

| No | Komponen | Rancangan UTS | Implementasi UAS | Tanggal |
|---|---|---|---|---|
| P04 | `dim_alat` | 10 baris | 11 baris (tensimeter "data tidak tersedia") — koreksi dokumen UTS | 15 Nov 2026 |
| P05 | `dim_waktu` & snapshot kader | 48 bulan; fakta snapshot memakai bulan akhir periode | 48 bulan + anggota "2026, bulan tidak diketahui" (`202600`); kader `periode_snapshot = 'Tahun 2026'` | 15 Nov 2026 |
| P06 | `dim_kelompok_umur` | "dipetakan ke kelompok sasaran secara pendekatan" tanpa rincian (seed tahap 4 sempat menandai 0–4, 10–14, 20–59 "tepat") | Hanya ≥ 60 "tepat"; lainnya "pendekatan" sesuai batas Panduan 2023 | 15 Nov 2026 |
| P07 | Fakta kader & KPI kader | 2.543 baris; < 5 kader: 32 dari 483 | 2.542 baris; < 5 kader: **31 dari 482** | 15 Nov 2026 |
| P08 | Status "Tidak berlaku" 2023/2024 | 11.728 / 10.785 | 12.083 / 10.861 | 15 Nov 2026 |
| P09 | Muat fakta | upsert per kunci fakta | DELETE + INSERT ulang dalam satu transaksi; dimensi tetap upsert | 15 Nov 2026 |
| P10 | Selisih kecil angka acuan | ibu hamil 2025 56,6%; T08 2023 625 sel | 56,70% (10.577 ÷ 18.655); T08 2023 624 sel | 15 Nov 2026 |

### P04 — `dim_alat` 11 baris (koreksi dokumen UTS)
- **UTS:** tabel penyimpanan menulis `dim_alat` 10 baris, tetapi aturan T14 dan tabel 5.2 menyebut tensimeter
  tetap dimuat di `dim_alat` dengan status "data tidak tersedia". Kedua pernyataan tidak bisa benar bersamaan.
- **Sekarang:** 11 baris (5 alat × standar/tidak standar + tensimeter). `fact_alat_snapshot` tetap 490 × 10 = 4.900
  karena tensimeter tidak dimuat ke fakta (kolom "Ada" kosong di semua baris).
- **Menjelaskan ke dosen:** angka 10 di dokumen UTS adalah salah hitung; yang benar 10 kombinasi alat + 1 baris
  tensimeter agar dashboard bisa menyatakan "data tensimeter tidak tersedia" secara eksplisit. Diputuskan pemilik (K15).

### P05 — anggota waktu "bulan tidak diketahui"
- **UTS:** fakta tahunan/snapshot memakai `waktu_key` bulan akhir periode; untuk kader berarti Desember 2026.
- **Sekarang:** Desember 2026 belum terjadi saat data dibuat, sehingga memakainya akan menyesatkan.
  `dim_waktu` mendapat anggota `202600` ("Tidak diketahui (2026)", `is_bulan_diketahui = false`, kolom
  bulan/tanggal NULL dengan CHECK constraint), dan fakta kader menyimpan `periode_snapshot = 'Tahun 2026'`.
- **Menjelaskan ke dosen:** teknik "unknown member" Kimball; tidak mengarang bulan yang tidak ada di sumber (K21).

### P06 — ketepatan pemetaan kelompok umur
- **Sekarang:** batas Panduan Kemenkes 2023 (0–6, >6–18, >18–59, ≥60 th). Kelompok 5 tahunan hanya pas untuk
  lansia; sisanya "pendekatan". Usia produktif = dewasa >18–59 adalah **asumsi** perancangan; definisi BPS
  (15–64 tahun) berbeda (K20).

### P07 — kader 2.542 baris, 31 dari 482 posyandu
- **Temuan:** sheet Kader memiliki 1 baris dengan kelurahan **dan** nama posyandu "N/A" (jenis kelamin terisi).
  Baris ini tidak dapat dipetakan ke posyandu mana pun, sehingga ditahan (dq_log T14) dan tidak masuk fakta.
- **Penjelasan selisih:** prototipe UTS tampaknya menghitung "N/A" sebagai posyandu ke-483 dengan 1 kader,
  sehingga ikut terhitung "< 5 kader". Tanpa baris itu: 482 posyandu berkader, 31 di antaranya < 5 baris kader.
  Rekonsiliasi Q09: 2.542 fakta + 2 dihapus T13 + 1 ditahan = 2.545 staging.

### P08 — status "Tidak berlaku" lebih besar
- **Temuan:** 974 baris (355 / 76 / 442 / 101 untuk 2023–2026) memiliki Sasaran kosong tetapi metrik lain terisi.
  Definisi UTS T06 tidak mencakup kasus ini. Angka UTS 11.728 dan 10.785 sama persis dengan jumlah baris yang
  **seluruh** isiannya kosong; implementasi menambahkan 355 dan 76 baris parsial ke "Tidak berlaku" (K28).
  Status Terlapor 2025 (28.037) dan Belum dilaporkan 2026 (7.350) sama dengan UTS.

### P09 — fakta dimuat ulang dalam satu transaksi
- **UTS 4.3:** upsert dengan kunci (posyandu_key, waktu_key, kelompok_key).
- **Sekarang:** fakta dihapus lalu disisip ulang dari curated di dalam satu transaksi (bersama Q04–Q06);
  dimensi tetap upsert sehingga surrogate key stabil. Hasilnya setara upsert (uji E8) dan bila ada pemeriksaan
  kritis gagal, seluruh perubahan di-ROLLBACK (uji E6) (K27).

### P10 — selisih kecil terhadap angka acuan
- **Cakupan ibu hamil 2025:** 10.577 ÷ 18.655 = 56,698% → 56,70% bila dibulatkan; dokumen UTS menulis 56,6%
  (kemungkinan dipotong, bukan dibulatkan — **belum diverifikasi**). Pembilang dan penyebut tidak dibandingkan
  karena UTS tidak mencantumkannya. Bayi balita sama persis (166.622 ÷ 283.775 = 58,7%).
- **T08 2023:** staging hanya berisi 624 sel Sasaran = 0 dan Cakupan kosong (dihitung langsung dari teks asli);
  UTS menulis 625. Penyebab selisih 1 sel **belum dapat dijelaskan**; T08 tidak mengubah nilai apa pun di fakta
  (tetap NULL), sehingga tidak berdampak pada angka lain.
