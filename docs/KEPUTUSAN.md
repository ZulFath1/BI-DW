# Catatan Keputusan Teknis

Format: nomor, tanggal, keputusan, alasan, dan siapa yang memutuskan. Keputusan yang
mengubah rancangan UTS dirinci di [PERUBAHAN_DARI_UTS.md](PERUBAHAN_DARI_UTS.md).

| No | Tanggal | Keputusan | Alasan singkat | Diputuskan |
|---|---|---|---|---|
| K01 | 15 Nov 2026 | Dashboard memakai Streamlit (bukan Metabase) | Source code dashboard wajib ada di repository (lihat P01) | Pemilik |
| K02 | 15 Nov 2026 | Data lake memakai PGSTY Silo `RELEASE.2026-11-12T00-00-00Z`, di-pin digest | Image MinIO community tidak tersedia (lihat P02) | Pemilik |
| K03 | 15 Nov 2026 | Orkestrasi Apache Airflow 3.3.2 (`apache/airflow:3.3.2-python3.12`), LocalExecutor | Airflow 2 dan 3.1 sudah EOL; 3.4.0 terlalu baru (lihat P03) | Pemilik |
| K04 | 15 Nov 2026 | ML utama: clustering posyandu (K-Means); anomaly detection opsional | Forecasting tidak layak (44 bulan, kategori berubah, 2026 belum lengkap) | Pemilik |
| K05 | 15 Nov 2026 | Dataset tidak di-commit ke repository | Dikonfirmasi ke dosen; repo publik; data kader berisi data pribadi | Pemilik |
| K06 | 15 Nov 2026 | Satu container PostgreSQL 16: database DWH terpisah dari metadata Airflow | Hemat sumber daya laptop, tetap terisolasi per database & role | Pemilik |
| K07 | 15 Nov 2026 | Airflow di port host 8081 | Port 8080 dipakai project lain di laptop yang sama | Pemilik |
| K08 | 15 Nov 2026 | Baris total Antrop (baris 498) disimpan di staging dengan `is_baris_total = true`, tidak diteruskan ke curated/DWH | Lineage utuh + dipakai rekonsiliasi | Pemilik |
| K09 | 15 Nov 2026 | Kolom ke-16 sheet Kader (tanpa header, 1 sel) diabaikan dan dicatat di `dq_log` | Tidak ada makna yang bisa dipastikan | Pemilik |
| K10 | 15 Nov 2026 | E2 (stunting) diambil task `ingest_web`; fallback berkas manual di `data/source/eksternal/` | Portal bisa gagal/menolak | Pemilik |
| K11 | 15 Nov 2026 | Python 3.12 di semua container; Python host tidak dipakai | Didukung Airflow 3.3.2 dan semua pustaka | Pemilik |

## Keputusan teknis tahap 2–4 (15 Nov 2026, mode otonom)

Status **Berlaku** = diambil Claude dalam batas rancangan yang sudah disetujui; **Menunggu** =
perlu konfirmasi pemilik sebelum dipakai di transform (minggu 23–29 Nov).

| No | Keputusan | Alasan | Status |
|---|---|---|---|
| K12 | E2 diambil dari **halaman HTML** portal (GET), bukan tombol unduh CSV/Excel | Unduhan portal berupa formulir yang meminta email, jenis kelamin, usia, pekerjaan; pipeline tidak mengirim data pribadi ke luar | Berlaku |
| K13 | Kolom **Alamat Posyandu** di sheet Kader tidak dimuat ke staging | Bisa berupa alamat rumah kader; tidak dibutuhkan analisis/DWH | Berlaku |
| K14 | Raw zone: kunci `sumber/YYYY-MM-DD/berkas`, tidak pernah ditimpa, + versioning bucket | Audit, proses ulang, lineage; versioning = lapis pengaman kedua | Berlaku |
| K15 | `dim_alat` berisi **11 baris** (5 alat × standar/tidak standar + tensimeter "data tidak tersedia") | Dokumen UTS menyebut 10 baris tetapi T14 meminta tensimeter dimuat di `dim_alat`; `fact_alat_snapshot` tetap maks. 4.900 | **Menunggu** |
| K16 | E1 blok **Kelayan Tengah** (hal. 22): kolom kelompok umur di buku berisi angka, bukan label. Staging menyimpan nilai asli + `urutan_dalam_blok`; usulan transform: kelompok umur diambil dari urutan baris, ditandai `is_umur_dari_urutan` | Terbukti dari render halaman; 16 baris lain konsisten (L+P=jumlah, jumlah = Total); pratinjau Q08 selisih 0 dengan cara ini | **Menunggu** |
| K17 | Staging dimuat ulang penuh per batch (DELETE+INSERT satu transaksi) | Sederhana dan atomik; riwayat ada di raw zone | Berlaku |
| K18 | `kader_kode` = HMAC-SHA256 dari nama ternormalisasi, format `KDR-` + 16 heks | Deterministik tanpa menyimpan nama | Berlaku |
| K19 | `kader_kode` **bukan identitas orang**: 2.545 baris → 2.049 kode (202 kode muncul > 1 kali, maks. 15) karena nama sama | Tidak ada NIK/ID kader; analisis kader dihitung per baris, bukan per orang unik | Berlaku (keterbatasan) |
| K20 | Pemetaan `dim_kelompok_umur` → kelompok sasaran: 0–4 bayi balita; 5–19 sekolah & remaja; 20–59 produktif; ≥60 lansia; 5–9 dan 15–19 "pendekatan" | Batas umur kelompok sasaran belum diverifikasi terhadap Panduan [5] UTS | **Menunggu** |
| K21 | Kader dan Antrop memakai `waktu_key` snapshot: Antrop = 202606 (CUT OFF Juni 2026); Kader = 202612 (aturan UTS "bulan akhir periode", karena sheet hanya menyebut "Tahun 2026") | Kader tidak punya bulan snapshot di sumber | **Menunggu** |
