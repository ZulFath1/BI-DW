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

Status **Berlaku** = diambil Claude dalam batas rancangan yang sudah disetujui; **Diputuskan** =
diputuskan pemilik (K15, K16, K20, K21 pada 15 Nov 2026, sesi kedua).

| No | Keputusan | Alasan | Status |
|---|---|---|---|
| K12 | E2 diambil dari **halaman HTML** portal (GET), bukan tombol unduh CSV/Excel | Unduhan portal berupa formulir yang meminta email, jenis kelamin, usia, pekerjaan; pipeline tidak mengirim data pribadi ke luar | Berlaku |
| K13 | Kolom **Alamat Posyandu** di sheet Kader tidak dimuat ke staging | Bisa berupa alamat rumah kader; tidak dibutuhkan analisis/DWH | Berlaku |
| K14 | Raw zone: kunci `sumber/YYYY-MM-DD/berkas`, tidak pernah ditimpa, + versioning bucket | Audit, proses ulang, lineage; versioning = lapis pengaman kedua | Berlaku |
| K15 | `dim_alat` berisi **11 baris** (5 alat × standar/tidak standar + tensimeter "data tidak tersedia"). Angka 10 di dokumen UTS dikoreksi (P04) | Dokumen UTS menyebut 10 baris tetapi T14 meminta tensimeter dimuat di `dim_alat`; `fact_alat_snapshot` tetap maks. 4.900 karena tensimeter tidak dimuat ke fakta | **Diputuskan** (pemilik) |
| K16 | E1 blok **Kelayan Tengah** (hal. 22): kelompok umur diambil dari **urutan baris**, hanya bila lolos validasi: tepat 16 baris, L+P=Jumlah tiap baris, Σ = total kelurahan, dan Q08 lolos. Baris diberi `flag_kualitas = 'label umur dari urutan baris'`. Anomali Total Kelayan Dalam (kolom bergeser) dan Total Pekauman (tanpa label) dicatat di `dq_log` (T17) | Label umur di buku berisi angka; 16 baris lain konsisten. Hasil 15 Nov: semua syarat lolos, Q08 selisih 0 | **Diputuskan** (pemilik) |
| K17 | Staging dimuat ulang penuh per batch (DELETE+INSERT satu transaksi) | Sederhana dan atomik; riwayat ada di raw zone | Berlaku |
| K18 | `kader_kode` = HMAC-SHA256 dari nama ternormalisasi, format `KDR-` + 16 heks | Deterministik tanpa menyimpan nama | Berlaku |
| K19 | `kader_kode` **bukan identitas orang**: 2.545 baris → 2.049 kode (202 kode muncul > 1 kali, maks. 15) karena nama sama | Tidak ada NIK/ID kader; analisis kader dihitung per baris, bukan per orang unik | Berlaku (keterbatasan) |
| K20 | Pemetaan `dim_kelompok_umur` memakai batas Panduan Kemenkes 2023 [5] dari dokumen UTS: bayi balita → 0–4 (pendekatan); sekolah & remaja → 5–19 (pendekatan); produktif → 20–59 (pendekatan; produktif = dewasa >18–59 adalah asumsi); lansia → 60+ (tepat); ibu hamil tidak dipetakan. Definisi BPS usia produktif (15–64) berbeda | Panduan: 0–6 th, >6–18 th, >18–59 th, ≥60 th — batasnya tidak jatuh di batas 5 tahunan kecuali lansia | **Diputuskan** (pemilik) |
| K21 | Antrop: `waktu_key` = 202606 (dari sel CUT OFF "JUNI 2026"). Kader: **bukan Desember 2026** (bulan yang belum terjadi). `dim_waktu` diberi anggota "bulan tidak diketahui" `202600` (tahun 2026, kolom bulan/tanggal NULL, `is_bulan_diketahui = false`) dan `fact_kader_snapshot.periode_snapshot = 'Tahun 2026'` | Sheet Kader hanya menyebut "Tahun 2026" | **Diputuskan** (pemilik) |

## Keputusan teknis minggu 23–29 Nov (15 Nov 2026, mode otonom)

Diambil Claude Code sesuai mode otonom (CLAUDE.md bagian 1). Format: pilihan, alternatif, alasan, dampak.

| No | Keputusan | Alternatif dipertimbangkan | Alasan | Dampak |
|---|---|---|---|---|
| K22 | **Grain `fact_kader_snapshot`** = satu baris kader per posyandu per snapshot, kunci `kader_snapshot_key` (surrogate). `kader_kode` hanya untuk mendeteksi pasangan kader-posyandu ganda; **kode bukan identitas orang** (ditulis di COMMENT kolom) | Grain per orang unik (`kader_kode`) | Arahan pemilik; tidak ada NIK/ID kader, nama sama → kode sama (K19) | Analisis kader dihitung per baris, bukan per orang |
| K23 | **`posyandu_id` = `<KELURAHAN>--<KUNCI>`**: nama dirapikan (entitas HTML, spasi, huruf besar), awalan POSYANDU/PYD dan akhiran nama kelurahan dibuang (toleran salah ketik ringan), angka Romawi→Arab, nol depan dibuang, inisial kelurahan di akhir dibuang | Kunci dari (kecamatan, kelurahan, nama) persis; hash nama | Teruji: 395/395/487/490 posyandu per tahun, gabungan **490**, **503 versi** (13 pindah Pekauman→Mantuil 2024) — sama dengan UTS; kunci terbaca saat demo | `etl/normalisasi.py` |
| K24 | **`ingest_web`: berubah/tidak dari checksum isi tabel** (JSON kanonik judul+baris+total, parser sama dengan staging). Sama → manifest `tidak_berubah` menunjuk objek lama, tanpa objek baru. Tabel gagal diekstrak → HTML tetap disimpan + WEB02 | Checksum HTML mentah | Arahan pemilik; HTML memuat token formulir yang berubah | Raw zone tidak bertambah setiap run; objek HTML lama dari uji minggu lalu tetap disimpan (raw zone tidak dihapus) |
| K25 | Pencocokan nama posyandu Antrop/Kader: (1) kunci sama persis dalam kelurahan, (2) **tabel manual `etl/pemetaan_posyandu_manual.csv`** (9 baris Antrop, sudah ditinjau), (3) fuzzy ≥ 0,85 yang unggul jelas → PERINGATAN untuk ditinjau, (4) sisanya ditahan (dq_log, tidak masuk fakta) | Hanya cocok persis (UTS: 94 dari 407); fuzzy tanpa tinjauan | Antrop: 481 kunci + 9 manual = 490/490; Kader: 2.544 kunci + 1 ditahan | Hasil deterministik; nama baru yang mirip tetap terlihat |
| K26 | **Lestari IV**: Antrop menulis kelurahan Kelayan Barat, laporan menulis "LESTARI IV KELAYAN BARAT" di kelurahan Pekauman → dipetakan manual ke posyandu laporan; kelurahan mengikuti laporan | Menahan baris | Satu-satunya pasangan tersisa, nama identik | Dicatat di CSV manual dengan alasan |
| K27 | **Load DWH**: dimensi di-upsert (surrogate key stabil antar-batch); kelima fakta dihapus lalu disisip ulang dari curated, **dalam satu transaksi** bersama pemeriksaan Q04–Q06 | Upsert per baris fakta (UTS 4.3) | Curated selalu lengkap, hasil sama dengan upsert tetapi lebih sederhana; atomik | Uji E8: load 2× hasil identik |
| K28 | Status baris dengan **Sasaran kosong tetapi metrik lain terisi** (974 baris): mengikuti definisi T06 (Terlapor = Sasaran terisi) → Tidak berlaku (2023–2025) / Tidak terisi (2026); nilai lain tetap dimuat apa adanya + PERINGATAN T06 | Status kelima; Terlapor | Tidak menambah kategori di luar rancangan; Q04 tetap seimbang | Jumlah "Tidak berlaku" 2023/2024 lebih besar dari angka UTS (P08) |
| K29 | Bit 8 `flag_anomali` (Cakupan > Sasaran) dihitung **harfiah** (termasuk Sasaran 0/negatif), boleh tumpang tindih dengan bit 32 | Hanya Sasaran > 0 | Definisi UTS; hasil 36/50/84/163 sama dengan acuan | — |
| K30 | Quality gate dua fase: Q01–Q03, Q07–Q09 sebelum load (task `quality_gate`, cabang `quarantine` bila Q02 gagal); Q04–Q06 di dalam transaksi `load_dw` (gagal → ROLLBACK). Hasil di `dq.quality_result` lewat koneksi autocommit | Semua cek setelah load | Q04/Q05 membutuhkan isi fakta; rollback menjamin DWH tidak berubah | Task `quarantine` sengaja GAGAL agar DAG run terlihat gagal (retries 0) |
| K31 | DAG `posyandu_pipeline`: cron `0 2 10 * *` WITA, `catchup=False`, `max_active_runs=1`, retries 3 dengan exponential backoff (2 → maks. 10 menit), `on_failure_callback` → dq_log KRITIS; `batch_id = run_id` | DAG terpisah per sumber | Satu DAG cukup untuk volume ini; jadwal sumber lain dicatat di UTS 4.4 | Saat diaktifkan 15 Nov, scheduler langsung membuat run terjadwal 10 Nov (interval terakhir) — sukses |
| K32 | Kader (T12): kelompok umur `<30, 30-39, 40-49, 50-59, >=60`; pendidikan diseragamkan (`S 1`→S1, dst.), nilai tak dikenal ("DMP", 1 baris) → "Tidak valid"; JKN/pelatihan → boolean | Menebak "DMP" = SMP | Tidak menebak data | — |
| K33 | Antrop: sel `-` pada jumlah alat dimuat NULL dengan `flag_kualitas` (1 baris); tensimeter tidak dimuat ke fakta (kolom "Ada" kosong di 490 baris) | `-` = 0 | Kosong ≠ 0 (prinsip T06) | — |
| K34 | Baris Kader berkelurahan & berposyandu "N/A" (1 baris) **ditahan** (tidak masuk fakta), dicatat di dq_log; Q09 = fakta + T13 + ditahan = staging | Membuat posyandu "N/A" | Tidak dapat dipetakan ke posyandu mana pun | Fakta kader 2.542 (UTS 2.543), lihat P07 |
| K35 | Screenshot UI Airflow **ditunda** ke tahap uji end-to-end (7–13 Des); bukti run sementara berupa teks (`docs/bukti/16_*.txt`) | Memasang Playwright di host | Playwright tidak ada di host; instal global dilarang CLAUDE.md. Rencana: container Playwright di jaringan compose | Belum ada bukti gambar Airflow |
| K36 | `etl/pemetaan_posyandu_manual.csv` **tidak di-commit** (tetap diabaikan pola `*.csv`); pipeline tetap berjalan tanpa berkas itu (PERINGATAN T14; kunci + fuzzy) | Membuat pengecualian `.gitignore` | Berisi nama posyandu dari dataset privat; repo publik; aturan privasi: bila ragu, jangan di-commit | Tanpa berkas: 8 nama tetap cocok (fuzzy), Lestari IV ditahan → fakta alat 4.890 (diuji) |
