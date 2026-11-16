# Laporan Tahap 9 — Data mart, ML clustering, dashboard Streamlit

- **Tanggal:** 16 Nov 2026 · **Jadwal:** minggu 30 Nov–6 Des (dikerjakan lebih awal) · **Mode:** otonom
- **Status:** selesai dan dites; ketiga task baru (`build_mart → train_ml → refresh_dashboard`) berjalan di
  DAG run nyata

## 1. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `sql/ddl/07_mart.sql` (baru) | 10 tabel schema `mart` |
| `etl/build_mart.py` (baru) | dw → mart dalam satu transaksi; filter KPI K38 |
| `ml/clustering.py`, `ml/__init__.py` (baru) | baseline aturan, K-Means & Agglomerative K=2–8, tiga set fitur, evaluasi, pemilihan (K39), profil & label cluster |
| `etl/refresh_dashboard.py` (baru) | penanda batch untuk cache Streamlit + cek health dashboard |
| `dashboard/app.py` (ditulis ulang), `dashboard/.streamlit/config.toml` | 5 tab, filter global, insight, hasil ML; tema terang |
| `dags/posyandu_pipeline.py` | + `build_mart`, `train_ml`, `refresh_dashboard` |
| `etl/cli.py` | + perintah `build-mart`, `train-ml`, `refresh-dashboard` |
| `sql/validasi/04_mart.sql` (baru), `sql/validasi/03_dwh.sql` (D05 → filter K38) | validasi terhadap angka acuan |
| `ml/README.md`, `dags/README.md`, `README.md` | dokumentasi |

## 2. Data mart (schema `mart`)

| Tabel | Grain / isi | Baris (run 16 Nov) |
|---|---|---|
| `kpi_cakupan_bulanan` | kecamatan × bulan × kelompok: Σ cakupan, Σ sasaran, persen (K38), baris anomali, baris status 5 | 1.200 |
| `kelengkapan_bulanan` | kecamatan × bulan: posyandu terdaftar & terlapor (kelompok mana pun) | 240 |
| `posyandu_profil` | 1 baris per posyandu (versi aktif): fitur 2025, alat Juni 2026, kader 2026, stunting kelurahan | 490 |
| `kpi_kelurahan` | kelurahan × tahun: stunting, penduduk 0–4, rasio (T19), sasaran & cakupan balita | 104 |
| `ringkasan_kualitas` | tahun × (status / jenis anomali / T07) | 49 |
| `quality_gate_terakhir` | Q01–Q09 batch ini | 9 |
| `ml_evaluasi`, `hasil_cluster`, `cluster_profil` | hasil ML | 45 / 487 / 36 |
| `dashboard_refresh` | penanda refresh per run | bertambah |

**Cocok dengan angka acuan 4.6** (`sql/validasi/04_mart.sql`, bukti `docs/bukti/25_validasi_mart.txt`):

| Angka acuan | Hasil | Status |
|---|---|---|
| Cakupan ibu hamil 2025 56,6% | 10.564 ÷ 18.669 = 56,59% | ✔ (setelah K38; lihat laporan tahap 8) |
| Cakupan bayi balita 2025 58,7% | 166.622 ÷ 283.775 = 58,72% | ✔ |
| Kelengkapan Des 2025 475/487 | 475 / 487 | ✔ |
| Baris anomali 2.073; 2025 = 826 | 284 + 333 + 826 + 630 = 2.073 | ✔ |
| Sasaran bayi balita Des 2025 per kecamatan, kota 23.299 | Tengah 3.499, Utara 6.281, Barat 5.151, Selatan 5.530, Timur 2.838; kota 23.299 | ✔ |
| Posyandu < 5 kader | 31 / 482 | ✔ (angka UTS 32/483 keliru, P07) |
| Alat rusak/tidak standar 335/490; lengkap 164 | 335 / 490; 164 | ✔ |
| Rasio stunting kota 2025 3,85% | 3,85% | ✔ |

## 3. Machine learning — clustering posyandu

Alur **Baseline → Improvement → Comparison → Model terpilih** (rincian `ml/README.md`):

1. **Baseline** aturan manual 3 kelompok: Silhouette 0,064–0,069, DBI 2,19–2,51 (tergantung ruang fitur).
2. **Percobaan 1 (v1, 7 fitur apa adanya):** aturan pemilihan awal memilih Agglomerative K=3 (Silhouette 0,357)
   tetapi cluster-nya 436/16/35 — memisahkan pencilan (kelengkapan < 100%, anomali tinggi), bukan profil.
3. **Improvement:** (a) rekayasa fitur v2 — `kelengkapan_2025` dibuang (97,5% = 100%), `% anomali` di-log1p;
   (b) syarat cluster terkecil ≥ 5% ditambahkan ke aturan pemilihan (dicatat jujur, K39); (c) uji fitur
   eksternal (rasio stunting kelurahan).
4. **Comparison:** 45 model (3 set fitur × [aturan + K-Means K2–8 + Agglomerative K2–8]) di `mart.ml_evaluasi`.
5. **Terpilih:** `kmeans_v2_internal_k6` — Silhouette 0,208, DBI 1,296, CH 95, ARI antar seed 0,941;
   cluster 120 / 117 / 90 / 71 / 59 / 30. Fitur eksternal menurunkan Silhouette menjadi 0,179 (K41).

**Arti & batasan:** Silhouette 0,21 = struktur lemah, cluster tumpang tindih; dipakai sebagai alat bantu
prioritas, bukan klasifikasi pasti. Tetap 3× lebih baik dari baseline aturan. Kesesuaian dengan baseline
rendah (ARI 0,10): aturan manual dan cluster mengelompokkan dengan cara berbeda — aturan hanya melihat ambang
satu-dua fitur, cluster melihat kombinasi. Fitur memakai periode 2025 sedangkan alat/kader snapshot 2026 (K42).

## 4. Dashboard (http://127.0.0.1:8501)

- **Sumber:** hanya schema `mart` dengan user `dashboard_ro` (read-only). Tidak ada data pribadi kader.
- **Filter global:** tahun, kecamatan (multi), kelompok sasaran.
- **Tab:** (1) KPI + 5 insight dihitung dari data; (2) tren cakupan bulanan per kecamatan + tabel; (3) hasil ML:
  ukuran cluster, heatmap profil (biru lebih baik / merah lebih buruk dari rata-rata kota), daftar posyandu per
  cluster, evaluasi model; (4) rasio stunting per kelurahan (label "rasio, bukan prevalensi"); (5) quality gate
  terakhir, anomali per jenis, status data per tahun.
- **Desain grafik:** mengikuti panduan dataviz (palet tervalidasi, warna mengikuti kecamatan, satu sumbu,
  tooltip, tabel data). Grafik anomali 7 warna diganti satu seri per tahun agar tidak terlalu ramai.
- Bukti: `docs/bukti/40–44_dashboard_*.png`.

## 5. Pertanyaan demo yang mungkin muncul

1. **Kenapa clustering, bukan prediksi?** — Tidak ada label "posyandu bermasalah"; tujuannya menemukan profil.
   Forecasting tidak layak (44 bulan, kategori berubah, 2026 belum lengkap; K04).
2. **Silhouette cuma 0,2, apakah modelnya jelek?** — Strukturnya memang lemah (data kinerja posyandu bersifat
   kontinu, tidak terpisah tegas). Model tetap jauh lebih konsisten dari aturan manual (0,07), stabil (ARI 0,94),
   dan profilnya bisa dijelaskan. Kami tidak mengklaim klasifikasi pasti.
3. **Kenapa tidak pilih model dengan Silhouette tertinggi (0,357)?** — 90% posyandu berada di satu cluster;
   tidak berguna untuk menentukan prioritas. Syarat ukuran ditambahkan dan dicatat (K39).
4. **Kenapa data stunting tidak dipakai di model?** — Sudah diuji: menurunkan kualitas cluster karena nilainya
   sama untuk semua posyandu sekelurahan. Tetap ditampilkan di dashboard.
5. **Bagaimana dashboard tahu ada data baru?** — `refresh_dashboard` menulis batch_id baru di
   `mart.dashboard_refresh`; cache Streamlit dikunci dengan batch_id itu.

## 6. Risiko / kendala

- `kelengkapan_2025` hampir konstan → tidak berguna sebagai pembeda; indikator kelengkapan per kelompok bisa
  dipertimbangkan bila kategori layanan sudah seragam.
- Label cluster dibuat otomatis dari z-score; urutan K1..K6 berdasarkan rata-rata semua fitur (bobot sama), bukan
  penilaian kebijakan. Bila dinas punya bobot prioritas, urutan perlu disesuaikan.
- Anomaly detection (Isolation Forest) belum dikerjakan (opsional, K04).

## 7. Untuk pemilik (git)

Lihat bagian 8 di `tahap_08_tindak_lanjut_uji_kegagalan.md` (satu usulan commit untuk tahap 8 dan 9).
