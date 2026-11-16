# ml/

**Clustering posyandu** — `clustering.py`, dijalankan task Airflow `train_ml` setelah `build_mart`.

| Langkah | Isi |
|---|---|
| Masalah bisnis | Mengelompokkan posyandu menurut kinerja & sumber daya untuk menentukan prioritas pembinaan (tidak ada label → unsupervised) |
| Data | `mart.posyandu_profil` (dari DWH), 487 posyandu yang punya data 2025 |
| Fitur | cakupan ibu hamil & bayi balita 2025, % baris anomali, jenis alat standar baik ÷ 5, jumlah kader, % kader terlatih (+ kelengkapan, rasio stunting kelurahan pada set pembanding) |
| Preprocessing | persen di-clip 0–100, kader di-clip p99, kosong → median, log1p pada % anomali (v2), StandardScaler |
| Baseline | aturan manual 3 kelompok (cakupan balita < 50% / kelengkapan < 75% → prioritas tinggi; alat < 5 jenis / kader < 5 / terlatih < 50% → perlu perhatian) |
| Improvement | K-Means (n_init 20) dan Agglomerative ward, K = 2–8, tiga set fitur: `v1_internal`, `v2_internal`, `v2_internal+eksternal` |
| Evaluasi | Silhouette, Davies-Bouldin, Calinski-Harabasz, inertia (elbow), stabilitas ARI antar 10 seed |
| Pemilihan (K39) | K 3–6, ARI ≥ 0,80, cluster terkecil ≥ 5%, Silhouette tertinggi (selisih < 0,02 → DBI terkecil) |
| Output | `mart.hasil_cluster`, `mart.cluster_profil`, `mart.ml_evaluasi`; dibaca dashboard |

## Hasil (run 16 Nov 2026)

| Model | Silhouette | Davies-Bouldin | ARI seed | Ukuran cluster |
|---|---|---|---|---|
| Baseline aturan (ruang fitur v2) | 0,069 | 2,192 | – | 54 / 307 / 126 |
| **K-Means v2 internal, K=6 (terpilih)** | **0,208** | **1,296** | 0,941 | 120 / 30 / 59 / 90 / 71 / 117 |
| K-Means v2 internal + stunting, K=6 | 0,179 | 1,528 | 0,945 | — |
| Agglomerative v1, K=3 (ditolak: 436/16/35) | 0,357 | 0,991 | – | cluster 90% |

**Arti metrik:** Silhouette 0,21 (skala −1..1) berarti cluster terpisah tetapi tumpang tindih cukup banyak
(struktur lemah) — label cluster dipakai sebagai alat bantu prioritas, bukan klasifikasi pasti. Tetap jauh di
atas baseline aturan (0,07), artinya pengelompokan berbasis data lebih konsisten daripada aturan manual.
DBI 1,30 (makin kecil makin baik) turun dari 2,19 pada baseline. ARI 0,94 berarti hasil hampir sama bila
seed diganti (stabil).

## Profil cluster (nama dibuat otomatis dari fitur paling menonjol, |z| ≥ 0,5)

| Cluster | Posyandu | Karakter |
|---|---|---|
| K1 alat standar kurang | 59 | rata-rata 0,8 dari 5 jenis alat standar baik (kota 3,6) |
| K2 cakupan rendah | 120 | cakupan ibu hamil 36%, bayi balita 45% |
| K3 data banyak anomali | 71 | 16,3% baris laporan bertanda anomali (kota 2,9%) |
| K4 kader belum terlatih & cakupan tinggi | 117 | kader terlatih 8%, tetapi cakupan tinggi |
| K5 kader banyak & cakupan rendah | 30 | 8 kader, cakupan bayi balita 44% |
| K6 kader terlatih & cakupan tinggi | 90 | kader terlatih 90%, cakupan ibu hamil 76% |

Angka di atas dari `mart.cluster_profil` run 16 Nov 2026; dashboard selalu menampilkan hasil run terakhir.
