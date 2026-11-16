# ml/

**Clustering posyandu** — `clustering.py`, dijalankan task Airflow `train_ml` setelah `build_mart`.
`laporan.py` mencetak seluruh tabel evaluasi dari database (`python -m ml.laporan`, bukti
`docs/bukti/26_ml_perbandingan.txt`).

| Langkah | Isi |
|---|---|
| Masalah bisnis | Mengelompokkan posyandu menurut kinerja & sumber daya untuk menentukan prioritas pembinaan (tidak ada label → unsupervised) |
| Data | `mart.posyandu_profil` (dari DWH), 487 posyandu yang punya data 2025, diurutkan menurut `posyandu_id` (K51) |
| Fitur | cakupan ibu hamil & bayi balita 2025, % baris anomali, jenis alat standar baik ÷ 5, jumlah kader, % kader terlatih (+ kelengkapan, rasio stunting kelurahan pada set pembanding) |
| Preprocessing | persen di-clip 0–100, kader di-clip p99, kosong → median, log1p pada % anomali (v2), StandardScaler |
| Baseline | aturan manual 3 kelompok (cakupan balita < 50% / kelengkapan < 75% → prioritas tinggi; alat < 5 jenis / kader < 5 / terlatih < 50% → perlu perhatian) |
| Improvement | K-Means (n_init 20) dan Agglomerative ward, K = 2–8, tiga set fitur: `v1_internal`, `v2_internal`, `v2_internal+eksternal` |
| Evaluasi | Silhouette, Davies-Bouldin, Calinski-Harabasz, inertia (elbow), stabilitas ARI antar 10 seed |
| Pemilihan (K39) | K 3–6, ARI ≥ 0,80, cluster terkecil ≥ 5%, Silhouette tertinggi (selisih < 0,02 → DBI terkecil) |
| Interpretasi (K48, K49) | nama cluster dari aturan z-score, tingkat prioritas, rekomendasi; skor & peringkat prioritas per posyandu |
| Output | `mart.hasil_cluster`, `mart.cluster_profil`, `mart.ml_evaluasi`; dibaca dashboard |

## Model terpilih vs model terbaik tanpa syarat ukuran (run 16 Nov 2026)

| | Baseline aturan | **K-Means v2, K=6 (terpilih)** | Agglomerative v1, K=3 (terbaik tanpa syarat ukuran) | Agglomerative v1, K=2 (terbaik mutlak) | K-Means v2 + stunting, K=5 |
|---|---|---|---|---|---|
| Silhouette ↑ | 0,068 | **0,211** | 0,357 | 0,494 | 0,189 |
| Davies-Bouldin ↓ | 2,188 | **1,274** | 0,992 | 0,785 | 1,553 |
| ARI antar seed | – | 0,91 | – (deterministik) | – | 0,95 |
| Ukuran cluster | 307/126/54 | 121/117/88/71/59/31 | **436**/35/16 | **471**/16 | 150/128/100/69/40 |
| Cluster terbesar | 63% | 24,8% | 89,5% | 96,7% | 30,8% |

Syarat "cluster terkecil ≥ 5%" adalah **kebutuhan bisnis**: model tanpa syarat menaruh 436 dari 487 posyandu di
satu cluster (isinya posyandu "rata-rata"; dua cluster kecil hanya pencilan anomali/laporan tidak lengkap), sehingga
tidak membantu menentukan prioritas. Model terpilih justru ber-Silhouette lebih rendah — syarat ini tidak dipakai
untuk menaikkan metrik (P15).

**Arti metrik:** Silhouette 0,21 (−1..1) = cluster terpisah tetapi tumpang tindih (struktur lemah) → label dipakai
sebagai alat bantu prioritas, bukan klasifikasi pasti. Tetap 3× baseline aturan (0,07). DBI 1,27 (makin kecil makin
baik) turun dari 2,19. ARI 0,91 = hasil hampir sama bila seed diganti. Uji reprodusibilitas (K51): urutan data lain
memberi solusi ber-ARI 0,93 dengan 6 nama cluster yang sama.

## Profil 6 cluster (rata-rata skala asli)

| Cluster | Posyandu | Prioritas | % ibu hamil | % bayi balita | % baris anomali | Alat std baik | Kader | % kader terlatih |
|---|---|---|---|---|---|---|---|---|
| K1 Cakupan rendah | 121 | Tinggi | 35,4 | 44,6 | 0,5 | 4,2/5 | 5,0 | 43,6 |
| K2 Kader banyak, cakupan rendah | 31 | Tinggi | 65,0 | 49,5 | 0,5 | 2,9/5 | 8,1 | 49,7 |
| K3 Kekurangan alat antropometri | 59 | Sedang | 59,3 | 57,8 | 1,7 | 0,8/5 | 5,1 | 65,8 |
| K4 Pencatatan perlu diverifikasi | 71 | Sedang | 70,8 | 69,2 | 16,3 | 4,1/5 | 5,2 | 39,3 |
| K5 Cakupan baik, kader belum terlatih | 117 | Sedang | 75,2 | 71,0 | 0,4 | 4,0/5 | 5,1 | 8,7 |
| K6 Kinerja baik (percontohan) | 88 | Rendah | 76,2 | 67,5 | 0,2 | 4,1/5 | 5,0 | 89,8 |
| **Kota** | 487 | | 62,3 | 60,6 | 2,9 | 3,6/5 | 5,3 | 46,0 |

| Cluster | Rekomendasi tindakan |
|---|---|
| K1 | sweeping/kunjungan rumah ibu hamil & balita, pengingat jadwal, koordinasi bidan kelurahan; cek kewajaran Sasaran |
| K2 | supervisi puskesmas atas pembagian tugas & jadwal kader, kunjungan rumah, evaluasi lokasi/jam buka (bukan kurang tenaga) |
| K3 | pengadaan/penggantian alat standar lebih dulu di cluster ini; kalibrasi alat yang ada |
| K4 | verifikasi Sasaran & Cakupan oleh puskesmas sebelum dipakai untuk target; pendampingan pengisian laporan |
| K5 | pelatihan 25 keterampilan bertahap untuk kader |
| K6 | percontohan & mentor bagi posyandu lain; pemantauan rutin |

**Apakah K lebih kecil lebih tepat?** Tidak (K50). K = 4, 5, 6 sama-sama Silhouette 0,211; aturan K39 memilih DBI
terkecil (K = 6: 1,274; K = 5: 1,349; K = 4: 1,514). Secara isi: di K = 5 cluster alat tersebar dan muncul
cluster "Mendekati rata-rata kota" (103 posyandu) yang tidak bisa ditindaklanjuti; K = 4 juga kehilangan cluster
alat dan menggabungkan posyandu berkader terlatih 90% dengan yang 9%. Cluster paling lemah di K = 6 adalah K2 (31
posyandu, 6,4%): pembedanya jumlah kader (rata-rata 8,1), dan jumlah kader dihitung dari baris data kader
(K22), sehingga perlu dicek lapangan sebelum dijadikan dasar kebijakan.

Angka di atas dari run `final2_tahap10__20261116` (setelah K58: jumlah kader 0 = data tidak ada); dashboard selalu menampilkan hasil run terakhir.
