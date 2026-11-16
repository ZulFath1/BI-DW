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
| P07 | Fakta kader & KPI kader | 2.543 baris; < 5 kader: 32 dari 483 | 2.542 baris; < 5 kader: **31 dari 482** — angka UTS keliru (diterima pemilik 16 Nov 2026) | 15 Nov 2026 |
| P08 | Status "Tidak berlaku" 2023/2024 | 11.728 / 10.785 | 12.082 / 10.856 (setelah K37; sebelumnya 12.083 / 10.861) | 16 Nov 2026 |
| P09 | Muat fakta | upsert per kunci fakta | DELETE + INSERT ulang dalam satu transaksi; dimensi tetap upsert | 15 Nov 2026 |
| P10 | Selisih kecil angka acuan | ibu hamil 2025 56,6%; T08 2023 625 sel | ibu hamil **56,59%** (10.564 ÷ 18.669) setelah filter KPI dibetulkan (K38; sebelumnya 56,70%); T08 2023 624 sel | 16 Nov 2026 |

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
- **Kesimpulan (diterima pemilik, 16 Nov 2026):** angka UTS **32 dari 483 keliru**, karena baris kader
  berposyandu "N/A" terhitung sebagai satu posyandu. Angka yang benar **31 dari 482**; dashboard dan laporan
  memakai angka ini.

### P08 — status "Tidak berlaku" lebih besar
- **Temuan:** 974 baris (355 / 76 / 442 / 101 untuk 2023–2026) memiliki Sasaran kosong tetapi metrik lain terisi.
  Definisi UTS T06 tidak mencakup kasus ini. Angka UTS 11.728 dan 10.785 sama persis dengan jumlah baris yang
  **seluruh** isiannya kosong.
- **Rincian isi 974 baris** (16 Nov 2026, dari nilai teks asli di staging):

  | Isi selain Sasaran | 2023 | 2024 | 2025 | 2026 | Jumlah | Perlakuan |
  |---|---|---|---|---|---|---|
  | hanya % Dilayani / % Dirujuk / Dirujuk bernilai 0 | 354 | 71 | 44 | 27 | 496 | dianggap kosong |
  | Cakupan = 0 (+ persen 0) | 0 | 0 | 16 | 3 | 19 | dianggap kosong |
  | **Cakupan > 0** (Dirujuk tidak pernah > 0) | 1 | 5 | 382 | 71 | **459** | status 5 "Cakupan tanpa sasaran" |

  Semua nilai persen dan Dirujuk di 974 baris itu bernilai 0; satu-satunya isi bermakna adalah Cakupan > 0
  (Σ cakupan 4.496; terutama kelompok usia sekolah, produktif, dan lansia).
- **Keputusan (K37, menggantikan K28):** nol bawaan dianggap kosong → tetap Tidak berlaku (2023–2025) /
  Tidak terisi (2026); baris dengan Cakupan > 0 diberi status baru **"Cakupan tanpa sasaran"** dan tidak masuk
  KPI cakupan. Hasil: Tidak berlaku 2023 / 2024 / 2025 = **12.082 / 10.856 / 801**; Tidak terisi 2026 = 2.747.
  Selisih dengan UTS 2023 (354) dan 2024 (71) adalah baris nol bawaan yang UTS hitung sebagai "terisi".
  Status Terlapor 2025 (28.037) dan Belum dilaporkan 2026 (7.350) tetap sama dengan UTS.
- **Menjelaskan ke dosen:** persen 0 adalah keluaran rumus templat, bukan laporan; tetapi Cakupan > 0 tanpa
  Sasaran adalah pelayanan nyata yang perlu diperbaiki Sasarannya oleh puskesmas, jadi ditampilkan terpisah.

### P09 — fakta dimuat ulang dalam satu transaksi
- **UTS 4.3:** upsert dengan kunci (posyandu_key, waktu_key, kelompok_key).
- **Sekarang:** fakta dihapus lalu disisip ulang dari curated di dalam satu transaksi (bersama Q04–Q06);
  dimensi tetap upsert sehingga surrogate key stabil. Hasilnya setara upsert (uji E8) dan bila ada pemeriksaan
  kritis gagal, seluruh perubahan di-ROLLBACK (uji E6) (K27).

### P10 — selisih kecil terhadap angka acuan
- **Cakupan ibu hamil 2025 — sudah ditelusuri (16 Nov 2026), bukan pembulatan:**

  | Filter | Σ cakupan | Σ sasaran | Persen |
  |---|---|---|---|
  | Implementasi minggu 2: Terlapor, bukan A1 | 10.577 | 18.655 | 56,698% |
  | **Prototipe UTS**: Terlapor, bukan A1, cakupan tidak NULL, sasaran > 0, termasuk T07 | **10.564** | **18.669** | **56,586% ≈ 56,6%** |
  | UTS tanpa cakupan turunan T07 (pembanding) | 10.564 | 18.283 | 57,780% |

  Beda filter = `sasaran > 0` (syarat `cakupan tidak NULL` tidak mengubah apa pun di sini). Implementasi lama ikut
  menghitung 494 baris yang tidak lolos `sasaran > 0`: 490 baris Sasaran 0 (cakupan 0/NULL, tanpa efek),
  **2 baris Sasaran 0 dengan Cakupan 9 dan 1** (+10 cakupan), dan **2 baris Sasaran negatif (−2 dan −12)
  dengan Cakupan 2 dan 1** (−14 sasaran, +3 cakupan). **Yang benar adalah filter UTS** (K38): persen
  cakupan tidak terdefinisi untuk sasaran ≤ 0, dan baris itu sudah bertanda anomali (bit 8/16/32).
  Bayi balita tidak berubah (166.622 ÷ 283.775 = 58,72%).
- **T08 2023:** staging hanya berisi 624 sel Sasaran = 0 dan Cakupan kosong (dihitung langsung dari teks asli);
  UTS menulis 625. Penyebab selisih 1 sel **belum dapat dijelaskan**; T08 tidak mengubah nilai apa pun di fakta
  (tetap NULL), sehingga tidak berdampak pada angka lain.

## Perubahan tindak lanjut & minggu 30 Nov–6 Des (dicatat 16 Nov 2026)

| No | Komponen | Rancangan UTS | Implementasi UAS | Tanggal |
|---|---|---|---|---|
| P11 | `dim_status_data` | 4 status | 5 status: + "Cakupan tanpa sasaran" (K37) | 16 Nov 2026 |
| P12 | Fitur ML | 8 fitur termasuk kelengkapan laporan dan rasio stunting kelurahan | Model terpilih memakai 6 fitur internal; kelengkapan dibuang (hampir konstan), stunting diuji tetapi menurunkan kualitas cluster (K40, K41) | 16 Nov 2026 |
| P13 | Perpindahan posyandu antar kecamatan (SCD 2) | Diperkirakan ada (keterangan narasumber); 503 versi | 503 versi = 490 posyandu + 13 perubahan **puskesmas**; **tidak ada** perpindahan kecamatan/kelurahan yang terdeteksi di data | 16 Nov 2026 |
| P14 | Data mart | `posyandu_profil`, `hasil_cluster`, tabel KPI | 10 tabel: + `kpi_cakupan_bulanan`, `kelengkapan_bulanan`, `kpi_kelurahan`, `ringkasan_kualitas`, `quality_gate_terakhir`, `cluster_profil`, `ml_evaluasi`, `dashboard_refresh` | 16 Nov 2026 |

### P12 — fitur ML
- **UTS 5.3:** cakupan ibu hamil & bayi balita, kelengkapan laporan, jumlah anomali, rasio alat baik, jumlah
  kader, % kader terlatih, rasio stunting kelurahan; improvement "tanpa vs dengan fitur eksternal".
- **Sekarang:** perbandingan tetap dijalankan (set `v1_internal`, `v2_internal`, `v2_internal+eksternal`), tetapi
  model terpilih `kmeans_v2_internal_k6` tidak memakai kelengkapan (475/487 posyandu = 100%) dan rasio stunting
  (Silhouette K=6 turun 0,208 → 0,179). Jumlah anomali dipakai sebagai **persen** baris anomali (log1p) agar
  tidak bergantung pada banyaknya kelompok yang dilaporkan.
- **Menjelaskan ke dosen:** ini hasil tahap Improvement → Comparison yang diminta; menolak fitur juga hasil.

### P13 — perpindahan kecamatan tidak terdeteksi
- **Pemeriksaan (16 Nov 2026):** 92 posyandu pertama kali muncul di 2025. Dibandingkan dengan nama posyandu
  2023–2024 di **kelurahan lain** (nama ternormalisasi, kunci T05):

  | Hasil | Jumlah |
  |---|---|
  | Nama sama persis dengan posyandu tahun sebelumnya di kelurahan lain | 16 (7 di antaranya ada pasangannya di kecamatan yang sama, 9 hanya di kecamatan lain) |
  | Nama mirip (kemiripan ≥ 0,85, `difflib`) dengan posyandu kelurahan lain | 16 |
  | Tanpa padanan nama sama sekali | 60 |

  Pada **semua 32** pasangan yang namanya sama/mirip, posyandu lama **masih melapor di 2025** di kelurahannya
  sendiri, dan tidak ada satu pun dari 395 posyandu 2024 yang hilang di 2025 (395 → 487 = 395 + 92). Jadi
  ke-32 itu adalah posyandu **berbeda yang kebetulan bernama sama/mirip** (nama umum seperti nama bunga), bukan
  posyandu yang pindah. Ke-92 posyandu diperlakukan sebagai posyandu baru (atau pemekaran/pendataan baru).
- **Akibat:** SCD tipe 2 hanya mencatat 13 perubahan puskesmas; perpindahan kecamatan yang disebut narasumber
  **tidak terlihat** di berkas laporan 2023–2026. Kemungkinan: perpindahan terjadi sebelum 2023, atau dilakukan
  dengan mengganti nama (sehingga tidak dapat dibedakan dari posyandu baru tanpa kode posyandu resmi). Ini batasan
  data (tidak ada kode posyandu), bukan kesalahan pipeline.

## Perubahan tahap 10: ML, dashboard, backup, uji end-to-end (dicatat 16 Nov 2026)

| No | Komponen | Rancangan UTS / sebelumnya | Implementasi UAS | Tanggal |
|---|---|---|---|---|
| P15 | Pemilihan model clustering | Metrik internal saja (Silhouette, DBI, elbow) | + syarat bisnis cluster terkecil ≥ 5% (K39); model tanpa syarat ditampilkan berdampingan | 16 Nov 2026 |
| P16 | Keluaran ML | `hasil_cluster` berisi nomor cluster | + nama cluster bagi dinas, tingkat prioritas, rekomendasi, peringkat & alasan per posyandu (K48, K49) | 16 Nov 2026 |
| P17 | Backup | `pg_dump` + mirror bucket, harian/mingguan | Sesuai rancangan, ditambah: dump metadata Airflow, manifest verifikasi (md5 isi tabel), retensi 7 harian + 4 mingguan, proses ulang dari raw zone (K54–K56) | 16 Nov 2026 |
| P18 | Uji end-to-end | Tidak dirinci | Stack terpisah dari volume kosong + uji pemulihan dengan berkas sumber dihapus (K57) | 16 Nov 2026 |

### P15 — syarat ukuran cluster adalah kebutuhan bisnis
- **Model terbaik menurut Silhouette** (K 3–6, stabil): `agglo_v1_internal_k3`, Silhouette 0,357, ukuran
  436 / 35 / 16. Cluster 436 posyandu (89,5%) berisi hampir semua posyandu dengan nilai mendekati rata-rata kota;
  dua cluster kecil hanya memisahkan pencilan (35 posyandu dengan 24% baris anomali, 16 posyandu dengan laporan
  tidak lengkap). Tanpa batas K sama sekali, yang tertinggi `agglo_v1_internal_k2` (0,494) bahkan 471 / 16.
- **Kenapa ditolak:** tujuan clustering adalah membagi posyandu menjadi kelompok yang masing-masing bisa diberi
  program berbeda. Satu kelompok berisi 9 dari 10 posyandu tidak membantu menentukan prioritas: dinas tetap harus
  memilah 436 posyandu secara manual. Silhouette tinggi di sini berasal dari jarak pencilan, bukan dari profil
  yang berbeda.
- **Jujur:** syarat ≥ 5% ditambahkan **setelah** percobaan pertama (K39). Syarat ini tidak dipilih untuk menaikkan
  metrik: model terpilih justru memiliki Silhouette **lebih rendah** (0,211 setelah K58; 0,208 sebelumnya). Yang dikorbankan metrik, yang
  diperoleh kegunaan. Kedua model ditampilkan berdampingan di `docs/bukti/26_ml_perbandingan.txt` bagian B–C.
- **Menjelaskan ke dosen:** "Metrik evaluasi clustering mengukur keterpisahan, bukan kegunaan. Kami memasang
  batas bisnis lebih dulu (cluster ≥ 5%, K 3–6, stabil), lalu memilih metrik terbaik di dalam batas itu."
