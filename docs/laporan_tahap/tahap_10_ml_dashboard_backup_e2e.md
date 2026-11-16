# Laporan Tahap 10 — Tindak lanjut ML & dashboard, performa, backup/restore, uji end-to-end

- **Tanggal:** 16 Nov 2026 · **Jadwal:** target 7–13 Des (dikerjakan lebih awal) · **Mode:** otonom
- **Status:** selesai dan dites. Run acuan: `final2_tahap10__20261116` (sukses, 63 detik).
- Keputusan: K48–K58 (`docs/KEPUTUSAN.md`). Penyimpangan dari UTS: P15–P18 (`docs/PERUBAHAN_DARI_UTS.md`).

## 1. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `ml/clustering.py` | nama cluster bagi dinas (aturan z-score), tingkat prioritas, rekomendasi; skor, peringkat & alasan prioritas per posyandu; data diurutkan `posyandu_id` (K51); kader 0 = data tidak ada (K58) |
| `ml/laporan.py` (baru) | tabel evaluasi lengkap dari database (bukti 26) |
| `sql/ddl/07_mart.sql`, `etl/build_mart.py` | kolom interpretasi ML; tabel baru `mart.kunjungan_posyandu_bulanan` (grain fakta, untuk filter) |
| `dashboard/app.py` | ditulis ulang: filter tahun/kecamatan/puskesmas/cluster/kelompok di semua visual terkait, daftar prioritas + unduh CSV, kelengkapan per bulan, kartu insight 5 bagian, anomali per kecamatan/puskesmas |
| `backup/backup.py`, `backup/restore.py`, `backup/__init__.py` (baru), `backup/README.md` | backup DB + mirror raw zone + retensi; restore + verifikasi |
| `dags/posyandu_backup.py` (baru), `dags/README.md` | DAG backup harian 01.00 WITA |
| `ingestion/ingest_files.py`, `sql/ddl/01_dq.sql` | fallback ke raw zone bila berkas sumber tidak ada (ING02, `cara_ambil = 'raw_zone'`) |
| `docker/airflow/Dockerfile`, `docker-compose.yml` | klien PostgreSQL 16; mount `backup/`; env `BACKUP_*` |
| `docker/uji/compose.uji.yml` (baru) | override stack uji terpisah (tanpa port, backup read-only, sumber bisa dikosongkan) |
| `tests/uji_e2e_bersih.sh`, `tests/uji_pemulihan.sh`, `tests/ukur_performa.py` (baru), `tests/status_run.py` | uji otomatis + pengukuran |
| `sql/validasi/04_mart.sql` | M10 (rekonsiliasi tabel mart baru), M11 (daftar prioritas) |
| `docs/persiapan_demo.md` (baru), README, `ml/README.md`, `docs/bukti/README.md` | dokumentasi |

## 2. Machine learning

### 2.1 Perbandingan lengkap
45 model (3 set fitur × [baseline aturan + K-Means K2–8 + Agglomerative K2–8]) ada di
`docs/bukti/26_ml_perbandingan.txt` bagian A. Ringkasan berdampingan (bagian B):

| | Baseline aturan | **K-Means v2 K=6 (terpilih)** | Agglo v1 K=3 (terbaik tanpa syarat ukuran) | Agglo v1 K=2 (terbaik mutlak) | K-Means v2+stunting K=5 | Agglo v2 K=6 |
|---|---|---|---|---|---|---|
| Silhouette ↑ | 0,068 | **0,211** | 0,357 | 0,494 | 0,189 | 0,173 |
| Davies-Bouldin ↓ | 2,188 | **1,274** | 0,992 | 0,785 | 1,553 | 1,448 |
| Calinski-Harabasz ↑ | 50 | **102** | 88 | 81 | 83 | 79 |
| ARI antar seed | – | 0,91 | – | – | 0,95 | – |
| Ukuran cluster | 307/126/54 | 121/117/88/71/59/31 | 436/35/16 | 471/16 | 150/128/100/69/40 | 140/105/95/75/42/30 |
| Cluster terbesar | 63,0% | 24,8% | 89,5% | 96,7% | 30,8% | 28,7% |

**Syarat "cluster terkecil ≥ 5%" adalah kebutuhan bisnis, bukan penyesuaian demi metrik.** Model tanpa syarat
menaruh 436 dari 487 posyandu (89,5%) di satu cluster: isinya posyandu dengan nilai mendekati rata-rata kota,
sedangkan dua cluster kecilnya hanya pencilan (35 posyandu dengan 24% baris anomali; 16 posyandu dengan laporan
tidak lengkap). Untuk menentukan prioritas, dinas tetap harus memilah 436 posyandu secara manual. Model terpilih
justru ber-Silhouette lebih rendah; syarat ini mengorbankan metrik demi kegunaan (P15). Calinski-Harabasz,
yang lebih memperhitungkan pembagian seimbang, malah tertinggi pada model terpilih.

### 2.2 Profil 6 cluster (rata-rata skala asli, run `final2_tahap10__20261116`)

| Cluster | Posyandu | Prioritas | % ibu hamil | % bayi balita | % baris anomali | Alat std baik | Kader | % kader terlatih |
|---|---|---|---|---|---|---|---|---|
| K1 Cakupan rendah | 121 | Tinggi | 35,4 | 44,6 | 0,5 | 4,2/5 | 5,0 | 43,6 |
| K2 Kader banyak, cakupan rendah | 31 | Tinggi | 65,0 | 49,5 | 0,5 | 2,9/5 | 8,1 | 49,7 |
| K3 Kekurangan alat antropometri | 59 | Sedang | 59,3 | 57,8 | 1,7 | 0,8/5 | 5,1 | 65,8 |
| K4 Pencatatan perlu diverifikasi | 71 | Sedang | 70,8 | 69,2 | 16,3 | 4,1/5 | 5,2 | 39,3 |
| K5 Cakupan baik, kader belum terlatih | 117 | Sedang | 75,2 | 71,0 | 0,4 | 4,0/5 | 5,1 | 8,7 |
| K6 Kinerja baik (percontohan) | 88 | Rendah | 76,2 | 67,5 | 0,2 | 4,1/5 | 5,0 | 89,8 |
| Kota | 487 | | 62,3 | 60,6 | 2,9 | 3,6/5 | 5,3 | 46,0 |

| Cluster | Karakter (satu kalimat) | Rekomendasi tindakan |
|---|---|---|
| K1 | Cakupan ibu hamil 35% dan bayi balita 45%, jauh di bawah kota, padahal alat & kader tidak jauh dari rata-rata | sweeping/kunjungan rumah, pengingat jadwal, koordinasi bidan kelurahan; cek kewajaran Sasaran |
| K2 | Rata-rata 8,1 kader (kota 5,3) tetapi cakupan bayi balita hanya 49% | supervisi pembagian tugas & jadwal kader, kunjungan rumah, evaluasi lokasi/jam buka |
| K3 | Hanya 0,8 dari 5 jenis alat standar baik (kota 3,6), cakupan mendekati rata-rata | pengadaan/penggantian alat standar lebih dulu di cluster ini; kalibrasi |
| K4 | 16,3% baris laporan anomali (kota 2,9%), cakupannya belum bisa dipercaya penuh | verifikasi Sasaran & Cakupan oleh puskesmas; pendampingan pengisian laporan |
| K5 | Cakupan di atas kota (75% / 71%) tetapi baru 9% kader terlatih | pelatihan 25 keterampilan bertahap |
| K6 | Semua indikator di atas/mendekati kota, 90% kader terlatih | percontohan & mentor; pemantauan rutin |

Nama dibuat oleh aturan eksplisit atas z-score (K48), sehingga tetap benar bila nomor cluster berubah.

### 2.3 Apakah K lebih kecil lebih tepat? (bukti 26 bagian E)
Tidak. K = 4, 5, 6 sama-sama Silhouette 0,211; aturan K39 memilih DBI terkecil (K = 6: 1,274; K = 5: 1,349;
K = 4: 1,514). Secara isi:
- K = 5: cluster alat hilang (59 posyandu tersebar) dan muncul cluster **"Mendekati rata-rata kota" (103
  posyandu)** yang tidak bisa diberi tindakan khusus.
- K = 4: cluster alat juga hilang, dan cluster "Kinerja baik" mencampur posyandu dengan kader terlatih 90%
  dan 9% (rata-rata 40%), sehingga program pelatihan tidak bisa diarahkan.
- Cluster paling lemah di K = 6 adalah **K2 (31 posyandu, 6,4%)**: pembedanya jumlah kader yang dihitung dari
  baris data kader (K22). Perlu dicek lapangan sebelum dipakai untuk kebijakan.

### 2.4 Dua kesalahan yang ditemukan dan diperbaiki pada tahap ini
1. **Reprodusibilitas (K51).** Uji E2E menunjukkan hasil cluster berbeda di database bersih (85/128/118/57/68/31)
   walau datanya sama, karena data diurutkan menurut surrogate key (477 dari 487 posisi berbeda). Kini diurutkan
   `posyandu_id`: label identik di stack utama, E2E, dan pemulihan. Solusi alternatif tadi punya 6 nama cluster
   yang sama dan ARI 0,928, artinya struktur cluster kokoh dan hanya posyandu di batas yang berpindah.
2. **Kader 0 (K58).** Saat memeriksa screenshot, 4 dari 10 posyandu teratas di daftar prioritas beralasan
   "0 kader". Ternyata 5 posyandu tidak punya baris di data kader. Kini dianggap data kosong (imputasi median).
   Akibatnya: Silhouette 0,208 → 0,211, ukuran 120/117/90/71/59/30 → 121/117/88/71/59/31, nama cluster sama.

## 3. Dashboard

| Tab | Filter yang berlaku | Isi |
|---|---|---|
| Ringkasan & insight | semua | 5 KPI, cakupan per kecamatan (per puskesmas bila satu kecamatan/puskesmas dipilih), 5 kartu insight |
| Tren cakupan | semua | cakupan per bulan per kecamatan; kelengkapan laporan per bulan |
| Posyandu prioritas (ML) | kecamatan, puskesmas, cluster (tahun tidak: model = data 2025) | daftar prioritas (tingkat, top-N, unduh CSV), jumlah per cluster, heatmap profil sesuai filter, karakter & rekomendasi, evaluasi model |
| Stunting kelurahan | tahun, kecamatan, puskesmas (cluster tidak: tingkat kelurahan) | rasio stunting per kelurahan |
| Kualitas data | semua kecuali quality gate (tingkat batch) | anomali per jenis, status data, anomali per kecamatan/puskesmas |

Insight memakai pola Data → Temuan → Interpretasi → Impact → Rekomendasi, dan dihitung ulang dari data sesuai
filter. Dengan filter bawaan (2025, semua wilayah):
1. Cakupan bayi balita 58,7% (ibu hamil 56,6%); terendah Banjarmasin Timur 52,4%, tertinggi Selatan 67,1%.
2. Sasaran bayi balita Desember 2025 = 23.299 = 59% dari 39.805 penduduk 0–4 tahun.
3. 335 dari 490 posyandu punya alat rusak/tidak standar; hanya 164 lengkap 5 jenis.
4. 826 baris laporan 2025 bertanda anomali; per kecamatan tertinggi Selatan (5,9% baris Terlapor) dan Utara
   (5,5%); per puskesmas Kayu Tangi 24,4%, Mantuil 17,7%, Pekauman 10,3%.
5. 45,9% kader terlatih (1.168 dari 2.542); 31 posyandu < 5 kader; 117 posyandu cakupan baik tetapi kader hampir semua belum terlatih.

Uji (Streamlit AppTest, 5 kombinasi filter): tanpa exception; KPI bawaan = angka acuan. Screenshot `40–44`.

## 4. Performa dan skalabilitas (bukti 27)

**Durasi run sukses terakhir:** 63 detik. Terlama: `train_ml` 18,8 detik (45 model + stabilitas 10 seed),
`staging_pdf` 16,5 detik (ekstraksi tabel PDF), `staging_laporan` 10,0 detik, `transform` 8,9 detik. Delapan run
sukses tercatat: min 39, median 57, maks 81 detik. Run sebelum 16 Nov memakai kode yang lebih awal, jadi angkanya
tidak sepenuhnya sebanding.

**Ukuran data per layer:**

| Layer | Objek/tabel | Baris | Ukuran |
|---|---|---|---|
| Raw zone (bucket raw) | 17 objek | – | 26,95 MB (2,20 MB objek uji) |
| stg | 5 | 110.154 | 44,20 MB |
| curated | 8 | 120.587 | 41,42 MB |
| dq | 5 | ±1.500 | 0,73 MB |
| dw | 12 | 115.871 | 37,31 MB |
| mart | 11 | 108.685 | 43,68 MB |
| Database DWH total | | | 176,37 MB |
| Metadata Airflow | | | 11,62 MB |
| Backup lokal | 3 dump + mirror | | 36,78 MB |

Mart lebih besar dari dw karena `kunjungan_posyandu_bulanan` menyimpan teks (kecamatan, puskesmas, status) per
baris demi kemudahan filter. Untuk volume besar sebaiknya diganti kunci + dimensi.

### 4.1 Titik kegagalan tunggal (single point of failure)

| Komponen | Kenapa tunggal | Dampak bila mati | Mitigasi sekarang | Pengembangan |
|---|---|---|---|---|
| **PostgreSQL** (satu container menampung DWH **dan** metadata Airflow) | satu proses, satu volume | pipeline berhenti (scheduler tidak bisa menyimpan status), dashboard kosong, callback tidak bisa menulis | retry koneksi + retry Airflow, outbox callback, backup harian + restore teruji (RTO 18 detik) | pisahkan instance metadata Airflow dan DWH; replika streaming / managed PostgreSQL dengan failover; PgBouncer |
| Data lake (Silo satu node) | satu disk/volume | ingestion gagal; raw zone hilang bila volume rusak | versioning bucket, mirror ke `backup/output` | mode terdistribusi (erasure coding, ≥ 4 node) atau object storage cloud |
| Airflow scheduler + LocalExecutor | satu scheduler, task di container yang sama | jadwal tidak berjalan (data tetap aman) | `restart: unless-stopped`, healthcheck | ≥ 2 scheduler (Airflow mendukung HA), Celery/Kubernetes executor |
| Laptop (semua layanan + backup di satu disk D:) | satu mesin | semua berhenti; backup ikut hilang bila disk rusak | backup terpisah dari volume Docker | salinan off-site otomatis (**butuh tindakan pemilik**) |

### 4.2 Bila data jauh lebih besar
Raw zone S3 tidak perlu diubah. Staging dimuat inkremental per tahun/batch (sekarang muat ulang penuh). Fakta
dipartisi per tahun. Transform berat dipindah ke SQL di database atau Spark. Airflow memakai Celery/Kubernetes
executor. ML tetap kecil karena grain-nya posyandu (±500 baris).

## 5. Backup & pemulihan (bukti 28, 30)

- **DAG `posyandu_backup`** aktif setiap hari 01.00 WITA. Tiga run sukses. Run pertama gagal 2× karena Airflow 3
  menyembunyikan string koneksi metadata dari task (K55); kegagalan dan callback retry tercatat di
  `dq.batch_log`, lalu sukses setelah diperbaiki. Mirror kedua dan ketiga inkremental (0 objek disalin ulang).
- **Uji pemulihan** (`tests/uji_pemulihan.sh`): stack kosong **tanpa berkas sumber**. Restore + verifikasi
  selesai dalam **18 detik** (44 tabel jumlah baris, 23 tabel dw/mart isi md5, 17 objek sha256 cocok). Riwayat
  Airflow dan dashboard kembali. Pipeline ulang mengambil Excel & PDF dari raw zone (ING02), Q01–Q09 lulus, dan
  hasil validasi DWH & mart **identik** dengan stack utama. Total 172 detik.
- RPO maksimal 24 jam (backup harian). Kelemahan: semua salinan masih di satu laptop.

## 6. Uji end-to-end dari kondisi bersih (bukti 29)

`tests/uji_e2e_bersih.sh`: stack `posyandu_e2e` dengan volume baru (0 tabel dw/mart). DAG diaktifkan, lalu
scheduler membuat run terjadwal sendiri. 16 task sukses, Q01–Q09 lulus, total **146 detik** dari stack kosong.
Keluaran `sql/validasi/03_dwh.sql` (146 baris) dan `04_mart.sql` (122 baris) **0 baris berbeda** dari stack
utama, termasuk profil cluster. Dashboard tanpa exception dengan KPI = angka acuan. Stack uji dihapus otomatis.

Kecocokan angka acuan 4.6 (dari validasi yang identik di ketiga stack): cakupan bayi balita 58,72%, ibu hamil
56,59%, kelengkapan Des 2025 475/487, anomali 2.073 (2025: 826), sasaran Des 2025 kota 23.299, kader < 5 31/482
(P07), alat 335/490 dan 164, rasio stunting 3,85%. Semuanya ✔.

## 7. Bukti

`docs/bukti/26_ml_perbandingan.txt`, `27_performa_skalabilitas.txt`, `28_backup.txt`, `29_uji_e2e_bersih.txt`,
`30_uji_pemulihan.txt`, screenshot `30–36_airflow_*.png` dan `40–44_dashboard_*.png` (diambil ulang
16 Nov 2026). Semua sudah diperiksa: hanya nama task/run/fasilitas dan angka agregat. Tidak ada data kader,
kata sandi, atau isi `.env`.

## 8. Penjelasan kode untuk demo

- `kategori_bisnis()` di `ml/clustering.py`: memberi nama cluster dengan memeriksa z-score rata-rata cluster
  berurutan (anomali → alat → kader+cakupan → cakupan → pelatihan → semua baik). `prioritas_posyandu()`: skor
  = rata-rata z-score tiap posyandu dengan arah dibalik; peringkat = tingkat cluster lalu skor.
- `backup_database()`: hitung isi tabel → `pg_dump -Fc` → `pg_restore --list` (verifikasi) → SHA-256 → salin ke
  bucket. Bila gagal di tengah, folder dihapus agar tidak dikira backup sah.
- `restore.py semua`: cek SHA-256 dump → `pg_restore --clean --if-exists --single-transaction` → unggah ulang
  objek dengan metadata asli → bandingkan dengan manifest.
- `_dari_raw_zone()` di `ingestion/ingest_files.py`: bila berkas lokal tidak ada, ambil objek terbaru di raw zone,
  cocokkan SHA-256, catat manifest `raw_zone` + PERINGATAN ING02.

**Pertanyaan dosen yang mungkin muncul** (lengkap di `docs/persiapan_demo.md`):
1. *Kenapa tidak memilih Silhouette tertinggi?* Lihat 2.1: 89,5% posyandu di satu cluster tidak bisa dipakai
   untuk prioritas. Model terpilih justru metriknya lebih rendah.
2. *Apa yang terjadi kalau server hilang?* Restore 18 detik dari backup, dan pipeline bisa dibangun ulang dari
   raw zone walau berkas sumber hilang (bukti 30).
3. *Apa titik lemah arsitektur?* PostgreSQL tunggal untuk DWH + metadata Airflow (4.1), dan backup masih satu
   laptop.
4. *Bagaimana yakin hasilnya bisa diulang?* Uji E2E dari volume kosong menghasilkan keluaran validasi identik,
   termasuk ML (setelah K51).
5. *Apakah daftar prioritas pasti benar?* Tidak. Ini alat bantu (Silhouette 0,21); alasannya ditampilkan agar
   puskesmas bisa memverifikasi.

## 9. Risiko / kendala & butuh tindakan pemilik

- **Butuh tindakan pemilik:** salin `backup/output/` ke penyimpanan lain (drive eksternal/cloud) secara berkala.
  Berisi data privat, jadi **jangan ke repo publik**.
- Backup hanya berjalan saat laptop dan Docker menyala pada pukul 01.00. Bila terlewat, catchup=False sehingga
  hanya interval terakhir yang dijalankan saat DAG aktif kembali.
- Pemeriksaan keamanan otomatis untuk perintah shell (alat Claude Code) sempat gagal berulang di sesi ini. Tidak
  berdampak pada hasil, tetapi beberapa langkah tertunda.
- Anomaly detection (opsional) belum dikerjakan.
- Bahan laporan PDF & PPT: kerangka di `docs/bahan_laporan_ppt.md`.

## 10. Untuk pemilik (git)

**Berkas baru:**
```
backup/__init__.py  backup/backup.py  backup/restore.py
dags/posyandu_backup.py
docker/uji/compose.uji.yml
ml/laporan.py
tests/uji_e2e_bersih.sh  tests/uji_pemulihan.sh  tests/ukur_performa.py
docs/persiapan_demo.md  docs/bahan_laporan_ppt.md
docs/laporan_tahap/tahap_10_ml_dashboard_backup_e2e.md
docs/bukti/26_ml_perbandingan.txt  27_performa_skalabilitas.txt  28_backup.txt
docs/bukti/29_uji_e2e_bersih.txt  30_uji_pemulihan.txt
```
**Berkas diubah:**
```
README.md  docker-compose.yml  docker/airflow/Dockerfile
backup/README.md  dags/README.md  ml/README.md  ml/clustering.py
dashboard/app.py  etl/build_mart.py  ingestion/ingest_files.py
sql/ddl/01_dq.sql  sql/ddl/07_mart.sql  sql/validasi/04_mart.sql  tests/status_run.py
docs/KEPUTUSAN.md  docs/PERUBAHAN_DARI_UTS.md  docs/bukti/README.md
docs/bukti/30–36_airflow_*.png  docs/bukti/40–44_dashboard_*.png   (diambil ulang)
```

**Usulan pesan commit:**
```
Tahap 10: interpretasi & prioritas ML, filter dashboard, backup/restore, uji E2E dan pemulihan
```

**Pemeriksaan berkas sensitif (tanpa git, 16 Nov 2026):**
- Nilai rahasia dari `.env` (kata sandi, kunci) dicari di semua berkas project di luar `data/`, `backup/output/`,
  `.env`, `CLAUDE.md`: **0 ditemukan**.
- Berkas data (`*.dump`, `*.xlsx`, `*.pdf`, `*.html` raw) hanya ada di `backup/output/` (diabaikan
  `backup/output/`) dan `data/` (diabaikan `data/*`). `etl/pemetaan_posyandu_manual.csv` tetap diabaikan (`*.csv`).
- Folder baru `data/kosong/` (untuk uji pemulihan) ada di bawah `data/*` sehingga diabaikan.
- Screenshot sudah diperiksa satu per satu (bagian 7).

**Perintah untuk pemilik:**
```bash
git status                 # pastikan tidak ada backup/output/, data/, .env
git add <berkas di atas>
git diff --cached --stat
git commit -m "Tahap 10: interpretasi & prioritas ML, filter dashboard, backup/restore, uji E2E dan pemulihan"
git push
```
