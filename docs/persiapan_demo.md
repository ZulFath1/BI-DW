# Persiapan demo UAS — pertanyaan yang mungkin muncul

Untuk pemilik (latihan demo). Semua angka berasal dari run pipeline dan uji yang tercatat di `docs/bukti/`.
Jawaban ditulis singkat; rujukan dalam kurung untuk membuka bukti saat ditanya.

## Urutan demo yang disarankan (± 10 menit)

1. `docker compose ps` → 6 layanan healthy (arsitektur).
2. Airflow (http://127.0.0.1:8081): DAG `posyandu_pipeline` (16 task, jadwal tanggal 10) dan `posyandu_backup`
   (harian). Trigger satu run → selesai ± 1 menit. Tunjukkan Graph dan run gagal (karantina, DB mati).
3. Konsol data lake (http://127.0.0.1:9001): bucket `raw` per `sumber/tanggal_ingest/`.
4. DWH: `sql/validasi/03_dwh.sql` atau DBeaver/psql: fakta & dimensi, PK/FK.
5. Dashboard (http://127.0.0.1:8501): ubah filter kecamatan → puskesmas → cluster; tab ML: daftar prioritas;
   kartu insight.
6. Bila ditanya DR: tunjukkan `docs/bukti/30_uji_pemulihan.txt` (jangan jalankan live; ± 3 menit).

## Business problem & data

| Pertanyaan | Jawaban |
|---|---|
| Masalah bisnisnya apa? | Dinkes punya laporan posyandu tiap bulan (Excel 305 kolom) tetapi tidak bisa cepat menjawab posyandu mana yang perlu dibina, alat apa yang kurang, dan angka mana yang tidak wajar. |
| Kenapa disebut Big Data, datanya kecil? | Volume memang kecil (±106 ribu baris fakta) dan kami akui. Yang ditekankan Variety (Excel header 3 tingkat, PDF tabel, web), Velocity batch bulanan/semesteran, Veracity (2.073 baris anomali, persen sebagai teks, tanpa kode posyandu), Value (prioritas posyandu), dan arsitektur yang skalabel. |
| Sumber eksternal untuk apa? | Penduduk 0–4 tahun (Disdukcapil, PDF) sebagai penyebut rasio stunting dan pembanding Sasaran; stunting per kelurahan (Satu Data, web). |
| Data pribadi kader? | Nama dipseudonimkan HMAC (kunci di `.env`), alamat tidak dimuat, usia jadi kelompok; Q06 memastikan tidak ada kolom nama/alamat di dw & mart. |

## Pipeline, data lake, DWH

| Pertanyaan | Jawaban |
|---|---|
| Kenapa ada data lake, tidak langsung ke database? | Berkas asli disimpan apa adanya (checksum, tidak pernah ditimpa) → bisa diaudit dan diproses ulang. Terbukti di uji pemulihan: berkas sumber dihapus, pipeline tetap membangun ulang DWH dari raw zone dengan hasil identik (K56, bukti 30). |
| Bagaimana kalau API/portal web gagal? | Retry dengan backoff; bila tetap gagal, fallback ke salinan manual di `data/source/eksternal/`; bila isi tabel sama dengan sebelumnya, status `tidak_berubah` (bukti 13, 21). |
| Database terputus? | Koneksi diulang 5× (2–16 detik), lalu retry Airflow 3× (backoff); callback mencatat ke outbox bila DB mati, dikirim saat pulih (bukti 23, 24). |
| Data corrupt? | Tanda tangan berkas salah → quarantine, task gagal. Data buruk (kunci ganda, persen teks) → quality gate kritis gagal → cabang `quarantine`, DWH tidak berubah (bukti 22). |
| Transform gagal di tengah? | Load DWH satu transaksi bersama Q04–Q06: gagal → ROLLBACK, dashboard tetap memakai data lama (bukti 19). |
| Grain fakta utama? | Posyandu × bulan × kelompok sasaran (106.020 baris = jumlah posyandu × 60 per tahun). |
| Kenapa SCD tipe 2? | Posyandu bisa pindah puskesmas; ada 13 perubahan puskesmas → 503 versi. Laporan lama tetap tercatat di puskesmas lama. |
| Galaxy schema? | 5 tabel fakta berbagi dimensi (posyandu, kelurahan, waktu, …). |

## Machine learning

| Pertanyaan | Jawaban |
|---|---|
| Kenapa clustering, bukan prediksi? | Tidak ada label "posyandu bermasalah"; tujuannya menemukan profil. Forecasting tidak layak (44 bulan, kategori berubah, 2026 belum lengkap). |
| Baseline apa? | Aturan manual 3 kelompok seperti cara dinas; Silhouette 0,069. |
| Improvement apa saja? | Rekayasa fitur (buang kelengkapan yang hampir konstan, log1p anomali), K = 2–8, K-Means vs Agglomerative, tanpa vs dengan fitur stunting: 45 model (bukti 26). |
| Silhouette cuma 0,21, jelek? | Struktur datanya memang lemah (kinerja posyandu kontinu). Tetap 3× baseline (0,211 vs 0,068), stabil (ARI antar seed 0,91), dan keenam cluster bisa dijelaskan. Kami tidak mengklaim klasifikasi pasti. |
| Kenapa tidak pilih Silhouette tertinggi (0,357)? | Model itu menaruh 436 dari 487 posyandu di satu cluster; tidak berguna untuk prioritas. Syarat cluster ≥ 5% adalah kebutuhan bisnis; model terpilih justru metriknya lebih rendah (P15). |
| Kenapa stunting tidak dipakai di model? | Sudah diuji: Silhouette lebih rendah (terbaik dengan stunting 0,189 vs 0,211) karena nilainya sama untuk semua posyandu sekelurahan. Tetap ditampilkan di dashboard. |
| Kenapa K = 6, tidak 5? | K = 4/5/6 Silhouette sama (0,211); aturan memilih DBI terkecil (K = 6). Secara isi K = 5 memunculkan cluster "mendekati rata-rata" (103 posyandu) yang tidak bisa ditindaklanjuti; K = 6 semua cluster terjelaskan (K50). |
| Ada kesalahan yang ditemukan sendiri? | Ya: (1) hasil cluster bergantung urutan surrogate key (ditemukan uji E2E, K51); (2) posyandu tanpa data kader dianggap "0 kader" dan naik ke puncak prioritas (ditemukan saat memeriksa dashboard, K58). Keduanya diperbaiki dan dicatat. |
| Hasilnya stabil? | Ya, setelah perbaikan urutan data (K51) hasil identik di 3 lingkungan; urutan berbeda pun memberi struktur yang sama (ARI 0,93). |
| Output ML dipakai untuk apa? | Daftar prioritas per posyandu (peringkat, alasan, rekomendasi) di dashboard, bisa diunduh CSV. |

## Dashboard & insight

| Pertanyaan | Jawaban |
|---|---|
| Dashboard membaca apa? | Hanya schema `mart` dengan user read-only; tidak membaca berkas mentah. |
| Bagaimana tahu data baru? | Task `refresh_dashboard` menulis batch_id baru; cache Streamlit dikunci batch_id. |
| Filter apa saja? | Tahun, kecamatan, puskesmas, cluster, kelompok sasaran; semua visual tab terkait ikut berubah. Pengecualian disebut di layar (model = data 2025; stunting = tingkat kelurahan). |
| Contoh insight? | Cakupan bayi balita 2025 58,7%; Sasaran balita posyandu Des 2025 hanya ±59% dari penduduk 0–4 tahun (23.299 dari 39.805) → banyak balita tidak terdata sebagai sasaran. |

## Backup, DR, performa

| Pertanyaan | Jawaban |
|---|---|
| Apa yang di-backup, ke mana, seberapa sering? | DWH + metadata Airflow (pg_dump) dan raw zone (mirror), harian 01.00 WITA, di `backup/output/` + bucket `backup`; retensi 7 harian + 4 mingguan (backup/README.md). |
| Sudah pernah diuji restore? | Ya, di stack kosong terpisah: restore + verifikasi 18 detik; 44 tabel, 23 isi tabel (md5), 17 objek cocok; pipeline ulang identik (bukti 30). |
| Kelemahan backup? | Masih satu laptop/disk; perlu salinan off-site. |
| Berapa lama pipeline? | ± 1 menit per run penuh (bukti 27); terlama: train_ml (45 model), staging PDF, transform. |
| Titik kegagalan tunggal? | Satu PostgreSQL menampung DWH dan metadata Airflow: bila mati, pipeline dan dashboard berhenti bersamaan (bisa dipulihkan dari backup). Pengembangan: pisahkan instance, replikasi/managed DB, executor terdistribusi, object storage multi-node. |
| Kalau datanya 100× lebih besar? | Raw zone S3 tetap; staging dimuat inkremental per batch/tahun (partisi), transform di SQL/Spark, fakta dipartisi per tahun, Airflow Celery/Kubernetes executor. |
| Kenapa Airflow 3.3 yang sudah EOL? | Patch matang, hanya lokal; upgrade 3.4 dicatat sebagai pengembangan lanjutan. |
