# Bukti eksekusi

Semua berkas di folder ini sudah diperiksa: tidak memuat nama/alamat kader, `kader_kode`, isi `.env`,
kata sandi, atau token. Data yang tampil hanya agregat dan nama wilayah/fasilitas dari data terbuka.

| Berkas | Tahap | Yang dibuktikan | Untuk laporan/slide |
|---|---|---|---|
| `01_versi_tools.png`, `01_compose_ps.txt`, `01_docker_stats.txt`, `01_cek_schema_role.txt` | 1 Infrastruktur | Versi tools, layanan healthy, pemakaian sumber daya, schema & role | Arsitektur |
| `02_docker_desktop_wsl2.png`, `03_docker_resources.png`, `04_wsl_status.png`, `05_ruang_disk.png` | 1 Infrastruktur | Lingkungan Docker/WSL 2 dan ruang disk | Arsitektur, scalability |
| `excel_daftar_sheet.png`, `09_pdf_tabel8_12.png` | Sumber data | Struktur sheet Excel; tabel PDF E1 | Dataset |
| `10_run_pipeline_minggu1.txt` | 2–3 Ingestion & staging | Run CLI ingestion → staging, batch_log | Pipeline |
| `11_validasi_staging.txt` | 3 Staging | Validasi V01–V11 (jumlah baris, rekonsiliasi, privasi) | Pipeline, data quality |
| `12_uji_view_rasio.txt` | 4 Schema DWH | Uji view rasio stunting (ROLLBACK) | DWH |
| `13_uji_error_handling.txt` | 2–3 | Uji E1–E5 (DB terputus, web gagal, fallback, korup, struktur) | Error handling |
| `14_schema_dw.txt` | 4 Schema DWH | Tabel, PK/FK, grain | DWH |
| `15_isi_raw_zone.txt` | 2 Data lake | Daftar objek raw zone (nama & ukuran, tanpa isi) | Data lake |
| `16_run_dag_minggu2.txt` | 7 Orkestrasi | DAG run terjadwal & manual sukses; status per task; batch_log | Pipeline & orkestrasi |
| `17_transform_ringkasan.txt` | 5 Transform | Ringkasan dq_log T02–T18, K16 | Pipeline, data quality |
| `18_quality_gate.txt` | 6 Quality gate | Hasil Q01–Q09 | Data quality |
| `19_uji_quality_gate.txt` | 6 Quality gate | Uji E6 rollback, E7 quarantine, E8 idempotensi; uji ulang E1–E5 | Error handling |
| `20_validasi_dwh.txt` | 6 Load DWH | DWH vs angka acuan (D01–D13) | DWH, evaluasi |
| `21_ingest_web_tidak_berubah.txt` | 2 Ingestion | Status `tidak_berubah` dari checksum isi tabel | Data lake, velocity |

| `22_uji_dag_data_buruk_karantina.txt` | 8 Uji kegagalan (b) | DAG run nyata dengan salinan uji buruk: Q02 kritis gagal → `quarantine`, DWH tidak berubah (md5 sama) | Error handling, data quality |
| `23_uji_dag_db_mati_retry_callback.txt` | 8 Uji kegagalan (a) | DB DWH tidak menerima koneksi → retry kode (5×) + retry Airflow (4 percobaan) → gagal; callback retry/gagal ke outbox lalu `dq.batch_log` | Error handling |
| `24_uji_dag_pulih_rerun_sukses.txt` | 8 Uji kegagalan (c) | Rerun setelah DB pulih: 16 task sukses, outbox terkirim | Error handling, DR |
| `25_validasi_mart.txt` | 9 Data mart & ML | Mart vs angka acuan (M01–M09), evaluasi model | DWH/mart, ML, evaluasi |
| `26_ml_perbandingan.txt` | 10 ML | 45 model (baseline, K-Means & Agglomerative K2–8, 3 set fitur); terpilih vs terbaik tanpa syarat ukuran; profil & rekomendasi 6 cluster; uji K = 4/5 (`python -m ml.laporan`) | ML, evaluasi |
| `27_performa_skalabilitas.txt` | 10 Performa | Durasi tiap task run sukses terakhir; ukuran data per layer (raw, stg, curated, dq, dw, mart, metadata, backup) | Evaluasi & scalability |
| `28_backup.txt` | 10 Backup | DAG `posyandu_backup`: dump + manifest, mirror inkremental, retensi; 2 percobaan gagal + callback retry tercatat | Disaster recovery |
| `29_uji_e2e_bersih.txt` | 10 Uji E2E | Stack baru volume kosong → run terjadwal 16 task sukses, Q01–Q09 lulus, validasi identik dengan stack utama, dashboard OK (`tests/uji_e2e_bersih.sh`) | Pipeline, evaluasi |
| `30_uji_pemulihan.txt` | 10 Uji DR | Stack kosong tanpa berkas sumber → restore + verifikasi (RTO 18 detik) → pipeline ulang dari raw zone, hasil identik (`tests/uji_pemulihan.sh`) | Disaster recovery |
| `30_airflow_daftar_dag.png`, `33_airflow_daftar_run.png` | 7–8 Orkestrasi | DAG aktif, daftar run sukses/gagal | Pipeline & orkestrasi |
| `31_airflow_graph.png`, `32_airflow_grid_run_sukses.png` | 7–9 Orkestrasi | Graph 16 task; grid run sukses | Pipeline & orkestrasi |
| `34_airflow_run_gagal_db_retry.png`, `35_airflow_retry_init_schema.png` | 8 Uji (a) | Run gagal, `init_schema` 4 percobaan (Task Tries 1–4) | Error handling |
| `34_airflow_run_gagal_karantina.png` | 8 Uji (b) | Cabang `quarantine` gagal, `load_dw` dilewati | Error handling |
| `36_airflow_detail_dag_jadwal.png` | 7 Orkestrasi | Jadwal `0 2 10 * *`, next run 2026-12-09 18:00 UTC = 10 Des 02.00 WITA, catchup false | Pipeline & orkestrasi |
| `37_lake_daftar_bucket.png`, `38_lake_raw_zone.png`, `39_lake_raw_laporan.png` | 2 Data lake | Bucket raw & backup (versioning aktif), folder per sumber/tanggal, awalan `uji/` | Data lake |
| `40–44_dashboard_*.png` | 9 Dashboard | Lima tab dashboard (KPI & insight, tren, ML, stunting, kualitas) | Dashboard, insight |

Screenshot diambil oleh `ambil_screenshot.py` di container Playwright (`docker compose --profile bukti run
--rm screenshot airflow|lake|dashboard`) dan sudah diperiksa satu per satu (16 Nov 2026): hanya memuat nama
task/run/bucket/folder, angka agregat, serta nama posyandu & kelurahan (nama fasilitas, bukan data pribadi).
Tidak ada nama/alamat kader, kata sandi, token, atau isi `.env`. Waktu di UI Airflow ditampilkan dalam UTC.
