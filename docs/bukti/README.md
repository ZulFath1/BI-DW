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

Belum ada: screenshot UI Airflow (grid/graph DAG) dan konsol data lake — dijadwalkan pada tahap uji
end-to-end (7–13 Des) memakai skrip `docs/bukti/ambil_screenshot.py` di container Playwright (K35).
