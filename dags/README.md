# dags/

DAG Apache Airflow 3.3.2. Folder ini di-mount ke `/opt/airflow/dags`; kode di `ingestion/`, `etl/`,
`quality/`, `ml/` dapat di-import karena `PYTHONPATH=/opt/airflow/project`.

## `posyandu_pipeline.py` (16 task)

```
init_schema
  └─> ingest_excel ─┐
      ingest_pdf ───┼─> load_staging (staging_laporan, staging_antrop, staging_kader, staging_pdf, staging_web)
      ingest_web ───┘        └─> transform ─> quality_gate ─┬─> load_dw ─> build_mart ─> train_ml ─> refresh_dashboard
                                                             └─> quarantine
```

| Pengaturan | Nilai | Alasan |
|---|---|---|
| Jadwal | `0 2 10 * *` zona Asia/Makassar (tanggal 10, 02.00 WITA); DAG **aktif** | Laporan puskesmas masuk tanggal 5 (Panduan), sisanya untuk validasi (UTS 4.4) |
| `catchup` | `False` | Setiap run memuat ulang seluruh sumber; run mundur tidak perlu |
| `max_active_runs` | 1 | Staging/curated dimuat ulang penuh; dua run bersamaan akan saling menimpa |
| `retries` | 3, jeda 2 menit, exponential backoff (maks. 10 menit) | Error sesaat (portal, DB, data lake) |
| `on_retry_callback` | `dq.batch_log` (tahap `orkestrasi`) + `dq.dq_log` PERINGATAN | Setiap percobaan gagal terlihat |
| `on_failure_callback` | `dq.batch_log` (tahap `orkestrasi`) + `dq.dq_log` KRITIS | Monitoring kegagalan walaupun log Airflow dihapus |
| Outbox callback | `/opt/airflow/logs/dq_outbox/*.json` → dikirim `init_schema` run berikutnya | Bila DB DWH sendiri yang mati (K44) |
| Parameter `skenario_uji` | `normal` (bawaan) / `data_buruk` | Hanya untuk uji kegagalan; membaca salinan uji `data/uji/` (K45) |
| `batch_id` | `run_id` Airflow | Lineage: setiap baris stg/curated/dw/mart membawa batch_id run-nya |

**Cabang quality gate:** task `quality_gate` (`@task.branch`) mengembalikan `load_dw` bila semua
pemeriksaan kritis sebelum load lulus, selain itu `quarantine`. `quarantine` mencatat batch di
`dq.quarantine` lalu sengaja gagal (retries 0) agar run terlihat gagal; DWH tidak diubah, dan
`build_mart`/`train_ml`/`refresh_dashboard` dilewati sehingga dashboard tetap menampilkan data terakhir yang sah.
`load_dw` sendiri menjalankan Q04–Q06 di dalam transaksi dan ROLLBACK bila gagal.

**Task setelah load:** `build_mart` (dw → mart, satu transaksi), `train_ml` (clustering, hasil ke mart),
`refresh_dashboard` (penanda batch baru untuk cache Streamlit + cek health endpoint dashboard).

Bukti eksekusi: `docs/bukti/16_run_dag_minggu2.txt`, `22–24_uji_dag_*.txt` (uji kegagalan nyata),
screenshot `30–36_airflow_*.png`.
