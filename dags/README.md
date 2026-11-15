# dags/

DAG Apache Airflow 3.3.2. Folder ini di-mount ke `/opt/airflow/dags`; kode di `ingestion/`, `etl/`,
`quality/`, `ml/` dapat di-import karena `PYTHONPATH=/opt/airflow/project`.

## `posyandu_pipeline.py`

```
init_schema
  └─> ingest_excel ─┐
      ingest_pdf ───┼─> load_staging (staging_laporan, staging_antrop, staging_kader, staging_pdf, staging_web)
      ingest_web ───┘        └─> transform ─> quality_gate ─┬─> load_dw
                                                             └─> quarantine
```

| Pengaturan | Nilai | Alasan |
|---|---|---|
| Jadwal | `0 2 10 * *` (tanggal 10, 02.00 WITA) | Laporan puskesmas masuk tanggal 5 (Panduan), sisanya untuk validasi (UTS 4.4) |
| `catchup` | `False` | Setiap run memuat ulang seluruh sumber; run mundur tidak perlu |
| `max_active_runs` | 1 | Staging/curated dimuat ulang penuh; dua run bersamaan akan saling menimpa |
| `retries` | 3, jeda 2 menit, exponential backoff (maks. 10 menit) | Error sesaat (portal, DB, data lake) |
| `on_failure_callback` | entri KRITIS di `dq.dq_log` | Monitoring kegagalan walaupun log Airflow dihapus |
| `batch_id` | `run_id` Airflow | Lineage: setiap baris stg/curated/dw membawa batch_id run-nya |

**Cabang quality gate:** task `quality_gate` (`@task.branch`) mengembalikan `load_dw` bila semua
pemeriksaan kritis sebelum load lulus, selain itu `quarantine`. `quarantine` mencatat batch di
`dq.quarantine` lalu sengaja gagal (retries 0) agar run terlihat gagal; DWH tidak diubah.
`load_dw` sendiri menjalankan Q04–Q06 di dalam transaksi dan ROLLBACK bila gagal.

Bukti eksekusi: `docs/bukti/16_run_dag_minggu2.txt` (run terjadwal `scheduled__2026-11-09T18:00:00+00:00`
dan run manual, keduanya sukses).

Tahap berikutnya menambahkan `build_mart → train_ml → refresh_dashboard` setelah `load_dw`.
