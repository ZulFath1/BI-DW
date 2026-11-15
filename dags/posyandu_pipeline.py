"""DAG utama pipeline posyandu (Apache Airflow 3.3.2).

    init_schema
        └─> ingest_excel ─┐
            ingest_pdf ───┼─> load_staging (5 task) ─> transform ─> quality_gate ─┬─> load_dw
            ingest_web ───┘                                                          └─> quarantine

Jadwal: tanggal 10 setiap bulan pukul 02.00 WITA (laporan puskesmas masuk tanggal 5 menurut
Panduan, sisanya untuk validasi; dokumen UTS 4.4). catchup=False: run yang terlewat tidak
dijalankan mundur karena setiap run memuat ulang seluruh data sumber.

Error handling:
  - retries=3 dengan jeda bertambah (exponential backoff, 2 -> 4 -> 8 menit) untuk error sesaat
    (portal web, database terputus, data lake);
  - setiap task menulis status BERJALAN/SUKSES/GAGAL ke dq.batch_log (common.batch.catat_task);
  - on_failure_callback menulis entri KRITIS ke dq.dq_log (task, percobaan, jenis error) sehingga
    kegagalan terlihat di monitoring walaupun log Airflow dihapus;
  - quality gate kritis gagal -> cabang quarantine (DWH tidak diubah, DAG run berstatus gagal);
  - pemeriksaan kritis saat load gagal -> ROLLBACK di dalam load_dw.
batch_id = run_id Airflow, sehingga setiap baris staging/curated/dw dapat ditelusuri ke run-nya.

Tahap berikutnya (minggu 30 Nov–6 Des) menambahkan build_mart -> train_ml -> refresh_dashboard
setelah load_dw.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

log = logging.getLogger(__name__)
ZONA = pendulum.timezone("Asia/Makassar")


def _ctx():
    c = get_current_context()
    return c["run_id"], c["ti"].try_number


def catat_kegagalan(context) -> None:
    """on_failure_callback: dipanggil Airflow setelah task gagal pada percobaan terakhir."""
    try:
        from common.batch import tulis_dq_log_mandiri

        ti = context["ti"]
        err = context.get("exception")
        tulis_dq_log_mandiri(context["run_id"], "orkestrasi", [{
            "kode_aturan": "AIRFLOW", "tingkat": "KRITIS", "objek": f"{ti.dag_id}.{ti.task_id}",
            "nilai": str(ti.try_number),
            "keterangan": f"task gagal setelah {ti.try_number} percobaan: {type(err).__name__}: {err}"[:2000],
        }])
    except Exception:  # callback tidak boleh menutupi error aslinya
        log.exception("on_failure_callback gagal menulis dq_log")


ARG_BAWAAN = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": catat_kegagalan,
}


@dag(
    dag_id="posyandu_pipeline",
    description="Ingestion -> data lake -> staging -> transform -> quality gate -> DWH (posyandu Banjarmasin)",
    schedule="0 2 10 * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,
    default_args=ARG_BAWAAN,
    tags=["posyandu", "uas-bigdata"],
    doc_md=__doc__,
)
def posyandu_pipeline():
    @task
    def init_schema():
        from etl.schema import init_schema as f
        f(_ctx()[0])

    @task
    def ingest_excel():
        from ingestion.ingest_files import ingest_excel as f
        return f(*_ctx())

    @task
    def ingest_pdf():
        from ingestion.ingest_files import ingest_pdf as f
        return f(*_ctx())

    @task
    def ingest_web():
        from ingestion.ingest_web import ingest_stunting as f
        b, p = _ctx()
        return {str(k): v for k, v in f(b, percobaan_ke=p).items()}

    @task_group(group_id="load_staging")
    def load_staging():
        @task
        def staging_laporan():
            from etl.staging_excel import staging_laporan as f
            return f(*_ctx())

        @task
        def staging_antrop():
            from etl.staging_excel import staging_antrop as f
            return f(*_ctx())

        @task
        def staging_kader():
            from etl.staging_excel import staging_kader as f
            return f(*_ctx())

        @task
        def staging_pdf():
            from etl.staging_pdf import staging_penduduk as f
            return f(*_ctx())

        @task
        def staging_web():
            from etl.staging_web import staging_stunting as f
            b, p = _ctx()
            return f(b, percobaan_ke=p)

        staging_laporan(), staging_antrop(), staging_kader(), staging_pdf(), staging_web()

    @task
    def transform():
        from etl.transform import transform as f
        f(*_ctx())

    @task.branch
    def quality_gate():
        from quality.quality_gate import quality_gate as f
        hasil = f(*_ctx())
        return "load_dw" if hasil["lulus"] else "quarantine"

    @task
    def load_dw():
        from etl.load_dw import load_dw as f
        return f(*_ctx())

    @task(retries=0)
    def quarantine():
        from quality.quality_gate import quarantine as f
        b, p = _ctx()
        f(b, None, p)

    schema = init_schema()
    ingest = [ingest_excel(), ingest_pdf(), ingest_web()]
    gate = quality_gate()
    schema >> ingest >> load_staging() >> transform() >> gate >> [load_dw(), quarantine()]


posyandu_pipeline()
