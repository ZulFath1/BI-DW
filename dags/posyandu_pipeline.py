"""DAG utama pipeline posyandu (Apache Airflow 3.3.2).

    init_schema
        └─> ingest_excel ─┐
            ingest_pdf ───┼─> load_staging (5 task) ─> transform ─> quality_gate ─┬─> load_dw ─> build_mart ─> train_ml ─> refresh_dashboard
            ingest_web ───┘                                                          └─> quarantine

Jadwal: tanggal 10 setiap bulan pukul 02.00 WITA (laporan puskesmas masuk tanggal 5 menurut
Panduan, sisanya untuk validasi; dokumen UTS 4.4). catchup=False: run yang terlewat tidak
dijalankan mundur karena setiap run memuat ulang seluruh data sumber.

Error handling:
  - retries=3 dengan jeda bertambah (exponential backoff, 2 -> 4 -> 8 menit) untuk error sesaat
    (portal web, database terputus, data lake);
  - setiap task menulis status BERJALAN/SUKSES/GAGAL ke dq.batch_log (common.batch.catat_task);
  - on_retry_callback / on_failure_callback menulis kejadian ke dq.batch_log (tahap 'orkestrasi')
    dan dq.dq_log (PERINGATAN / KRITIS). Bila database DWH sendiri tidak tersedia, kejadian
    disimpan di outbox (volume log Airflow) dan dikirim oleh init_schema pada run berikutnya;
  - quality gate kritis gagal -> cabang quarantine (DWH tidak diubah, DAG run berstatus gagal);
  - pemeriksaan kritis saat load gagal -> ROLLBACK di dalam load_dw.
batch_id = run_id Airflow, sehingga setiap baris staging/curated/dw/mart dapat ditelusuri ke run-nya.

Parameter `skenario_uji` (hanya untuk uji kegagalan saat trigger manual): "data_buruk" membuat
ingest_excel membaca salinan uji di data/uji/ (bukan data asli). Run terjadwal selalu "normal".
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task, task_group

log = logging.getLogger(__name__)
ZONA = pendulum.timezone("Asia/Makassar")


def _ctx():
    c = get_current_context()
    return c["run_id"], c["ti"].try_number


def _kejadian(context, jenis: str) -> None:
    try:
        from common.batch import catat_kejadian_orkestrasi

        ti = context["ti"]
        tempat = catat_kejadian_orkestrasi(context["run_id"], ti.dag_id, ti.task_id, jenis, ti.try_number,
                                           context.get("exception"))
        log.info("callback %s untuk %s dicatat di %s", jenis, ti.task_id, tempat)
    except Exception:  # callback tidak boleh menutupi error aslinya
        log.exception("callback %s gagal mencatat kejadian", jenis)


def catat_retry(context) -> None:
    """on_retry_callback: percobaan gagal dan Airflow akan mencoba ulang."""
    _kejadian(context, "retry")


def catat_kegagalan(context) -> None:
    """on_failure_callback: task gagal pada percobaan terakhir."""
    _kejadian(context, "gagal_final")


ARG_BAWAAN = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_retry_callback": catat_retry,
    "on_failure_callback": catat_kegagalan,
}


@dag(
    dag_id="posyandu_pipeline",
    description="Ingestion -> data lake -> staging -> transform -> quality gate -> DWH -> mart -> ML -> dashboard",
    schedule="0 2 10 * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,
    default_args=ARG_BAWAAN,
    params={"skenario_uji": Param("normal", type="string", enum=["normal", "data_buruk"],
                                  description="Hanya untuk uji kegagalan. 'data_buruk' = salinan uji di data/uji/")},
    tags=["posyandu", "uas-bigdata"],
    doc_md=__doc__,
)
def posyandu_pipeline():
    @task
    def init_schema():
        from etl.schema import init_schema as f
        f(*_ctx())

    @task
    def ingest_excel():
        from ingestion.ingest_files import ingest_excel as f
        skenario = get_current_context()["params"].get("skenario_uji", "normal")
        return f(*_ctx(), skenario_uji=skenario)

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

    @task
    def build_mart():
        from etl.build_mart import build_mart as f
        return f(*_ctx())

    @task
    def train_ml():
        from ml.clustering import train_ml as f
        return f(*_ctx())

    @task
    def refresh_dashboard():
        from etl.refresh_dashboard import refresh_dashboard as f
        return f(*_ctx())

    schema = init_schema()
    ingest = [ingest_excel(), ingest_pdf(), ingest_web()]
    gate = quality_gate()
    muat = load_dw()
    schema >> ingest >> load_staging() >> transform() >> gate >> [muat, quarantine()]
    muat >> build_mart() >> train_ml() >> refresh_dashboard()


posyandu_pipeline()
