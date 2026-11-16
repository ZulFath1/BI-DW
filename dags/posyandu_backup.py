"""DAG backup harian (Apache Airflow 3.3.2).

    backup_database ─> mirror_lake ─> retensi

Jadwal: setiap hari pukul 01.00 WITA — sebelum jendela pipeline (02.00 tanggal 10), sehingga isi database
tidak berubah selama dump. catchup=False, max_active_runs=1. Retry 2× dengan jeda 5 menit; callback
retry/gagal sama dengan DAG utama (dq.batch_log / dq.dq_log, outbox bila database mati).

Isi backup, lokasi, retensi, dan cara restore: backup/README.md.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

ZONA = pendulum.timezone("Asia/Makassar")


def _kejadian(context, jenis: str) -> None:
    """Callback retry/gagal: dicatat ke dq.batch_log & dq.dq_log (outbox bila DB mati), seperti DAG utama."""
    try:
        from common.batch import catat_kejadian_orkestrasi

        ti = context["ti"]
        catat_kejadian_orkestrasi(context["run_id"], ti.dag_id, ti.task_id, jenis, ti.try_number,
                                  context.get("exception"))
    except Exception:  # callback tidak boleh menutupi error aslinya
        pass


def catat_retry(context) -> None:
    _kejadian(context, "retry")


def catat_kegagalan(context) -> None:
    _kejadian(context, "gagal_final")


def _ctx():
    c = get_current_context()
    return c["run_id"], c["ti"].try_number


@dag(
    dag_id="posyandu_backup",
    description="Backup harian: pg_dump DWH & metadata Airflow, mirror raw zone data lake, retensi",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineer", "retries": 2, "retry_delay": timedelta(minutes=5),
                  "on_retry_callback": catat_retry, "on_failure_callback": catat_kegagalan},
    tags=["posyandu", "backup", "uas-bigdata"],
    doc_md=__doc__,
)
def posyandu_backup():
    @task
    def backup_database():
        from backup.backup import backup_database as f
        return f(*_ctx())

    @task
    def mirror_lake():
        from backup.backup import mirror_lake as f
        return f(*_ctx())

    @task
    def retensi():
        from backup.backup import retensi as f
        return f(*_ctx())

    backup_database() >> mirror_lake() >> retensi()


posyandu_backup()
