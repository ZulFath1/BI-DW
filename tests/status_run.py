"""Mencetak status setiap task pada satu DAG run (untuk bukti uji), langsung dari metadata Airflow.

    docker compose exec airflow-scheduler python -m tests.status_run <run_id> [dag_id]

Kolom: task_id, state, try_number (percobaan terakhir), mulai, selesai (UTC).
"""

from __future__ import annotations

import sys

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session


def main(run_id: str, dag_id: str = "posyandu_pipeline") -> None:
    with create_session() as s:
        dr = s.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).one()
        print(f"DAG run {run_id}: state={dr.state}, conf={dr.conf}, mulai={dr.start_date:%Y-%m-%d %H:%M:%S}, "
              f"selesai={dr.end_date:%Y-%m-%d %H:%M:%S}" if dr.end_date else f"DAG run {run_id}: state={dr.state}")
        tis = (s.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id)
               .order_by(TaskInstance.start_date.nulls_last()).all())
        print(f"{'task_id':38} {'state':16} {'try':>3}  {'mulai':19}  {'selesai':19}")
        for ti in tis:
            f = (lambda d: f"{d:%Y-%m-%d %H:%M:%S}" if d else "-")
            print(f"{ti.task_id:38} {str(ti.state):16} {ti.try_number:>3}  {f(ti.start_date):19}  {f(ti.end_date):19}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
