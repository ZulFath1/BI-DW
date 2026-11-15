"""Menjalankan task pipeline secara manual dari terminal (untuk pengembangan & uji).

DAG Airflow memanggil fungsi yang sama; CLI ini hanya pintu masuk alternatif.

Contoh (dari folder project di laptop):
    docker compose exec airflow-scheduler python -m etl.cli schema
    docker compose exec airflow-scheduler python -m etl.cli ingest --batch-id manual__uji1
    docker compose exec airflow-scheduler python -m etl.cli staging --batch-id manual__uji1
    docker compose exec airflow-scheduler python -m etl.cli semua
"""

from __future__ import annotations

import argparse
import logging
import sys

from common.batch import buat_batch_id


def _task_ingest():
    from ingestion.ingest_files import ingest_excel, ingest_pdf
    from ingestion.ingest_web import ingest_stunting
    return {"ingest-excel": ingest_excel, "ingest-pdf": ingest_pdf, "ingest-web": ingest_stunting}


def _task_staging():
    from etl.staging_excel import staging_antrop, staging_kader, staging_laporan
    from etl.staging_pdf import staging_penduduk
    from etl.staging_web import staging_stunting
    return {
        "staging-laporan": staging_laporan, "staging-antrop": staging_antrop, "staging-kader": staging_kader,
        "staging-pdf": staging_penduduk, "staging-web": staging_stunting,
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="python -m etl.cli", description="Task pipeline posyandu")
    p.add_argument("perintah", help="schema | ingest | staging | semua | nama task (mis. ingest-excel, staging-kader)")
    p.add_argument("--batch-id", default=None, help="default: manual__<waktu sekarang>")
    a = p.parse_args(argv)
    batch_id = a.batch_id or buat_batch_id()
    logging.getLogger(__name__).info("batch_id = %s", batch_id)

    from etl.schema import init_schema

    if a.perintah == "schema":
        urutan = {"schema": init_schema}
    elif a.perintah == "ingest":
        urutan = _task_ingest()
    elif a.perintah == "staging":
        urutan = _task_staging()
    elif a.perintah == "semua":
        urutan = {"schema": init_schema, **_task_ingest(), **_task_staging()}
    else:
        semua = {**_task_ingest(), **_task_staging()}
        if a.perintah not in semua:
            p.error(f"perintah tidak dikenal: {a.perintah}")
        urutan = {a.perintah: semua[a.perintah]}

    for nama, fungsi in urutan.items():
        try:
            fungsi(batch_id)
        except Exception as err:  # error sudah dicatat di batch_log oleh task
            logging.getLogger(__name__).error("task %s gagal: %s: %s — pipeline berhenti", nama, type(err).__name__, err)
            return 1
    print(f"SELESAI batch_id={batch_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
