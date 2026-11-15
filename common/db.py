"""Koneksi ke database DWH (posyandu_dw) sebagai etl_user, dengan retry.

Bila database sesaat terputus (mis. container PostgreSQL baru dinyalakan ulang),
koneksi dicoba ulang dengan jeda bertambah (2, 4, 8, 16 detik). Bila tetap gagal,
error dilempar sehingga task gagal dan Airflow menjalankan retry di level task.
"""

from __future__ import annotations

import logging
import os
import time

import psycopg2

from common.config import wajib

log = logging.getLogger(__name__)


def buka_koneksi(autocommit: bool = False, percobaan: int = 5, jeda_awal: float = 2.0):
    """Membuka koneksi psycopg2 ke database DWH. Kredensial dari environment."""
    for ke in range(1, percobaan + 1):
        try:
            conn = psycopg2.connect(
                host=os.environ.get("DW_HOST", "postgres"),
                port=int(os.environ.get("DW_PORT", "5432")),
                dbname=wajib("DW_DB"),
                user=wajib("ETL_USER"),
                password=wajib("ETL_PASSWORD"),
                connect_timeout=10,
                application_name="posyandu-pipeline",
            )
            conn.autocommit = autocommit
            return conn
        except psycopg2.OperationalError as err:
            if ke == percobaan:
                log.error("koneksi database gagal setelah %d percobaan", percobaan)
                raise
            jeda = jeda_awal * 2 ** (ke - 1)
            log.warning(
                "koneksi database gagal (percobaan %d/%d): %s — coba lagi dalam %.0f detik",
                ke, percobaan, str(err).strip(), jeda,
            )
            time.sleep(jeda)
    raise AssertionError("tidak terjangkau")
