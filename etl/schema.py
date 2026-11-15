"""Task init_schema: menjalankan semua berkas DDL di sql/ddl/ secara berurutan.

Semua DDL ditulis idempotent (CREATE ... IF NOT EXISTS, CREATE OR REPLACE VIEW,
INSERT ... ON CONFLICT DO NOTHING), sehingga aman dijalankan pada setiap run pipeline
dan tidak pernah menghapus data. Seluruh berkas dijalankan dalam SATU transaksi:
bila satu berkas gagal, tidak ada perubahan schema yang tersimpan.
"""

from __future__ import annotations

import logging

from common.batch import catat_task
from common.config import SQL_DIR
from common.db import buka_koneksi

log = logging.getLogger(__name__)


def daftar_ddl() -> list:
    berkas = sorted((SQL_DIR / "ddl").glob("*.sql"))
    if not berkas:
        raise FileNotFoundError(f"tidak ada berkas DDL di {SQL_DIR / 'ddl'}")
    return berkas


def terapkan_ddl() -> list[str]:
    berkas = daftar_ddl()
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            for f in berkas:
                log.info("menjalankan %s", f.name)
                cur.execute(f.read_text(encoding="utf-8"))
    finally:
        conn.close()
    return [f.name for f in berkas]


def init_schema(batch_id: str) -> None:
    # DDL dijalankan dulu karena tabel dq.batch_log sendiri dibuat oleh DDL.
    nama = terapkan_ddl()
    with catat_task(batch_id, "init_schema", "schema") as hasil:
        hasil.jumlah_baris = len(nama)
        hasil.pesan = "DDL diterapkan: " + ", ".join(nama)
