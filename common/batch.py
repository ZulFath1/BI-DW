"""Pencatatan status task (dq.batch_log), temuan kualitas (dq.dq_log),
dan baris karantina (dq.quarantine).

batch_log dan quarantine ditulis lewat koneksi autocommit tersendiri, sehingga
catatan GAGAL tetap tersimpan walaupun transaksi utama task di-rollback.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from psycopg2.extras import Json, execute_values

from common.config import ZONA_WAKTU
from common.db import buka_koneksi

log = logging.getLogger(__name__)


def buat_batch_id() -> str:
    """batch_id untuk eksekusi manual (CLI). Di Airflow dipakai run_id DAG."""
    return "manual__" + datetime.now(ZONA_WAKTU).strftime("%Y%m%dT%H%M%S")


@dataclass
class HasilTask:
    """Diisi oleh task; dicatat ke batch_log saat task selesai."""

    status: str = "SUKSES"
    jumlah_baris: int | None = None
    pesan: str | None = None


@contextmanager
def catat_task(batch_id: str, task_name: str, tahap: str, percobaan_ke: int | None = None) -> Iterator[HasilTask]:
    """Mencatat task ke dq.batch_log: BERJALAN saat mulai, lalu SUKSES/GAGAL.

    Pemakaian:
        with catat_task(batch_id, "ingest_excel", "ingestion") as hasil:
            ...kerja...
            hasil.jumlah_baris = n
    Error apa pun di dalam blok dicatat sebagai GAGAL (jenis + pesan), lalu dilempar ulang.
    """
    conn = buka_koneksi(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dq.batch_log (batch_id, task_name, tahap, status, percobaan_ke) "
                "VALUES (%s, %s, %s, 'BERJALAN', %s) RETURNING log_id",
                (batch_id, task_name, tahap, percobaan_ke),
            )
            log_id = cur.fetchone()[0]
        log.info("[%s] %s mulai (log_id=%s)", batch_id, task_name, log_id)
        hasil = HasilTask()
        try:
            yield hasil
        except BaseException as err:
            pesan = f"{type(err).__name__}: {err}"[:4000]
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE dq.batch_log SET status = 'GAGAL', waktu_selesai = now(), pesan = %s "
                    "WHERE log_id = %s",
                    (pesan, log_id),
                )
            log.error("[%s] %s GAGAL: %s", batch_id, task_name, pesan)
            raise
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dq.batch_log SET status = %s, waktu_selesai = now(), jumlah_baris = %s, pesan = %s "
                "WHERE log_id = %s",
                (hasil.status, hasil.jumlah_baris, hasil.pesan, log_id),
            )
        log.info("[%s] %s %s: %s baris. %s", batch_id, task_name, hasil.status, hasil.jumlah_baris, hasil.pesan or "")
    finally:
        conn.close()


def tulis_dq_log(cur, batch_id: str, tahap: str, entri: list[dict]) -> int:
    """Menulis temuan kualitas ke dq.dq_log memakai cursor transaksi yang sedang berjalan.

    Setiap entri: kode_aturan, tingkat (INFO/PERINGATAN/KRITIS), objek, baris_sumber,
    kolom, nilai, keterangan. ATURAN PRIVASI: jangan pernah mengisi `nilai` dengan nama
    atau alamat kader.
    """
    if not entri:
        return 0
    execute_values(
        cur,
        "INSERT INTO dq.dq_log (batch_id, tahap, kode_aturan, tingkat, objek, baris_sumber, kolom, nilai, keterangan) "
        "VALUES %s",
        [
            (
                batch_id, tahap, e["kode_aturan"], e.get("tingkat", "INFO"), e.get("objek"),
                e.get("baris_sumber"), e.get("kolom"), e.get("nilai"), e.get("keterangan"),
            )
            for e in entri
        ],
    )
    return len(entri)


def tulis_dq_log_mandiri(batch_id: str, tahap: str, entri: list[dict]) -> int:
    """Seperti tulis_dq_log, tetapi dengan koneksi autocommit sendiri (dipakai di luar transaksi)."""
    conn = buka_koneksi(autocommit=True)
    try:
        with conn.cursor() as cur:
            return tulis_dq_log(cur, batch_id, tahap, entri)
    finally:
        conn.close()


def catat_quarantine(batch_id: str, tahap: str, sumber: str, objek: str, alasan: str,
                     data: dict | None = None, baris_sumber: int | None = None) -> None:
    """Menyimpan data/berkas bermasalah ke dq.quarantine (koneksi autocommit sendiri),
    agar tetap tercatat walaupun task gagal."""
    conn = buka_koneksi(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dq.quarantine (batch_id, tahap, sumber, objek, baris_sumber, alasan, data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (batch_id, tahap, sumber, objek, baris_sumber, alasan,
                 Json(json.loads(json.dumps(data, default=str))) if data is not None else None),
            )
    finally:
        conn.close()
