"""Pencatatan status task (dq.batch_log), temuan kualitas (dq.dq_log),
dan baris karantina (dq.quarantine).

batch_log dan quarantine ditulis lewat koneksi autocommit tersendiri, sehingga
catatan GAGAL tetap tersimpan walaupun transaksi utama task di-rollback.
"""

from __future__ import annotations

import json
import os
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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


# ---------------------------------------------------------------- callback Airflow + outbox
# Callback gagal/retry menulis ke dq.batch_log (tahap 'orkestrasi') dan dq.dq_log. Bila database DWH
# justru sedang tidak tersedia (penyebab kegagalannya), catatan disimpan dulu sebagai berkas JSON di
# volume log Airflow (outbox) lalu dikirim ke database oleh task pertama run berikutnya (init_schema).
# Dengan begitu kegagalan tidak pernah hilang dari monitoring walaupun database mati.
OUTBOX_DIR = Path(os.environ.get("DQ_OUTBOX_DIR", "/opt/airflow/logs/dq_outbox"))


def _tulis_kejadian(e: dict, percobaan: int = 2) -> None:
    conn = buka_koneksi(autocommit=True, percobaan=percobaan, jeda_awal=1.0)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dq.batch_log (batch_id, task_name, tahap, status, waktu_mulai, waktu_selesai, pesan, "
                "percobaan_ke) VALUES (%s, %s, 'orkestrasi', 'GAGAL', %s, %s, %s, %s)",
                (e["batch_id"], e["task_name"], e["waktu"], e["waktu"], e["pesan"], e["percobaan_ke"]))
            tulis_dq_log(cur, e["batch_id"], "orkestrasi", [{
                "kode_aturan": "AIRFLOW", "tingkat": "KRITIS" if e["jenis"] == "gagal_final" else "PERINGATAN",
                "objek": e["objek"], "nilai": str(e["percobaan_ke"]), "keterangan": e["pesan"]}])
    finally:
        conn.close()


def catat_kejadian_orkestrasi(batch_id: str, dag_id: str, task_id: str, jenis: str, percobaan_ke: int,
                              error: BaseException | None) -> str:
    """jenis: 'retry' (percobaan gagal, akan diulang) | 'gagal_final' (semua percobaan habis).
    Mengembalikan 'database' atau 'outbox' (tempat catatan disimpan)."""
    sebab = f"{type(error).__name__}: {error}" if error is not None else "tidak diketahui"
    keterangan = ("percobaan gagal, Airflow akan mencoba ulang" if jenis == "retry"
                  else "task GAGAL setelah semua percobaan")
    e = {"batch_id": batch_id, "task_name": task_id, "objek": f"{dag_id}.{task_id}", "jenis": jenis,
         "percobaan_ke": percobaan_ke, "waktu": datetime.now(ZONA_WAKTU).isoformat(),
         "pesan": f"[{'on_retry_callback' if jenis == 'retry' else 'on_failure_callback'}] {keterangan} "
                  f"(percobaan ke-{percobaan_ke}): {sebab}"[:4000]}
    try:
        _tulis_kejadian(e)
        return "database"
    except Exception as err:  # database tidak tersedia -> outbox
        OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
        e["pesan"] = (e["pesan"] + " | ditunda di outbox karena database tidak tersedia: "
                      f"{type(err).__name__}")[:4000]
        nama = f"{datetime.now(ZONA_WAKTU):%Y%m%dT%H%M%S%f}_{task_id}_{percobaan_ke}.json"
        (OUTBOX_DIR / nama).write_text(json.dumps(e, ensure_ascii=False), encoding="utf-8")
        log.error("callback: database tidak tersedia, kejadian disimpan di outbox %s", nama)
        return "outbox"


def kirim_outbox() -> int:
    """Mengirim catatan callback yang tertunda ke database; berkas yang terkirim diberi akhiran .terkirim."""
    if not OUTBOX_DIR.is_dir():
        return 0
    n = 0
    for f in sorted(OUTBOX_DIR.glob("*.json")):
        e = json.loads(f.read_text(encoding="utf-8"))
        e["pesan"] = (e["pesan"] + f" | dikirim dari outbox pada {datetime.now(ZONA_WAKTU):%Y-%m-%d %H:%M:%S}")[:4000]
        _tulis_kejadian(e, percobaan=5)
        f.rename(f.with_suffix(".terkirim"))
        n += 1
    if n:
        log.info("outbox: %d catatan callback tertunda dikirim ke dq.batch_log", n)
    return n
