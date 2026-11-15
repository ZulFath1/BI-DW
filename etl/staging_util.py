"""Fungsi bantu tahap staging: ambil objek dari data lake, konversi nilai sel ke teks
asli, dan muat ulang tabel staging secara atomik."""

from __future__ import annotations

import logging
from datetime import date, datetime, time

from psycopg2.extras import execute_values

from common.batch import catat_quarantine, tulis_dq_log
from common.db import buka_koneksi
from common.errors import DataKorupError
from common.lake import ObjekManifest, baca_objek_batch

log = logging.getLogger(__name__)


def ambil_sumber(batch_id: str, sumber: str) -> ObjekManifest:
    """Membaca objek raw batch ini. Objek korup (checksum tak cocok) -> quarantine + error."""
    try:
        return baca_objek_batch(batch_id, sumber)
    except DataKorupError as err:
        catat_quarantine(batch_id, "staging", sumber, sumber, str(err))
        raise


def ke_teks(v) -> str | None:
    """Nilai sel -> teks apa adanya. Angka bulat tetap tanpa '.0' bila sumbernya int;
    float memakai repr terpendek Python (mis. 68.25). Teks tidak diubah (spasi dipertahankan)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    return str(v)


def kode_tipe(v) -> str:
    if v is None:
        return "_"
    if isinstance(v, bool):
        return "b"
    if isinstance(v, (int, float)):
        return "n"
    if isinstance(v, (datetime, date, time)):
        return "d"
    return "s"


def norm(v) -> str:
    """Untuk membandingkan label header: spasi dirapikan, huruf kecil."""
    return " ".join(str(v).split()).casefold() if v is not None else ""


def muat_ulang(tabel: str, kolom: list[str], baris: list[tuple], batch_id: str,
               dq_entri: list[dict] | None = None, filter_hapus: tuple[str, tuple] | None = None) -> int:
    """DELETE + INSERT dalam satu transaksi: bila insert gagal, isi staging lama tetap utuh.

    filter_hapus: (klausa WHERE, parameter) bila hanya sebagian isi tabel yang diganti.
    dq_entri ikut ditulis dalam transaksi yang sama.
    """
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            if filter_hapus:
                cur.execute(f"DELETE FROM {tabel} WHERE {filter_hapus[0]}", filter_hapus[1])
            else:
                cur.execute(f"DELETE FROM {tabel}")
            execute_values(cur, f"INSERT INTO {tabel} ({', '.join(kolom)}) VALUES %s", baris, page_size=5000)
            if dq_entri:
                tulis_dq_log(cur, batch_id, "staging", dq_entri)
    finally:
        conn.close()
    return len(baris)
