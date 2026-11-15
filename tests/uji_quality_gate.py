"""Uji quality gate, rollback load, dan idempotensi (dijalankan manual; hasilnya menjadi bukti).

    docker compose exec airflow-scheduler python -m tests.uji_quality_gate

Skenario (lanjutan E1–E5 di tests/uji_error_handling.py):
  E6  pemeriksaan kritis Q06 gagal di tengah load (ada kolom 'nama_kader' di mart)
      -> load_dw GAGAL, ROLLBACK, isi DWH sama persis dengan sebelum load
  E7  pemeriksaan kritis Q02 gagal sebelum load (versi aktif ganda di curated)
      -> quality_gate mengembalikan lulus=False (cabang quarantine), quarantine mencatat batch di
         dq.quarantine dan GAGAL; DWH tidak disentuh
  E8  load_dw dijalankan dua kali berturut-turut -> jumlah baris & surrogate key sama (idempotent)

Prasyarat: curated dan dw sudah terisi (transform + load_dw sudah pernah sukses).
Uji membuat tabel sementara mart.uji_q06_kader lalu menghapusnya, dan merusak curated sesaat lalu
membangunnya ulang dengan transform (curated selalu dapat dibangun ulang dari staging).
"""

from __future__ import annotations

import logging
from datetime import datetime

from common.config import ZONA_WAKTU
from common.db import buka_koneksi
from etl.load_dw import QualityGateKritisError, load_dw
from etl.transform import transform
from quality.quality_gate import quality_gate, quarantine

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
STAMP = datetime.now(ZONA_WAKTU).strftime("%Y%m%dT%H%M%S")
hasil: list[tuple[str, bool, str]] = []


def lapor(kode: str, lulus: bool, ket: str) -> None:
    hasil.append((kode, lulus, ket))
    print(f"[{'LULUS' if lulus else 'GAGAL'}] {kode}: {ket}", flush=True)


def jalankan(sql: str, params=()) -> list[tuple]:
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def sidik_dw() -> tuple:
    """Ringkasan isi DWH untuk membandingkan sebelum/sesudah (jumlah baris, total, kunci, batch)."""
    return jalankan("""
        SELECT (SELECT count(*) FROM dw.fact_kunjungan_bulanan), (SELECT sum(sasaran) FROM dw.fact_kunjungan_bulanan),
               (SELECT count(*) FROM dw.fact_alat_snapshot), (SELECT count(*) FROM dw.fact_kader_snapshot),
               (SELECT count(*) FROM dw.fact_penduduk_kelurahan), (SELECT count(*) FROM dw.fact_stunting_kelurahan),
               (SELECT count(*) FROM dw.dim_posyandu), (SELECT sum(posyandu_key) FROM dw.dim_posyandu),
               (SELECT sum(kelurahan_key) FROM dw.dim_kelurahan),
               (SELECT max(batch_id) FROM dw.fact_kunjungan_bulanan)""")[0]


def e6_rollback_q06() -> None:
    batch = f"uji__q06_{STAMP}"
    sebelum = sidik_dw()
    jalankan("CREATE TABLE IF NOT EXISTS mart.uji_q06_kader (nama_kader text)")
    try:
        try:
            load_dw(batch)
            lapor("E6", False, "load_dw tidak gagal padahal ada kolom nama_kader di mart")
            return
        except QualityGateKritisError as err:
            pesan = str(err)
    finally:
        jalankan("DROP TABLE IF EXISTS mart.uji_q06_kader")
    sesudah = sidik_dw()
    status = jalankan("SELECT status FROM dq.batch_log WHERE batch_id = %s AND task_name = 'load_dw'", (batch,))
    q06 = jalankan("SELECT lulus, nilai FROM dq.quality_result WHERE batch_id = %s AND kode_cek = 'Q06'", (batch,))
    ok = sebelum == sesudah and status == [("GAGAL",)] and q06 and q06[0][0] is False
    lapor("E6", ok, f"{pesan}; batch_log={status}; Q06={q06}; DWH sama sebelum/sesudah: {sebelum == sesudah}")


def e7_quarantine_q02() -> None:
    batch = f"uji__q02_{STAMP}"
    sebelum = sidik_dw()
    # Rusak curated: versi lama posyandu yang pindah puskesmas ikut ditandai aktif -> 2 versi aktif.
    jalankan("""UPDATE curated.posyandu_versi SET is_current = true
                WHERE (posyandu_id, versi) = (SELECT posyandu_id, 1 FROM curated.posyandu_versi WHERE versi = 2 LIMIT 1)""")
    try:
        h = quality_gate(batch)
        try:
            quarantine(batch)
            raised = False
        except RuntimeError:
            raised = True
    finally:
        transform(f"uji__q02_pulih_{STAMP}")  # bangun ulang curated dari staging
    q = jalankan("SELECT alasan FROM dq.quarantine WHERE batch_id = %s", (batch,))
    ok = (not h["lulus"]) and h["gagal_kritis"] == ["Q02"] and raised and len(q) == 1 and sidik_dw() == sebelum
    lapor("E7", ok, f"gate lulus={h['lulus']} kritis={h['gagal_kritis']} -> cabang quarantine; quarantine raise={raised}; "
                    f"dq.quarantine: {q[0][0] if q else None}; DWH tidak berubah: {sidik_dw() == sebelum}")


def e8_idempotent() -> None:
    load_dw(f"uji__idem1_{STAMP}")
    a = sidik_dw()[:9]
    load_dw(f"uji__idem2_{STAMP}")
    b = sidik_dw()[:9]
    lapor("E8", a == b, f"load_dw 2× berturut-turut: jumlah baris, Σ sasaran, dan Σ surrogate key sama = {a == b} {b[:3]}")


def main() -> int:
    for f in (e6_rollback_q06, e7_quarantine_q02, e8_idempotent):
        try:
            f()
        except Exception as err:  # skenario yang error dianggap tidak lulus
            lapor(f.__name__, False, f"error tak terduga {type(err).__name__}: {err}")
    lulus = sum(1 for _, ok, _ in hasil if ok)
    print(f"RINGKASAN: {lulus}/{len(hasil)} skenario lulus")
    return 0 if lulus == len(hasil) else 1


if __name__ == "__main__":
    raise SystemExit(main())
