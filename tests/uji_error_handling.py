"""Uji skenario error handling ingestion & staging (dijalankan manual; hasilnya menjadi bukti).

    docker compose exec airflow-scheduler python -m tests.uji_error_handling

Skenario:
  E1  database terputus             -> koneksi dicoba ulang, lalu gagal dengan OperationalError
  E2  portal web gagal, tanpa fallback -> ingest_web GAGAL; WEB01 di dq_log; status GAGAL di batch_log
  E3  portal web gagal, ada fallback   -> ingest_web SUKSES dengan cara_ambil = fallback_manual
  E4  objek data lake korup (checksum) -> staging GAGAL, tercatat di dq.quarantine, isi staging tidak berubah
  E5  struktur header Excel berubah    -> StrukturSumberError (data salah posisi tidak dimuat)

Uji tidak mengubah berkas sumber. batch_id uji berawalan 'uji__' agar mudah dibedakan di log.
E3 menulis salinan halaman E2 (data terbuka) ke /tmp di container dan ke raw zone sebagai objek baru.
"""

from __future__ import annotations

import io
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import openpyxl
import psycopg2

from common.config import ZONA_WAKTU
from common.db import buka_koneksi
from common.errors import DataKorupError, StrukturSumberError, SumberTidakTersediaError
from common.lake import buat_klien_s3
from etl import staging_excel
from etl.staging_web import parse_halaman
from ingestion import ingest_web

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
STAMP = datetime.now(ZONA_WAKTU).strftime("%Y%m%dT%H%M%S")
hasil: list[tuple[str, bool, str]] = []


def lapor(kode: str, lulus: bool, ket: str) -> None:
    hasil.append((kode, lulus, ket))
    print(f"[{'LULUS' if lulus else 'GAGAL'}] {kode}: {ket}", flush=True)


def query(sql: str, params=()) -> list[tuple]:
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def manifest_terakhir(sumber: str) -> tuple:
    return query("SELECT batch_id, bucket, object_key, sha256 FROM dq.ingest_manifest WHERE sumber = %s "
                 "ORDER BY manifest_id DESC LIMIT 1", (sumber,))[0]


def e1_database_terputus() -> None:
    asli = os.environ["DW_HOST"]
    os.environ["DW_HOST"] = "host-db-tidak-ada"
    try:
        buka_koneksi(percobaan=3, jeda_awal=1)
        lapor("E1", False, "koneksi ke host salah malah berhasil")
    except psycopg2.OperationalError:
        lapor("E1", True, "3 percobaan dengan jeda 1 s & 2 s, lalu OperationalError dilempar (task gagal -> retry Airflow)")
    finally:
        os.environ["DW_HOST"] = asli


def e2_web_gagal_tanpa_fallback() -> None:
    batch = f"uji__web_gagal_{STAMP}"
    url_asli, dir_asli = dict(ingest_web.SUMBER_WEB), ingest_web.SUMBER_DIR
    ingest_web.SUMBER_WEB[2025] = "http://127.0.0.1:9/halaman-tidak-ada"
    ingest_web.SUMBER_DIR = Path("/tmp/uji_kosong")
    try:
        ingest_web.ingest_stunting(batch, tahun_list=(2025,))
        lapor("E2", False, "ingest_web tidak gagal")
    except SumberTidakTersediaError as err:
        status = query("SELECT status FROM dq.batch_log WHERE batch_id = %s", (batch,))
        web01 = query("SELECT count(*) FROM dq.dq_log WHERE batch_id = %s AND kode_aturan = 'WEB01'", (batch,))[0][0]
        ok = status == [("GAGAL",)] and web01 == 1
        lapor("E2", ok, f"SumberTidakTersediaError; batch_log={status}; WEB01={web01}; pesan: {str(err)[:110]}")
    finally:
        ingest_web.SUMBER_WEB.clear()
        ingest_web.SUMBER_WEB.update(url_asli)
        ingest_web.SUMBER_DIR = dir_asli


def e3_web_gagal_dengan_fallback() -> None:
    batch = f"uji__web_fallback_{STAMP}"
    _, bucket, key, _ = manifest_terakhir("e2_stunting_2025")
    isi = buat_klien_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    folder = Path("/tmp/uji_sumber/eksternal")
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "stunting_2025.html").write_bytes(isi)
    url_asli, dir_asli = dict(ingest_web.SUMBER_WEB), ingest_web.SUMBER_DIR
    ingest_web.SUMBER_WEB[2025] = "http://127.0.0.1:9/halaman-tidak-ada"
    ingest_web.SUMBER_DIR = Path("/tmp/uji_sumber")
    try:
        ingest_web.ingest_stunting(batch, tahun_list=(2025,))
        cara = query("SELECT cara_ambil, lokasi_asal FROM dq.ingest_manifest WHERE batch_id = %s", (batch,))
        _, _, data, _ = parse_halaman(isi, 2025)
        ok = cara == [("fallback_manual", "eksternal/stunting_2025.html")] and len(data) == 52
        lapor("E3", ok, f"manifest={cara}; isi fallback ter-parse {len(data)} kelurahan")
    except Exception as err:  # noqa: BLE001
        lapor("E3", False, f"{type(err).__name__}: {err}")
    finally:
        ingest_web.SUMBER_WEB.clear()
        ingest_web.SUMBER_WEB.update(url_asli)
        ingest_web.SUMBER_DIR = dir_asli
        shutil.rmtree("/tmp/uji_sumber", ignore_errors=True)


def e4_objek_korup() -> None:
    batch = f"uji__korup_{STAMP}"
    _, bucket, key, _ = manifest_terakhir("internal_laporan_posyandu")
    query("INSERT INTO dq.ingest_manifest (batch_id, sumber, cara_ambil, lokasi_asal, bucket, object_key, sha256, "
          "ukuran_byte, status_unggah, waktu_ambil) VALUES (%s, 'internal_laporan_posyandu', 'lokal', 'uji', %s, %s, "
          "%s, 0, 'sudah_ada', now()) RETURNING manifest_id", (batch, bucket, key, "0" * 64))
    sebelum = query("SELECT count(*), min(batch_id) FROM stg.kunjungan_bulanan")[0]
    try:
        staging_excel.staging_laporan(batch)
        lapor("E4", False, "staging tidak menolak objek dengan checksum salah")
    except DataKorupError:
        sesudah = query("SELECT count(*), min(batch_id) FROM stg.kunjungan_bulanan")[0]
        q = query("SELECT count(*) FROM dq.quarantine WHERE batch_id = %s", (batch,))[0][0]
        ok = q == 1 and sebelum == sesudah
        lapor("E4", ok, f"DataKorupError; quarantine={q}; staging sebelum={sebelum} sesudah={sesudah} (tidak berubah)")


def e5_struktur_berubah() -> None:
    _, bucket, key, _ = manifest_terakhir("internal_laporan_posyandu")
    isi = buat_klien_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    wb = openpyxl.load_workbook(io.BytesIO(isi), read_only=True, data_only=True)
    rows = [tuple(r) for r in wb["2025"].iter_rows(min_row=1, max_row=8, values_only=True)]
    wb.close()
    h = staging_excel._cari_header(rows, "No.")
    staging_excel.validasi_header_laporan("2025", rows, h)  # struktur asli harus lolos
    # Simulasi dinas menyisipkan satu kolom setelah kolom Posyandu: semua kolom bergeser 1.
    geser = [r[:5] + (None,) + r[5:] for r in rows]
    try:
        staging_excel.validasi_header_laporan("2025", geser, h)
        lapor("E5", False, "header bergeser tidak terdeteksi")
    except StrukturSumberError as err:
        lapor("E5", True, f"struktur asli lolos; struktur bergeser ditolak: {str(err)[:120]}")


def main() -> int:
    for f in (e1_database_terputus, e2_web_gagal_tanpa_fallback, e3_web_gagal_dengan_fallback,
              e4_objek_korup, e5_struktur_berubah):
        try:
            f()
        except Exception as err:  # noqa: BLE001
            lapor(f.__name__, False, f"error tak terduga {type(err).__name__}: {err}")
    lulus = sum(ok for _, ok, _ in hasil)
    print(f"RINGKASAN: {lulus}/{len(hasil)} skenario lulus")
    return 0 if lulus == len(hasil) else 1


if __name__ == "__main__":
    raise SystemExit(main())
