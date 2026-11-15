"""Staging E2: halaman HTML portal Satu Data (jumlah stunting per kelurahan) -> stg.

Yang diambil dari halaman:
  - judul (mis. 'Jumlah Stunting Menurut Kelurahan Kota Banjarmasin Tahun 2025'),
    tahunnya harus sama dengan tahun sumber;
  - tabel pertama yang header-nya sesuai daftar kolom di bawah (baris tbody = data,
    tfoot = total yang tertulis di portal, dipakai untuk rekonsiliasi T18);
  - teks 'Catatan' (tanggal tarikan data; dipakai sebagai tanggal_data di fakta).
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from common.batch import catat_task
from common.errors import StrukturSumberError
from etl.staging_util import ambil_sumber, muat_ulang, norm
from ingestion.ingest_web import kode_sumber

log = logging.getLogger(__name__)

HEADER_E2 = ["No", "Kode Wilayah Kecamatan", "Kecamatan", "Kode Wilayah Kelurahan", "Kelurahan",
             "Stunting", "Wasting", "Underweight"]
KOLOM_STG = [
    "batch_id", "manifest_id", "sumber", "tahun_data", "baris_tabel", "jenis_baris", "judul_asli", "no_asli",
    "kode_kecamatan_asli", "kecamatan_asli", "kode_kelurahan_asli", "kelurahan_asli",
    "stunting_asli", "wasting_asli", "underweight_asli", "catatan_asli",
]


def _teks(el) -> str:
    return " ".join(el.get_text(" ").split())


def parse_halaman(isi: bytes, tahun: int) -> tuple[str, str | None, list[list[str]], list[str] | None]:
    soup = BeautifulSoup(isi, "lxml")
    judul = next((_teks(s) for s in soup.find_all("span") if _teks(s).startswith("Jumlah Stunting")), None)
    if judul is None:
        raise StrukturSumberError("judul 'Jumlah Stunting ...' tidak ditemukan di halaman")
    m = re.search(r"Tahun\s+(\d{4})", judul)
    if not m or int(m.group(1)) != tahun:
        raise StrukturSumberError(f"judul halaman '{judul}' tidak sesuai tahun {tahun}")

    tabel = None
    for t in soup.find_all("table"):
        thead = t.find("thead")
        if thead and [norm(_teks(th)) for th in thead.find_all("th")] == [norm(h) for h in HEADER_E2]:
            tabel = t
            break
    if tabel is None:
        raise StrukturSumberError(f"tabel dengan header {HEADER_E2} tidak ditemukan")

    data = []
    for tr in tabel.find("tbody").find_all("tr"):
        sel = [_teks(td) for td in tr.find_all("td")]
        if len(sel) != len(HEADER_E2):
            raise StrukturSumberError(f"baris tabel berisi {len(sel)} sel, harap {len(HEADER_E2)}: {sel}")
        data.append(sel)
    total = None
    tfoot = tabel.find("tfoot")
    if tfoot:
        total = [_teks(th) for th in tfoot.find_all(["th", "td"])]

    catatan = None
    for span in soup.find_all("span"):
        if _teks(span).startswith("Catatan"):
            catatan = _teks(span.parent).removeprefix("Catatan :").strip() or None
            break
    return judul, catatan, data, total


def staging_stunting(batch_id: str, tahun_list: tuple[int, ...] = (2024, 2025), percobaan_ke: int | None = None) -> int:
    with catat_task(batch_id, "staging_web", "staging", percobaan_ke) as hasil:
        rows, dq, ringkas = [], [], []
        for tahun in tahun_list:
            sumber = kode_sumber(tahun)
            obj = ambil_sumber(batch_id, sumber)
            judul, catatan, data, total = parse_halaman(obj.isi, tahun)
            for i, sel in enumerate(data, start=1):
                rows.append((batch_id, obj.manifest_id, sumber, tahun, i, "data", judul, *sel, catatan))
            if total:
                if len(total) != len(HEADER_E2):
                    raise StrukturSumberError(f"baris total {tahun} berisi {len(total)} sel: {total}")
                rows.append((batch_id, obj.manifest_id, sumber, tahun, 0, "total", judul, *total, catatan))
            else:
                dq.append({"kode_aturan": "STG05", "tingkat": "PERINGATAN", "objek": sumber,
                           "keterangan": "baris total (tfoot) tidak ada; rekonsiliasi T18 tidak dapat dilakukan"})
            dq.append({"kode_aturan": "STG00", "tingkat": "INFO", "objek": sumber, "nilai": str(len(data)),
                       "keterangan": f"jumlah baris kelurahan; judul '{judul}'; catatan '{catatan}'"})
            ringkas.append(f"{tahun}={len(data)}")
        n = muat_ulang("stg.stunting_kelurahan", KOLOM_STG, rows, batch_id, dq)
        hasil.jumlah_baris = n
        hasil.pesan = "baris kelurahan: " + ", ".join(ringkas)
        return n
