"""Ingestion E2 (jumlah stunting per kelurahan) dari portal Satu Data Kota Banjarmasin.

Cara ambil: halaman HTML data statistik diunduh dengan HTTP GET lalu disimpan APA ADANYA
ke raw zone. Tabel di halaman tersebut di-parse pada tahap staging.

Kenapa bukan tombol unduh CSV/Excel di portal: unduhan tersebut berupa formulir POST yang
meminta email, jenis kelamin, rentang usia, dan pekerjaan pengunduh. Pipeline tidak
mengirim data pribadi ke layanan luar, jadi yang diambil adalah halaman publiknya.

Error handling:
  - setiap URL dicoba hingga 3 kali (jeda 5 dan 10 detik) untuk error jaringan/HTTP;
  - respons dianggap tidak valid bila tidak memuat judul "Jumlah Stunting" dan tabel;
  - bila tetap gagal -> FALLBACK: berkas simpanan manual
    data/source/eksternal/stunting_<tahun>.html ("Save page as" dari browser);
    kegagalan web dicatat di dq.dq_log (WEB01) dan batch_log;
  - bila fallback juga tidak ada -> task GAGAL (Airflow akan retry).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

import requests

from common.batch import catat_task, tulis_dq_log_mandiri
from common.config import SUMBER_DIR, ZONA_WAKTU
from common.errors import SumberTidakTersediaError
from common.lake import buat_klien_s3, catat_manifest, simpan_objek_raw

log = logging.getLogger(__name__)

SUMBER_WEB = {
    2024: "https://satudata.banjarmasinkota.go.id/data-statistik/bbeb5ace-d013-43d1-a948-198b8012940b",
    2025: "https://satudata.banjarmasinkota.go.id/data-statistik/32e25001-5c8f-4b01-ae2c-efbcde39c695",
}
USER_AGENT = "posyandu-pipeline/1.0 (tugas akademik; data terbuka)"
PENANDA_VALID = (b"Jumlah Stunting", b"<table")


class HalamanTidakValid(RuntimeError):
    """Portal merespons, tetapi isinya bukan halaman data yang diharapkan."""


def kode_sumber(tahun: int) -> str:
    return f"e2_stunting_{tahun}"


def unduh_halaman(url: str, percobaan: int = 3, jeda_awal: float = 5.0) -> tuple[bytes, int, str]:
    """GET dengan retry. Mengembalikan (isi, status HTTP, content type)."""
    galat: Exception | None = None
    for ke in range(1, percobaan + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=(10, 60))
            resp.raise_for_status()
            if not all(p in resp.content for p in PENANDA_VALID):
                raise HalamanTidakValid(f"respons {len(resp.content)} byte tanpa judul/tabel data stunting")
            return resp.content, resp.status_code, resp.headers.get("Content-Type", "text/html")
        except (requests.RequestException, HalamanTidakValid) as err:
            galat = err
            if ke < percobaan:
                jeda = jeda_awal * 2 ** (ke - 1)
                log.warning("unduh %s gagal (percobaan %d/%d): %s — coba lagi dalam %.0f detik",
                            url, ke, percobaan, err, jeda)
                time.sleep(jeda)
    raise SumberTidakTersediaError(f"portal gagal setelah {percobaan} percobaan: {type(galat).__name__}: {galat}")


def ingest_stunting(batch_id: str, tahun_list: tuple[int, ...] = (2024, 2025),
                    percobaan_ke: int | None = None) -> dict[int, str]:
    hasil_per_tahun: dict[int, str] = {}
    with catat_task(batch_id, "ingest_web", "ingestion", percobaan_ke) as hasil:
        s3 = buat_klien_s3()
        catatan = []
        for tahun in tahun_list:
            url = SUMBER_WEB[tahun]
            sumber = kode_sumber(tahun)
            waktu_ambil = datetime.now(ZONA_WAKTU)
            try:
                isi, status, ctype = unduh_halaman(url)
                cara, asal = "web", url
            except SumberTidakTersediaError as err_web:
                cadangan = SUMBER_DIR / "eksternal" / f"stunting_{tahun}.html"
                tulis_dq_log_mandiri(batch_id, "ingestion", [{
                    "kode_aturan": "WEB01", "tingkat": "PERINGATAN", "objek": sumber, "nilai": url,
                    "keterangan": f"pengambilan web gagal ({err_web}); mencoba fallback {cadangan.name}",
                }])
                if not cadangan.is_file():
                    raise SumberTidakTersediaError(
                        f"E2 {tahun}: {err_web}; berkas fallback eksternal/{cadangan.name} juga tidak ada"
                    ) from err_web
                isi, status, ctype = cadangan.read_bytes(), None, "text/html"
                cara, asal = "fallback_manual", f"eksternal/{cadangan.name}"
                log.warning("E2 %s memakai berkas fallback manual %s", tahun, cadangan.name)

            unggah = simpan_objek_raw(s3, sumber, f"stunting_{tahun}.html", isi, ctype, asal, waktu_ambil)
            catat_manifest(batch_id, sumber, cara, asal, unggah, ctype, waktu_ambil, status)
            hasil_per_tahun[tahun] = unggah.object_key
            catatan.append(f"{tahun}: {cara} -> {unggah.object_key} ({unggah.status_unggah})")

        hasil.jumlah_baris = len(hasil_per_tahun)
        hasil.pesan = "; ".join(catatan)
    return hasil_per_tahun
