"""Ingestion berkas lokal (Excel internal dan PDF Disdukcapil) ke raw zone data lake.

Alur per berkas:
  1. baca berkas dari data/source (read-only);
  2. periksa tidak kosong dan formatnya benar (tanda tangan berkas: xlsx = 'PK', pdf = '%PDF');
     bila rusak -> dicatat di dq.quarantine dan task GAGAL (data korup tidak diteruskan);
  3. hitung SHA-256; bila berbeda dari checksum berkas pengembangan -> PERINGATAN di dq_log
     (wajar bila dinas mengirim versi baru, tetapi harus terlihat);
  4. unggah apa adanya ke bucket raw (`sumber/tanggal_ingest/nama_berkas`) tanpa menimpa objek lama;
  5. catat di dq.ingest_manifest (lineage) dan dq.batch_log (status).
"""

from __future__ import annotations

import logging
from datetime import datetime

from common.batch import catat_quarantine, catat_task, tulis_dq_log_mandiri
from common.config import SUMBER_DIR, ZONA_WAKTU
from common.errors import DataKorupError, SumberTidakTersediaError
from common.lake import buat_klien_s3, catat_manifest, sha256_hex, simpan_objek_raw

log = logging.getLogger(__name__)

SUMBER_LOKAL = {
    "internal_laporan_posyandu": {
        "berkas": "Laporan Kunjungan Posyandu.xlsx",
        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "tanda_tangan": b"PK",  # xlsx adalah arsip zip
        "sha256_pengembangan": "f21c45b8000fa992f46cfc348fa942da88d51420d3df57cd1bd52af77159ebe5",
    },
    "e1_disdukcapil_agregat_2025s2": {
        "berkas": "Buku Data Agregat Kependudukan Kota Banjarmasin Semester 2 Tahun 2025.pdf",
        "content_type": "application/pdf",
        "tanda_tangan": b"%PDF",
        "sha256_pengembangan": "2382f1bc0c8a878a5e86c08f4ac0a5e25e82a2cf76370cb1d2a56ceea9398d3d",
    },
}


def ingest_berkas_lokal(batch_id: str, sumber: str, percobaan_ke: int | None = None) -> str:
    cfg = SUMBER_LOKAL[sumber]
    with catat_task(batch_id, f"ingest_{sumber}", "ingestion", percobaan_ke) as hasil:
        path = SUMBER_DIR / cfg["berkas"]
        if not path.is_file():
            raise SumberTidakTersediaError(f"berkas sumber tidak ditemukan: {cfg['berkas']} (lihat data/README.md)")

        waktu_ambil = datetime.now(ZONA_WAKTU)
        isi = path.read_bytes()
        sha = sha256_hex(isi)

        if not isi.startswith(cfg["tanda_tangan"]):
            alasan = "berkas kosong" if not isi else "format berkas tidak sesuai (tanda tangan berkas salah)"
            catat_quarantine(batch_id, "ingestion", sumber, cfg["berkas"], alasan,
                             {"ukuran_byte": len(isi), "sha256": sha})
            raise DataKorupError(f"{cfg['berkas']}: {alasan}")

        if sha != cfg["sha256_pengembangan"]:
            tulis_dq_log_mandiri(batch_id, "ingestion", [{
                "kode_aturan": "ING01", "tingkat": "PERINGATAN", "objek": cfg["berkas"],
                "nilai": sha,
                "keterangan": "checksum berbeda dari berkas saat pengembangan; kemungkinan versi baru dari dinas",
            }])
            log.warning("%s: checksum berbeda dari berkas pengembangan", cfg["berkas"])

        unggah = simpan_objek_raw(buat_klien_s3(), sumber, cfg["berkas"], isi, cfg["content_type"],
                                  cfg["berkas"], waktu_ambil)
        catat_manifest(batch_id, sumber, "lokal", cfg["berkas"], unggah, cfg["content_type"], waktu_ambil)

        hasil.jumlah_baris = 1
        hasil.pesan = (f"{unggah.bucket}/{unggah.object_key} ({unggah.status_unggah}), "
                       f"{unggah.ukuran_byte} byte, sha256 {sha[:12]}…")
        return unggah.object_key


def ingest_excel(batch_id: str, percobaan_ke: int | None = None) -> str:
    return ingest_berkas_lokal(batch_id, "internal_laporan_posyandu", percobaan_ke)


def ingest_pdf(batch_id: str, percobaan_ke: int | None = None) -> str:
    return ingest_berkas_lokal(batch_id, "e1_disdukcapil_agregat_2025s2", percobaan_ke)
