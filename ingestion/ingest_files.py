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
from common.config import BERKAS_UJI, SUMBER_DIR, UJI_DIR, ZONA_WAKTU
from common.errors import DataKorupError, SumberTidakTersediaError
from common.lake import HasilUnggah, bucket_raw, buat_klien_s3, catat_manifest, sha256_hex, simpan_objek_raw

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


def ingest_berkas_lokal(batch_id: str, sumber: str, percobaan_ke: int | None = None,
                        skenario_uji: str | None = None) -> str:
    """skenario_uji (hanya untuk uji kegagalan, parameter DAG): mis. 'data_buruk' -> membaca SALINAN
    UJI dari data/uji/, bukan data asli. Objeknya disimpan di raw zone dengan awalan `uji/` sehingga
    tidak tercampur dengan berkas asli, dan dq_log mencatat bahwa run ini memakai data uji."""
    cfg = SUMBER_LOKAL[sumber]
    with catat_task(batch_id, f"ingest_{sumber}", "ingestion", percobaan_ke) as hasil:
        uji = skenario_uji not in (None, "", "normal")
        if uji:
            if skenario_uji not in BERKAS_UJI:
                raise ValueError(f"skenario uji tidak dikenal: {skenario_uji}")
            path = UJI_DIR / BERKAS_UJI[skenario_uji]
            tulis_dq_log_mandiri(batch_id, "ingestion", [{
                "kode_aturan": "UJI01", "tingkat": "PERINGATAN", "objek": path.name, "nilai": skenario_uji,
                "keterangan": "run memakai SALINAN UJI (bukan data asli) untuk menguji quality gate & karantina"}])
        else:
            path = SUMBER_DIR / cfg["berkas"]
        if not path.is_file():
            if uji:
                raise SumberTidakTersediaError(f"berkas uji tidak ditemukan: {path.name} (tests/buat_data_uji.py)")
            # Pemulihan / proses ulang: berkas lokal hilang, tetapi raw zone menyimpan salinan asli.
            return _dari_raw_zone(batch_id, sumber, cfg, hasil)

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

        prefiks = f"uji/{sumber}" if uji else sumber
        lokasi = f"uji/{path.name}" if uji else cfg["berkas"]
        unggah = simpan_objek_raw(buat_klien_s3(), prefiks, path.name, isi, cfg["content_type"], lokasi, waktu_ambil)
        catat_manifest(batch_id, sumber, "lokal", lokasi, unggah, cfg["content_type"], waktu_ambil)

        hasil.jumlah_baris = 1
        hasil.pesan = (f"{unggah.bucket}/{unggah.object_key} ({unggah.status_unggah}), "
                       f"{unggah.ukuran_byte} byte, sha256 {sha[:12]}…")
        return unggah.object_key


def _dari_raw_zone(batch_id: str, sumber: str, cfg: dict, hasil) -> str:
    """Fallback bila berkas di data/source tidak ada (mis. laptop/server diganti, lalu raw zone dipulihkan
    dari backup): pakai objek TERBARU sumber ini di raw zone. Isi diunduh dan SHA-256 dicocokkan dengan
    metadata objek; tidak ada objek baru yang ditulis. Manifest mencatat cara_ambil = 'raw_zone' dan
    dq_log mencatat PERINGATAN ING02, sehingga run ini terlihat sebagai proses ulang, bukan data baru."""
    s3 = buat_klien_s3()
    bucket = bucket_raw()
    objek = [o for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{sumber}/")
             for o in hal.get("Contents", [])]
    if not objek:
        raise SumberTidakTersediaError(f"berkas sumber tidak ditemukan: {cfg['berkas']} dan raw zone tidak "
                                       f"menyimpan objek '{sumber}/' (lihat data/README.md)")
    terbaru = max(objek, key=lambda o: (o["LastModified"], o["Key"]))
    key = terbaru["Key"]
    isi = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    sha = sha256_hex(isi)
    sha_meta = s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("sha256")
    if sha != sha_meta or not isi.startswith(cfg["tanda_tangan"]):
        catat_quarantine(batch_id, "ingestion", sumber, key, "objek raw zone tidak cocok checksum/format",
                         {"sha256": sha, "sha256_metadata": sha_meta})
        raise DataKorupError(f"objek raw zone {key} rusak (checksum/format tidak cocok)")
    waktu = datetime.now(ZONA_WAKTU)
    catat_manifest(batch_id, sumber, "raw_zone", f"raw_zone:{key}",
                   HasilUnggah(bucket, key, sha, len(isi), "sudah_ada"), cfg["content_type"], waktu)
    tulis_dq_log_mandiri(batch_id, "ingestion", [{
        "kode_aturan": "ING02", "tingkat": "PERINGATAN", "objek": cfg["berkas"], "nilai": key,
        "keterangan": "berkas lokal tidak ada; diproses ulang dari objek raw zone terbaru (checksum cocok)"}])
    log.warning("%s tidak ada di data/source — memakai raw zone %s/%s", cfg["berkas"], bucket, key)
    hasil.jumlah_baris = 1
    hasil.pesan = f"{bucket}/{key} (raw_zone, proses ulang), {len(isi)} byte, sha256 {sha[:12]}…"
    return key


def ingest_excel(batch_id: str, percobaan_ke: int | None = None, skenario_uji: str | None = None) -> str:
    return ingest_berkas_lokal(batch_id, "internal_laporan_posyandu", percobaan_ke, skenario_uji)


def ingest_pdf(batch_id: str, percobaan_ke: int | None = None) -> str:
    return ingest_berkas_lokal(batch_id, "e1_disdukcapil_agregat_2025s2", percobaan_ke)
