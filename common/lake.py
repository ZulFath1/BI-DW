"""Akses data lake (object storage kompatibel S3: PGSTY Silo) dan manifest ingestion.

Aturan raw zone:
- Berkas disimpan APA ADANYA di bucket raw dengan kunci `sumber/tanggal_ingest/nama_berkas`.
- Objek yang sudah ada tidak pernah ditimpa. Bila isi sama (SHA-256 sama) -> tidak diunggah
  ulang ("sudah_ada"). Bila isi berbeda pada tanggal yang sama -> disimpan dengan awalan jam
  (HHMMSS_nama_berkas), sehingga versi lama tetap utuh untuk audit dan proses ulang.
- Setiap pengambilan dicatat di dq.ingest_manifest (checksum, ukuran, waktu, asal), dan
  staging hanya membaca objek yang tercatat di manifest batch-nya (lineage).
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from common.config import ZONA_WAKTU, wajib
from common.db import buka_koneksi
from common.errors import DataKorupError, SumberTidakTersediaError

log = logging.getLogger(__name__)


def bucket_raw() -> str:
    return os.environ.get("LAKE_BUCKET_RAW", "raw")


def bucket_backup() -> str:
    return os.environ.get("LAKE_BUCKET_BACKUP", "backup")


def buat_klien_s3():
    """Klien S3 ke data lake lokal.

    - addressing_style "path": URL http://host:9000/bucket/objek, cocok untuk object storage lokal.
    - retries: botocore mencoba ulang otomatis bila koneksi sesaat gagal.
    """
    return boto3.client(
        "s3",
        endpoint_url=wajib("LAKE_ENDPOINT"),
        aws_access_key_id=wajib("LAKE_ROOT_USER"),
        aws_secret_access_key=wajib("LAKE_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=10,
            read_timeout=60,
        ),
    )


def sha256_hex(isi: bytes) -> str:
    return hashlib.sha256(isi).hexdigest()


def _nama_aman(nama: str) -> str:
    """Nama berkas untuk kunci objek: spasi -> '_', karakter selain huruf/angka/._- dibuang."""
    return re.sub(r"[^A-Za-z0-9._-]", "", nama.replace(" ", "_"))


@dataclass
class HasilUnggah:
    bucket: str
    object_key: str
    sha256: str
    ukuran_byte: int
    status_unggah: str  # 'diunggah' | 'sudah_ada'


def simpan_objek_raw(s3, sumber: str, nama_berkas: str, isi: bytes, content_type: str,
                     lokasi_asal: str, waktu_ambil: datetime) -> HasilUnggah:
    """Menyimpan berkas apa adanya ke raw zone tanpa menimpa objek lama."""
    bucket = bucket_raw()
    sha = sha256_hex(isi)
    tanggal = waktu_ambil.astimezone(ZONA_WAKTU).strftime("%Y-%m-%d")
    nama = _nama_aman(nama_berkas)
    key = f"{sumber}/{tanggal}/{nama}"

    try:
        lama = s3.head_object(Bucket=bucket, Key=key)
        if lama.get("Metadata", {}).get("sha256") == sha:
            log.info("objek %s/%s sudah ada dengan isi sama — tidak diunggah ulang", bucket, key)
            return HasilUnggah(bucket, key, sha, len(isi), "sudah_ada")
        key = f"{sumber}/{tanggal}/{waktu_ambil.astimezone(ZONA_WAKTU):%H%M%S}_{nama}"
        log.info("isi berbeda dari objek lama pada tanggal yang sama — disimpan sebagai %s", key)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=isi,
        ContentType=content_type,
        Metadata={
            # Metadata S3 harus ASCII: lokasi asal di-URL-encode.
            "sha256": sha,
            "sumber": sumber,
            "lokasi-asal": quote(lokasi_asal, safe=":/"),
            "waktu-ambil": waktu_ambil.isoformat(),
        },
    )
    # Verifikasi: ukuran objek di data lake harus sama dengan ukuran berkas.
    cek = s3.head_object(Bucket=bucket, Key=key)
    if cek["ContentLength"] != len(isi):
        raise DataKorupError(f"ukuran objek {key} di data lake ({cek['ContentLength']}) ≠ berkas ({len(isi)})")
    return HasilUnggah(bucket, key, sha, len(isi), "diunggah")


def catat_manifest(batch_id: str, sumber: str, cara_ambil: str, lokasi_asal: str, hasil: HasilUnggah,
                   content_type: str, waktu_ambil: datetime, http_status: int | None = None) -> int:
    """Mencatat pengambilan ke dq.ingest_manifest. Idempotent per (batch_id, sumber):
    bila task di-retry pada batch yang sama, baris lama diperbarui."""
    conn = buka_koneksi(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dq.ingest_manifest
                    (batch_id, sumber, cara_ambil, lokasi_asal, bucket, object_key, sha256,
                     ukuran_byte, content_type, http_status, status_unggah, waktu_ambil)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, sumber) DO UPDATE SET
                    cara_ambil = EXCLUDED.cara_ambil, lokasi_asal = EXCLUDED.lokasi_asal,
                    bucket = EXCLUDED.bucket, object_key = EXCLUDED.object_key,
                    sha256 = EXCLUDED.sha256, ukuran_byte = EXCLUDED.ukuran_byte,
                    content_type = EXCLUDED.content_type, http_status = EXCLUDED.http_status,
                    status_unggah = EXCLUDED.status_unggah, waktu_ambil = EXCLUDED.waktu_ambil
                RETURNING manifest_id
                """,
                (batch_id, sumber, cara_ambil, lokasi_asal, hasil.bucket, hasil.object_key, hasil.sha256,
                 hasil.ukuran_byte, content_type, http_status, hasil.status_unggah, waktu_ambil),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


@dataclass
class ObjekManifest:
    manifest_id: int
    sumber: str
    bucket: str
    object_key: str
    sha256: str
    isi: bytes


def baca_objek_batch(batch_id: str, sumber: str) -> ObjekManifest:
    """Mengambil objek raw milik batch ini (menurut manifest) dan memverifikasi checksum-nya.

    Bila checksum tidak cocok, berarti objek rusak/berubah di data lake -> DataKorupError
    (pemanggil mencatatnya ke quarantine). Staging tidak pernah membaca berkas lokal langsung.
    """
    conn = buka_koneksi()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT manifest_id, bucket, object_key, sha256 FROM dq.ingest_manifest "
                "WHERE batch_id = %s AND sumber = %s",
                (batch_id, sumber),
            )
            baris = cur.fetchone()
    finally:
        conn.close()
    if baris is None:
        raise SumberTidakTersediaError(
            f"manifest untuk sumber '{sumber}' pada batch '{batch_id}' tidak ada — jalankan ingestion dulu"
        )
    manifest_id, bucket, key, sha = baris
    isi = buat_klien_s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    if sha256_hex(isi) != sha:
        raise DataKorupError(f"checksum objek {bucket}/{key} tidak cocok dengan manifest {manifest_id}")
    return ObjekManifest(manifest_id, sumber, bucket, key, sha, isi)
