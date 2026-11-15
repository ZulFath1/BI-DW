"""Menyiapkan bucket data lake (raw & backup) memakai boto3.

Dijalankan oleh layanan `datalake-init` di docker-compose.yml setiap kali stack
dinyalakan. Idempotent: bucket yang sudah ada tidak diubah isinya, jadi aman
dijalankan berulang kali.

Selain membuat bucket, versioning diaktifkan: bila sebuah objek tertimpa atau
terhapus, versi lamanya tetap tersimpan (lapis pengaman tambahan untuk raw zone
dan backup). Bila object storage menolak versioning, hanya diberi peringatan.

Kenapa boto3 dan bukan `mc`: data lake memakai API S3, sehingga kode yang sama
bisa dipakai untuk object storage S3 lain. Pustaka boto3 juga sudah ada di image
Airflow, jadi tidak perlu image tambahan.
"""

from __future__ import annotations

import logging
import sys

from botocore.exceptions import BotoCoreError, ClientError

from common.config import KonfigurasiError
from common.lake import bucket_backup, bucket_raw, buat_klien_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s lake_setup: %(message)s")
log = logging.getLogger(__name__)


def pastikan_bucket(s3, nama: str) -> str:
    """Membuat bucket bila belum ada. Mengembalikan 'sudah ada' atau 'dibuat'."""
    try:
        s3.head_bucket(Bucket=nama)
        return "sudah ada"
    except ClientError as err:
        kode = err.response.get("Error", {}).get("Code", "")
        if kode not in ("404", "NoSuchBucket", "NotFound"):
            raise  # error lain (mis. akses ditolak) tidak boleh disembunyikan
    s3.create_bucket(Bucket=nama)
    return "dibuat"


def aktifkan_versioning(s3, nama: str) -> str:
    try:
        s3.put_bucket_versioning(Bucket=nama, VersioningConfiguration={"Status": "Enabled"})
        return s3.get_bucket_versioning(Bucket=nama).get("Status", "tidak diketahui")
    except ClientError as err:
        log.warning("versioning bucket '%s' tidak dapat diaktifkan: %s", nama, err)
        return "tidak aktif"


def main() -> int:
    try:
        s3 = buat_klien_s3()
        for nama in (bucket_raw(), bucket_backup()):
            status = pastikan_bucket(s3, nama)
            versi = aktifkan_versioning(s3, nama)
            log.info("bucket '%s': %s, versioning: %s", nama, status, versi)
        daftar = sorted(b["Name"] for b in s3.list_buckets().get("Buckets", []))
        log.info("bucket di data lake: %s", ", ".join(daftar))
    except KonfigurasiError as err:
        log.error("%s", err)
        return 1
    except (ClientError, BotoCoreError) as err:
        log.error("gagal menyiapkan data lake: %s", err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
