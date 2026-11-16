"""Restore database dan raw zone dari hasil backup (backup/backup.py), lalu verifikasi.

Dipakai saat pemulihan bencana (server/laptop baru, volume Docker hilang) dan untuk uji restore berkala.
Dijalankan di container Airflow (punya klien PostgreSQL 16, boto3, dan kredensial dari .env):

    # stack baru yang masih kosong: hanya postgres + datalake (+ datalake-init) yang dinyalakan
    docker compose run --rm --no-deps airflow-scheduler python -m backup.restore semua            # dump terbaru
    docker compose run --rm --no-deps airflow-scheduler python -m backup.restore db --stempel 20261116_120000
    docker compose run --rm --no-deps airflow-scheduler python -m backup.restore lake
    docker compose run --rm --no-deps airflow-scheduler python -m backup.restore verifikasi

Urutan `semua`: (1) restore database DWH dan metadata Airflow dengan pg_restore --clean --if-exists
--single-transaction (gagal di tengah -> tidak ada perubahan); (2) unggah ulang setiap objek raw zone dengan
metadata aslinya (sha256 dll.), objek yang sudah ada dengan checksum sama dilewati; (3) verifikasi: jumlah
baris semua tabel dan checksum isi tabel dw & mart harus sama dengan manifest saat backup, dan setiap objek
raw zone harus ada dengan sha256 yang sama. Keluar dengan kode 1 bila ada selisih.
Restore dijalankan sebagai pemilik database (etl_user / airflow_user); role & database kosong dibuat oleh
sql/init/00_init.sh saat volume PostgreSQL baru.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from backup.backup import (DB_DIR, LAKE_DIR, PG_BIN, SCHEMA_CHECKSUM, SCHEMA_DWH, hitung_isi, hitung_isi_airflow,
                           jalankan_pg, kredensial_db, sha256_berkas)
from common.db import buka_koneksi
from common.lake import buat_klien_s3

log = logging.getLogger(__name__)


def folder_dump(stempel: str | None) -> Path:
    semua = sorted(p for p in DB_DIR.iterdir() if (p / "manifest.json").exists()) if DB_DIR.exists() else []
    if not semua:
        raise FileNotFoundError(f"tidak ada backup database di {DB_DIR}")
    if stempel:
        f = DB_DIR / stempel
        if f not in semua:
            raise FileNotFoundError(f"backup {stempel} tidak ada")
        return f
    return semua[-1]


def restore_db(folder: Path) -> None:
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    for k in kredensial_db():
        m = manifest["database"][k["nama"]]
        berkas = folder / m["berkas"]
        if sha256_berkas(berkas) != m["sha256"]:
            raise RuntimeError(f"{berkas.name}: checksum berbeda dari manifest — dump rusak, restore dibatalkan")
        log.info("restore %s -> database %s", berkas.name, k["db"])
        jalankan_pg([str(PG_BIN / "pg_restore"), "-h", os.environ.get("DW_HOST", "postgres"), "-U", k["user"],
                     "-d", k["db"], "--clean", "--if-exists", "--no-owner", "--single-transaction", "--exit-on-error",
                     "--no-password", str(berkas)], k["password"])
        print(f"database {k['db']}: dipulihkan dari {folder.name}/{berkas.name} ({m['ukuran_byte']:,} byte)")


def restore_lake() -> None:
    manifest = json.loads((LAKE_DIR / "manifest.json").read_text(encoding="utf-8"))
    s3, bucket = buat_klien_s3(), manifest["bucket"]
    naik = lewat = 0
    for key, info in manifest["objek"].items():
        try:
            if s3.head_object(Bucket=bucket, Key=key).get("Metadata", {}).get("sha256") == info["sha256"]:
                lewat += 1
                continue
        except s3.exceptions.ClientError:
            pass
        berkas = LAKE_DIR / bucket / key
        if sha256_berkas(berkas) != info["sha256"]:
            raise RuntimeError(f"mirror {key} rusak (checksum berbeda dari manifest)")
        s3.upload_file(str(berkas), bucket, key, ExtraArgs={"Metadata": info["metadata"],
                                                            "ContentType": info["content_type"] or "binary/octet-stream"})
        naik += 1
    print(f"raw zone: {naik} objek diunggah ulang, {lewat} sudah ada (bucket {bucket})")


def verifikasi(folder: Path) -> bool:
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    selisih = []
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            sekarang = hitung_isi(cur, SCHEMA_DWH, SCHEMA_CHECKSUM)
    finally:
        conn.close()
    af = next(k for k in kredensial_db() if k["nama"] == "airflow")
    sekarang_af = hitung_isi_airflow(af)
    n_tabel = n_md5 = 0
    for nama, isi_now in (("posyandu_dw", sekarang), ("airflow", sekarang_af)):
        acuan = manifest["database"][nama]["isi"]
        for t, a in acuan.items():
            b = isi_now.get(t)
            n_tabel += 1
            if b is None:
                selisih.append(f"{nama}: tabel {t} tidak ada")
            elif b["baris"] != a["baris"]:
                selisih.append(f"{nama}: {t} {b['baris']} baris, backup {a['baris']}")
            elif a["md5"]:
                n_md5 += 1
                if b["md5"] != a["md5"]:
                    selisih.append(f"{nama}: isi {t} berbeda (md5)")
    print(f"database: {n_tabel} tabel dibandingkan (jumlah baris), {n_md5} tabel dw/mart dibandingkan isinya (md5)")

    lake = json.loads((LAKE_DIR / "manifest.json").read_text(encoding="utf-8"))
    s3 = buat_klien_s3()
    for key, info in lake["objek"].items():
        try:
            meta = s3.head_object(Bucket=lake["bucket"], Key=key).get("Metadata", {})
            if meta.get("sha256") != info["sha256"]:
                selisih.append(f"lake: {key} checksum berbeda")
        except s3.exceptions.ClientError:
            selisih.append(f"lake: {key} tidak ada")
    print(f"raw zone: {len(lake['objek'])} objek dibandingkan (sha256)")
    for s in selisih:
        print("SELISIH", s)
    print("VERIFIKASI", "LULUS: data identik dengan backup" if not selisih else f"GAGAL: {len(selisih)} selisih")
    return not selisih


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="python -m backup.restore")
    p.add_argument("perintah", choices=["db", "lake", "verifikasi", "semua"])
    p.add_argument("--stempel", help="folder backup/output/db/<stempel>; default terbaru")
    a = p.parse_args(argv)
    folder = folder_dump(a.stempel)
    print(f"backup dipakai: db/{folder.name}")
    if a.perintah in ("db", "semua"):
        restore_db(folder)
    if a.perintah in ("lake", "semua"):
        restore_lake()
    if a.perintah in ("verifikasi", "semua"):
        return 0 if verifikasi(folder) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
