"""Backup database dan data lake (dipanggil DAG `posyandu_backup`, atau manual lewat CLI).

    docker compose exec airflow-scheduler python -m backup.backup            # satu putaran penuh

Yang di-backup:
  1. Database DWH `posyandu_dw` (schema stg, curated, dq, dw, mart) dan database metadata Airflow
     -> `pg_dump -Fc` (format custom, terkompresi) memakai klien PostgreSQL 16 (sama dengan server;
     klien 18 bawaan image bisa menulis perintah yang tidak dikenal PG 16 saat restore).
     Dump dibuat sebagai pemilik database masing-masing (etl_user / airflow_user), bukan superuser.
     Bersama dump disimpan manifest.json: SHA-256 & ukuran berkas, jumlah baris setiap tabel, dan
     checksum isi tabel dw & mart (dipakai restore untuk membuktikan data kembali utuh).
  2. Raw zone data lake (bucket raw) -> salinan berkas per objek (mirror inkremental) beserta
     metadata objek (sha256, sumber, lokasi asal, waktu ambil) di lake/manifest.json. Objek raw
     tidak pernah diubah, sehingga mirror cukup menambah objek baru (tidak pernah menghapus).

Lokasi (dua tempat, agar satu kerusakan tidak menghilangkan keduanya):
  - lokal: BACKUP_DIR (= folder backup/output/ di laptop, di luar volume Docker; diabaikan git);
  - bucket `backup` di data lake: salinan dump database (db/<stempel>/...).
  Keduanya masih di satu laptop: untuk produksi, folder backup/output perlu disalin ke penyimpanan
  lain (drive eksternal / cloud) — dicatat sebagai batasan di backup/README.md.

Retensi: 7 dump harian terbaru + dump terbaru dari 4 minggu ISO terakhir (lokal dan bucket).
Mirror raw zone tidak dirotasi (raw zone bersifat append-only).
Setiap langkah menulis status ke dq.batch_log (tahap 'backup').
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from common.batch import buat_batch_id, catat_task
from common.config import ZONA_WAKTU, wajib
from common.db import buka_koneksi
from common.lake import bucket_backup, bucket_raw, buat_klien_s3

log = logging.getLogger(__name__)

PG_BIN = Path(os.environ.get("PG16_BIN", "/usr/lib/postgresql/16/bin"))
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", "/opt/airflow/project/backup/output"))
DB_DIR, LAKE_DIR = BACKUP_DIR / "db", BACKUP_DIR / "lake"
SIMPAN_HARIAN, SIMPAN_MINGGUAN = 7, 4
SCHEMA_DWH = ("stg", "curated", "dq", "dw", "mart")
SCHEMA_CHECKSUM = ("dw", "mart")  # isi tabel dibandingkan saat uji restore


# ---------------------------------------------------------------- utilitas
def kredensial_db() -> list[dict]:
    """Database yang di-backup dan pemiliknya (dari .env lewat docker-compose.yml)."""
    return [{"nama": "posyandu_dw", "db": wajib("DW_DB"), "user": wajib("ETL_USER"), "password": wajib("ETL_PASSWORD")},
            {"nama": "airflow", "db": wajib("BACKUP_AIRFLOW_DB"), "user": wajib("BACKUP_AIRFLOW_DB_USER"),
             "password": wajib("BACKUP_AIRFLOW_DB_PASSWORD")}]


def jalankan_pg(perintah: list[str], password: str, timeout: int = 1800) -> str:
    """Menjalankan program klien PostgreSQL. Kata sandi lewat PGPASSWORD (tidak muncul di argumen/log)."""
    try:
        r = subprocess.run(perintah, env={**os.environ, "PGPASSWORD": password}, capture_output=True, text=True,
                           timeout=timeout, check=True)
    except subprocess.CalledProcessError as err:
        raise RuntimeError(f"{Path(perintah[0]).name} gagal (kode {err.returncode}): {err.stderr.strip()[-800:]}") from err
    return r.stdout


def sha256_berkas(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for blok in iter(lambda: f.read(1 << 20), b""):
            h.update(blok)
    return h.hexdigest()


def hitung_isi(cur, schema: tuple[str, ...], checksum: tuple[str, ...]) -> dict:
    """Jumlah baris semua tabel di schema tsb. + md5 isi tabel (urut teks baris) untuk schema `checksum`."""
    cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
                   WHERE table_type = 'BASE TABLE' AND table_schema = ANY(%s) ORDER BY 1, 2""", (list(schema),))
    hasil = {}
    for s, t in cur.fetchall():
        cur.execute(f'SELECT count(*) FROM "{s}"."{t}"')
        n = cur.fetchone()[0]
        md5 = None
        if s in checksum:
            cur.execute(f'SELECT md5(coalesce(string_agg(x::text, \'|\' ORDER BY x::text), \'\')) FROM "{s}"."{t}" x')
            md5 = cur.fetchone()[0]
        hasil[f"{s}.{t}"] = {"baris": n, "md5": md5}
    return hasil


def hitung_isi_airflow(kred: dict) -> dict:
    import psycopg2

    conn = psycopg2.connect(host=os.environ.get("DW_HOST", "postgres"), dbname=kred["db"], user=kred["user"],
                            password=kred["password"], connect_timeout=10)
    try:
        with conn.cursor() as cur:
            hasil = {}
            for t in ("dag_run", "task_instance", "dag"):
                cur.execute(f"SELECT count(*) FROM {t}")
                hasil[f"public.{t}"] = {"baris": cur.fetchone()[0], "md5": None}
            return hasil
    finally:
        conn.close()


# ---------------------------------------------------------------- 1. database
def backup_database(batch_id: str, percobaan_ke: int | None = None) -> str:
    with catat_task(batch_id, "backup_database", "backup", percobaan_ke) as hasil:
        stempel = datetime.now(ZONA_WAKTU).strftime("%Y%m%d_%H%M%S")
        folder = DB_DIR / stempel
        folder.mkdir(parents=True, exist_ok=False)
        manifest = {"stempel": stempel, "batch_id": batch_id, "dibuat_pada": datetime.now(ZONA_WAKTU).isoformat(),
                    "pg_dump": jalankan_pg([str(PG_BIN / "pg_dump"), "--version"], "").strip(), "database": {}}
        s3 = buat_klien_s3()
        try:
            for k in kredensial_db():
                # Jumlah baris dihitung tepat sebelum dump; backup dijadwalkan di luar jam pipeline (01.00 vs
                # 02.00 tanggal 10) agar isi tidak berubah di antara keduanya.
                if k["nama"] == "posyandu_dw":
                    conn = buka_koneksi()
                    try:
                        with conn, conn.cursor() as cur:
                            isi = hitung_isi(cur, SCHEMA_DWH, SCHEMA_CHECKSUM)
                    finally:
                        conn.close()
                else:
                    isi = hitung_isi_airflow(k)
                berkas = folder / f"{k['nama']}.dump"
                jalankan_pg([str(PG_BIN / "pg_dump"), "-h", os.environ.get("DW_HOST", "postgres"), "-U", k["user"],
                             "-d", k["db"], "-Fc", "-Z", "6", "--no-password", "-f", str(berkas)], k["password"])
                # Verifikasi: dump bisa dibaca pg_restore (daftar isi tidak kosong).
                toc = jalankan_pg([str(PG_BIN / "pg_restore"), "--list", str(berkas)], "")
                n_toc = sum(1 for b in toc.splitlines() if b and not b.startswith(";"))
                if n_toc == 0:
                    raise RuntimeError(f"dump {berkas.name} kosong")
                manifest["database"][k["nama"]] = {"berkas": berkas.name, "ukuran_byte": berkas.stat().st_size,
                                                   "sha256": sha256_berkas(berkas), "entri_toc": n_toc, "isi": isi}
                s3.upload_file(str(berkas), bucket_backup(), f"db/{stempel}/{berkas.name}")
            (folder / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
            s3.upload_file(str(folder / "manifest.json"), bucket_backup(), f"db/{stempel}/manifest.json")
        except BaseException:
            shutil.rmtree(folder, ignore_errors=True)  # backup setengah jadi tidak boleh dikira valid
            raise
        total = sum(v["ukuran_byte"] for v in manifest["database"].values())
        hasil.jumlah_baris = sum(t["baris"] for v in manifest["database"].values() for t in v["isi"].values())
        hasil.pesan = (f"db/{stempel}: " + ", ".join(f"{n} {v['ukuran_byte']:,} byte ({v['entri_toc']} entri TOC)"
                                                   for n, v in manifest["database"].items())
                       + f"; total {total:,} byte; disalin ke bucket {bucket_backup()}")
        return stempel


# ---------------------------------------------------------------- 2. data lake
def mirror_lake(batch_id: str, percobaan_ke: int | None = None) -> dict:
    """Menyalin setiap objek bucket raw ke BACKUP_DIR/lake/<bucket>/<key> (inkremental, terverifikasi)."""
    with catat_task(batch_id, "mirror_lake", "backup", percobaan_ke) as hasil:
        s3, bucket = buat_klien_s3(), bucket_raw()
        akar = LAKE_DIR / bucket
        path_manifest = LAKE_DIR / "manifest.json"
        lama = json.loads(path_manifest.read_text(encoding="utf-8"))["objek"] if path_manifest.exists() else {}
        baru, dilewati, objek = 0, 0, {}
        for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for o in hal.get("Contents", []):
                key = o["Key"]
                tujuan = akar / key
                kepala = s3.head_object(Bucket=bucket, Key=key)
                meta = kepala.get("Metadata", {})
                info = {"ukuran_byte": o["Size"], "etag": o["ETag"].strip('"'), "content_type": kepala.get("ContentType"),
                        "metadata": meta, "last_modified": o["LastModified"].isoformat()}
                if (tujuan.exists() and key in lama and lama[key]["etag"] == info["etag"]
                        and tujuan.stat().st_size == o["Size"]):
                    info["sha256"] = lama[key]["sha256"]
                    dilewati += 1
                else:
                    tujuan.parent.mkdir(parents=True, exist_ok=True)
                    sementara = tujuan.with_suffix(tujuan.suffix + ".part")
                    s3.download_file(bucket, key, str(sementara))
                    sha = sha256_berkas(sementara)
                    if meta.get("sha256") and meta["sha256"] != sha:
                        sementara.unlink(missing_ok=True)
                        raise RuntimeError(f"checksum unduhan {key} tidak cocok dengan metadata objek")
                    sementara.replace(tujuan)
                    info["sha256"] = sha
                    baru += 1
                objek[key] = info
        LAKE_DIR.mkdir(parents=True, exist_ok=True)
        path_manifest.write_text(json.dumps({"bucket": bucket, "diperbarui": datetime.now(ZONA_WAKTU).isoformat(),
                                             "objek": objek}, indent=1), encoding="utf-8")
        total = sum(v["ukuran_byte"] for v in objek.values())
        hasil.jumlah_baris = len(objek)
        hasil.pesan = f"{len(objek)} objek ({total:,} byte): {baru} disalin baru, {dilewati} sudah ada di mirror"
        return {"objek": len(objek), "baru": baru, "byte": total}


# ---------------------------------------------------------------- 3. retensi
def pilih_dihapus(stempel: list[str]) -> list[str]:
    """7 terbaru + yang terbaru dari tiap 4 minggu ISO terakhir dipertahankan; sisanya dihapus."""
    urut = sorted(stempel, reverse=True)
    simpan = set(urut[:SIMPAN_HARIAN])
    minggu = {}
    for s in urut:
        th, mg, _ = datetime.strptime(s, "%Y%m%d_%H%M%S").isocalendar()
        minggu.setdefault((th, mg), s)
    simpan |= set(sorted(minggu.values(), reverse=True)[:SIMPAN_MINGGUAN])
    return [s for s in urut if s not in simpan]


def retensi(batch_id: str, percobaan_ke: int | None = None) -> list[str]:
    with catat_task(batch_id, "retensi_backup", "backup", percobaan_ke) as hasil:
        lokal = [p.name for p in DB_DIR.iterdir() if p.is_dir()] if DB_DIR.exists() else []
        hapus = pilih_dihapus(lokal)
        s3 = buat_klien_s3()
        for s in hapus:
            shutil.rmtree(DB_DIR / s)
            for hal in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_backup(), Prefix=f"db/{s}/"):
                for o in hal.get("Contents", []):
                    s3.delete_object(Bucket=bucket_backup(), Key=o["Key"])
        hasil.jumlah_baris = len(hapus)
        hasil.pesan = (f"{len(lokal)} dump lokal; dihapus {len(hapus)} ({', '.join(hapus) or '-'}); aturan "
                       f"{SIMPAN_HARIAN} harian + {SIMPAN_MINGGUAN} mingguan")
        return hapus


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    b = "backup_" + buat_batch_id()
    backup_database(b)
    mirror_lake(b)
    retensi(b)
    print(f"SELESAI batch_id={b}")


if __name__ == "__main__":
    main()
