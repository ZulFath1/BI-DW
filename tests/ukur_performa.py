"""Bahan performa & skalabilitas: durasi tiap task run sukses terakhir dan ukuran data per layer.

    docker compose exec airflow-scheduler python -m tests.ukur_performa > docs/bukti/27_performa_skalabilitas.txt

Isi:
  1. durasi setiap task pada DAG run `posyandu_pipeline` sukses terakhir (metadata Airflow) + jalur kritis;
  2. statistik durasi run sukses (semua run sukses yang tercatat);
  3. ukuran data per layer: raw zone (bucket raw), stg, curated, dq, dw, mart (ukuran di disk termasuk indeks,
     dan jumlah baris), database metadata Airflow, hasil backup.
Hanya angka; tidak ada isi data.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from sqlalchemy import text

from common.db import buka_koneksi
from common.lake import bucket_backup, bucket_raw, buat_klien_s3

DAG = "posyandu_pipeline"


def mb(n: float) -> str:
    return f"{n / 1_048_576:,.2f} MB".replace(",", "_").replace(".", ",").replace("_", ".")


def durasi() -> None:
    with create_session() as s:
        runs = (s.query(DagRun).filter(DagRun.dag_id == DAG, DagRun.state == "success")
                .order_by(DagRun.end_date.desc()).all())
        dr = runs[0]
        print(f"## 1. Durasi task — run sukses terakhir `{dr.run_id}`")
        print(f"mulai {dr.start_date:%Y-%m-%d %H:%M:%S} UTC, selesai {dr.end_date:%H:%M:%S} UTC, "
              f"total {(dr.end_date - dr.start_date).total_seconds():.0f} detik\n")
        tis = (s.query(TaskInstance).filter(TaskInstance.dag_id == DAG, TaskInstance.run_id == dr.run_id)
               .order_by(TaskInstance.start_date).all())
        print(f"{'task':34} {'mulai (+detik)':>15} {'durasi (detik)':>15}")
        for ti in tis:
            if ti.state == "skipped":
                print(f"{ti.task_id:34} {'':>15} {'dilewati':>15}")
                continue
            print(f"{ti.task_id:34} {(ti.start_date - dr.start_date).total_seconds():15.1f} {ti.duration or 0:15.1f}")
        top = sorted((t for t in tis if t.duration), key=lambda t: -t.duration)[:4]
        print("\nTask terlama: " + ", ".join(f"{t.task_id} {t.duration:.1f} s" for t in top))
        dur = sorted((r.end_date - r.start_date).total_seconds() for r in runs)
        print(f"\n## 2. Statistik {len(dur)} run sukses: min {dur[0]:.0f} s, median {dur[len(dur) // 2]:.0f} s, "
              f"maks {dur[-1]:.0f} s")
        print("(run sebelum 16 Nov memakai kode tahap lebih awal: tanpa mart/ML atau dengan uji kegagalan; "
              "penyebab selisih durasi tidak diukur terpisah)")


def ukuran() -> None:
    print("\n## 3. Ukuran data per layer")
    s3 = buat_klien_s3()
    for b in (bucket_raw(), bucket_backup()):
        obj = [o for h in s3.get_paginator("list_objects_v2").paginate(Bucket=b) for o in h.get("Contents", [])]
        uji = [o for o in obj if o["Key"].startswith("uji/")]
        print(f"bucket {b:7}: {len(obj):3} objek, {mb(sum(o['Size'] for o in obj))}"
              + (f" (termasuk {len(uji)} objek uji/ {mb(sum(o['Size'] for o in uji))})" if uji else ""))
    conn = buka_koneksi()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT n.nspname, c.relname, pg_total_relation_size(c.oid)
                           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE c.relkind = 'r' AND n.nspname IN ('stg', 'curated', 'dq', 'dw', 'mart')""")
            tabel = cur.fetchall()
            print(f"\n{'schema':8} {'tabel':>5} {'baris':>10} {'ukuran disk':>12}  tabel terbesar")
            for sc in ("stg", "curated", "dq", "dw", "mart"):
                t = [r for r in tabel if r[0] == sc]
                baris = {}
                for _, nama, _ in t:
                    cur.execute(f'SELECT count(*) FROM "{sc}"."{nama}"')
                    baris[nama] = cur.fetchone()[0]
                besar = max(t, key=lambda r: r[2])
                print(f"{sc:8} {len(t):5} {sum(baris.values()):10,} {mb(sum(r[2] for r in t)):>12}  "
                      f"{besar[1]} ({baris[besar[1]]:,} baris, {mb(besar[2])})")
            cur.execute("SELECT pg_database_size(current_database())")
            print(f"database DWH total (termasuk katalog): {mb(cur.fetchone()[0])}")
    finally:
        conn.close()
    with create_session() as s:
        n = s.execute(text("SELECT pg_database_size(current_database())")).scalar()
        print(f"database metadata Airflow: {mb(n)}")
    bk = Path(os.environ.get("BACKUP_DIR", "/opt/airflow/project/backup/output"))
    if bk.exists():
        tot = sum(p.stat().st_size for p in bk.rglob("*") if p.is_file())
        dumps = sorted(p for p in (bk / "db").iterdir() if p.is_dir())
        print(f"backup lokal: {mb(tot)} ({len(dumps)} dump database + mirror raw zone)")


if __name__ == "__main__":
    print("# Performa & ukuran data — pipeline posyandu\n")
    durasi()
    ukuran()
