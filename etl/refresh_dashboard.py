"""Task refresh_dashboard: memberi tahu dashboard bahwa data baru tersedia.

Streamlit menyimpan hasil query di cache. Dashboard memakai batch_id terakhir di
mart.dashboard_refresh sebagai bagian kunci cache, sehingga begitu baris baru ditulis di sini,
kunjungan berikutnya otomatis membaca data baru (tanpa restart container).

Task juga memeriksa:
  1. mart berisi data batch ini (build_mart dan train_ml sukses);
  2. endpoint kesehatan Streamlit menjawab (dashboard hidup). Bila dashboard mati, task GAGAL
     -> retry Airflow -> callback; data di mart tetap benar, hanya tampilannya yang tertunda.
"""

from __future__ import annotations

import logging
import os
import urllib.request

from common.batch import catat_task
from common.db import buka_koneksi

log = logging.getLogger(__name__)
URL_SEHAT = os.environ.get("DASHBOARD_HEALTH_URL", "http://dashboard:8501/_stcore/health")


def refresh_dashboard(batch_id: str, percobaan_ke: int | None = None) -> dict:
    with catat_task(batch_id, "refresh_dashboard", "dashboard", percobaan_ke) as hasil:
        conn = buka_koneksi()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""SELECT (SELECT count(*) FROM mart.posyandu_profil WHERE batch_id = %(b)s),
                                      (SELECT count(*) FROM mart.hasil_cluster WHERE batch_id = %(b)s)""",
                            {"b": batch_id})
                n_profil, n_cluster = cur.fetchone()
                if n_profil == 0 or n_cluster == 0:
                    raise RuntimeError(f"mart belum berisi batch {batch_id} (profil {n_profil}, cluster {n_cluster})")
                cur.execute("INSERT INTO mart.dashboard_refresh (batch_id, keterangan) VALUES (%s, %s)",
                            (batch_id, f"posyandu_profil {n_profil}, hasil_cluster {n_cluster}"))
        finally:
            conn.close()
        with urllib.request.urlopen(URL_SEHAT, timeout=10) as r:
            status = r.status
        if status != 200:
            raise RuntimeError(f"dashboard tidak sehat (HTTP {status})")
        hasil.jumlah_baris = n_cluster
        hasil.pesan = f"dashboard sehat (HTTP {status}); cache memakai batch {batch_id}"
        return {"batch_id": batch_id, "profil": n_profil, "cluster": n_cluster}
