"""Konfigurasi pipeline. Semua nilai rahasia dibaca dari environment (.env),
tidak pernah ditulis di kode."""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

# Waktu lokal Banjarmasin (WITA, UTC+8) dipakai untuk tanggal ingest di data lake.
ZONA_WAKTU = ZoneInfo("Asia/Makassar")

# Folder berkas sumber (di-mount read-only ke container).
SUMBER_DIR = Path(os.environ.get("DATA_SOURCE_DIR", "/opt/airflow/data/source"))

# Folder SQL (DDL & query validasi) di container.
SQL_DIR = Path(os.environ.get("SQL_DIR", "/opt/airflow/project/sql"))


class KonfigurasiError(RuntimeError):
    """Variabel lingkungan wajib belum diisi."""


def wajib(nama: str) -> str:
    """Mengambil variabel lingkungan wajib; gagal dengan pesan jelas bila kosong."""
    nilai = os.environ.get(nama)
    if not nilai:
        raise KonfigurasiError(f"variabel lingkungan {nama} belum diisi (lihat .env.example)")
    return nilai
