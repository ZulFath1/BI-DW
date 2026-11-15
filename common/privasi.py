"""Pseudonimisasi nama kader (T12).

Nama kader diganti `kader_kode` = HMAC-SHA256(kunci rahasia, nama ternormalisasi).
- Berkunci: tanpa PSEUDONYM_KEY, kode tidak bisa dibuat ulang dari daftar nama
  (berbeda dengan hash biasa yang bisa ditebak dengan mencoba nama-nama umum).
- Deterministik: nama yang sama -> kode yang sama, sehingga kader ganda tetap
  terdeteksi tanpa menyimpan namanya.
Kunci disimpan di .env (PSEUDONYM_KEY), tidak pernah di kode atau repository.
"""

from __future__ import annotations

import hashlib
import hmac

from common.config import KonfigurasiError, wajib

PANJANG_KUNCI_MIN = 32


def ambil_kunci() -> bytes:
    kunci = wajib("PSEUDONYM_KEY")
    if len(kunci) < PANJANG_KUNCI_MIN:
        raise KonfigurasiError(f"PSEUDONYM_KEY terlalu pendek (minimal {PANJANG_KUNCI_MIN} karakter)")
    return kunci.encode("utf-8")


def normalisasi_nama(nama: str) -> str:
    """Spasi berlebih dibuang dan huruf diseragamkan agar 'Siti  Aminah ' = 'SITI AMINAH'."""
    return " ".join(str(nama).split()).upper()


def pseudonim_kader(nama: str | None, kunci: bytes) -> str | None:
    """Mengembalikan kode 'KDR-' + 16 karakter heksadesimal (64 bit), atau None bila nama kosong."""
    if nama is None or not str(nama).strip():
        return None
    digest = hmac.new(kunci, normalisasi_nama(nama).encode("utf-8"), hashlib.sha256).hexdigest()
    return "KDR-" + digest[:16]
