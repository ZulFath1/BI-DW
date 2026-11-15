"""Jenis error pipeline. Dibedakan agar log dan batch_log menjelaskan penyebabnya:
struktur sumber berubah, data korup, atau sumber tidak tersedia."""


class StrukturSumberError(ValueError):
    """Header/kolom/sheet sumber tidak sesuai yang diharapkan (mis. format berkas berubah).
    Pipeline berhenti agar data yang salah posisi tidak masuk ke staging."""


class DataKorupError(ValueError):
    """Isi berkas rusak: kosong, bukan format yang diharapkan, atau checksum tidak cocok
    dengan manifest data lake."""


class SumberTidakTersediaError(RuntimeError):
    """Sumber tidak dapat diambil (berkas tidak ada, portal web gagal, dan fallback tidak ada)."""
