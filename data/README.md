# Folder data (tidak di-commit)

Dataset bersifat privat dan **tidak disertakan di repository**. Seluruh isi folder
`data/` diabaikan oleh git kecuali berkas README ini.

Untuk menjalankan pipeline, letakkan berkas berikut secara manual:

| Letak | Berkas | Keterangan |
|---|---|---|
| `data/source/` | `Laporan Kunjungan Posyandu.xlsx` | Sumber internal: sheet 2023, 2024, 2025, 2026, Antrop, Kader |
| `data/source/` | `Buku Data Agregat Kependudukan Kota Banjarmasin Semester 2 Tahun 2025.pdf` | Data terbuka Disdukcapil (Tabel 8–12: penduduk per kelurahan menurut kelompok umur) |
| `data/source/eksternal/` | `stunting_2024.html`, `stunting_2025.html` | **Opsional.** Fallback bila pengambilan otomatis dari portal Satu Data gagal: buka halaman data stunting di browser, lalu "Save page as" (HTML saja) dengan nama tersebut. Format CSV/Excel unduhan portal belum didukung (belum diverifikasi strukturnya) |

Checksum SHA-256 berkas yang dipakai saat pengembangan (untuk memastikan berkas sama):

| Berkas | SHA-256 |
|---|---|
| `Laporan Kunjungan Posyandu.xlsx` | `f21c45b8000fa992f46cfc348fa942da88d51420d3df57cd1bd52af77159ebe5` |
| `Buku Data Agregat Kependudukan Kota Banjarmasin Semester 2 Tahun 2025.pdf` | `2382f1bc0c8a878a5e86c08f4ac0a5e25e82a2cf76370cb1d2a56ceea9398d3d` |

Folder `data/source/` di-mount **read-only** ke container Airflow
(`/opt/airflow/data/source`), sehingga pipeline tidak dapat mengubah berkas sumber.
