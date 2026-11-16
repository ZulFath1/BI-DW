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

## Berkas privat lain yang tidak di-commit

| Letak | Berkas | Keterangan |
|---|---|---|
| `etl/pemetaan_posyandu_manual.csv` | tabel pemetaan nama posyandu Antrop/Kader → `posyandu_id` (9 baris, ditinjau manual) | **Bagian dari data privat:** berisi nama posyandu dan kelurahan persis seperti di dataset. Tidak di-commit (pola `*.csv`). Tanpa berkas ini pipeline tetap berjalan: 8 nama tetap cocok lewat pencocokan fuzzy (PERINGATAN T14) dan 1 posyandu Antrop (Lestari IV) ditahan, sehingga fakta alat menjadi 4.890 baris, bukan 4.900 (lihat `docs/KEPUTUSAN.md` K25, K26, K36). Format kolom: `sumber, kelurahan, kunci_sumber, kelurahan_tujuan, kunci_tujuan, keterangan` (kunci = nama ternormalisasi T05) |
| `data/uji/` | `Laporan_uji_data_buruk.xlsx` | Salinan UJI (dibuat `tests/buat_data_uji.py` dari berkas asli; satu baris sheet 2025 digandakan dan satu sel persen diisi teks). Hanya dipakai DAG run dengan parameter `skenario_uji=data_buruk`. Karena turunan dari dataset privat, tidak di-commit |
