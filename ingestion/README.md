# ingestion/

Tahap **Extract / Ingestion**: mengambil berkas sumber dan menyimpannya **apa adanya** ke raw zone
data lake (bucket `raw`, kunci `sumber/tanggal_ingest/nama_berkas`), lalu mencatat checksum
SHA-256, ukuran, waktu ambil, dan asal berkas di `dq.ingest_manifest`.

| Berkas | Fungsi |
|---|---|
| `lake_setup.py` | Membuat bucket `raw` dan `backup` dengan boto3 dan mengaktifkan versioning (idempotent; dijalankan layanan `datalake-init`) |
| `ingest_files.py` | `ingest_excel`, `ingest_pdf`: berkas lokal `data/source/` → raw zone (cek tanda tangan berkas, checksum) |
| `ingest_web.py` | `ingest_stunting`: halaman E2 portal Satu Data (2024, 2025) → raw zone; retry 3× lalu fallback berkas manual |

## Kode sumber di data lake

| Kode `sumber` | Isi | Cara ambil |
|---|---|---|
| `internal_laporan_posyandu` | `Laporan Kunjungan Posyandu.xlsx` | lokal |
| `e1_disdukcapil_agregat_2025s2` | Buku Data Agregat Kependudukan Sem. II 2025 (PDF) | lokal |
| `e2_stunting_2024`, `e2_stunting_2025` | Halaman HTML "Jumlah Stunting Menurut Kelurahan" | web, fallback `data/source/eksternal/stunting_<tahun>.html` |

## Aturan raw zone

- Objek lama **tidak pernah ditimpa**. Isi sama (SHA-256 sama) → tidak diunggah ulang (`sudah_ada`).
  Isi berbeda pada tanggal yang sama → disimpan sebagai `HHMMSS_nama_berkas`.
- Versioning bucket aktif sebagai lapis pengaman kedua.
- Halaman web E2 memuat token formulir yang berubah setiap diakses, sehingga setiap pengambilan
  tersimpan sebagai objek baru walaupun isi tabelnya sama (±220 KB per pengambilan).

## Error handling

| Kejadian | Tindakan sistem |
|---|---|
| Berkas lokal tidak ada | `SumberTidakTersediaError`, task GAGAL di `dq.batch_log` |
| Berkas kosong / format salah | `DataKorupError`, dicatat di `dq.quarantine`, task GAGAL |
| Checksum berbeda dari berkas pengembangan | PERINGATAN `ING01` di `dq.dq_log` (bisa berarti versi baru dari dinas) |
| Portal web gagal / isi tidak valid | retry 3× (jeda 5 s, 10 s) → fallback berkas manual, PERINGATAN `WEB01`; tanpa fallback → task GAGAL |
| Data lake sesaat tidak tersedia | retry otomatis botocore (5 percobaan) |

Mengapa E2 tidak memakai tombol unduh CSV/Excel portal: tombol itu berupa formulir yang meminta
email, jenis kelamin, rentang usia, dan pekerjaan pengunduh. Pipeline tidak mengirim data pribadi
ke layanan luar.
