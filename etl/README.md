# etl/

Tahap **Staging → Transform → Curated → Load** (aturan T01–T19).

| Berkas | Fungsi | Status |
|---|---|---|
| `schema.py` | Task `init_schema`: menjalankan semua `sql/ddl/*.sql` dalam satu transaksi (idempotent) | ada |
| `staging_util.py` | Ambil objek dari data lake (verifikasi checksum), konversi sel → teks asli, muat ulang atomik | ada |
| `staging_excel.py` | `staging_laporan` (sheet 2023–2026), `staging_antrop`, `staging_kader` | ada |
| `staging_pdf.py` | `staging_penduduk`: Tabel 6, 7, 8–12 buku Disdukcapil (E1) | ada |
| `staging_web.py` | `staging_stunting`: tabel halaman Satu Data (E2) 2024 & 2025 | ada |
| `normalisasi.py` | T04 rapikan nama, T05 `posyandu_id`, pencocokan nama Antrop/Kader (kunci → manual → fuzzy) | ada |
| `pemetaan_posyandu_manual.csv` | Pemetaan nama posyandu yang sudah ditinjau manual (9 baris Antrop) | ada |
| `transform.py` | Task `transform`: T01–T19 dari `stg` ke `curated` (satu transaksi) | ada |
| `load_dw.py` | Task `load_dw`: `curated` → `dw` + pemeriksaan kritis Q04–Q06, ROLLBACK bila gagal | ada |
| `cli.py` | Menjalankan task secara manual (lihat di bawah) | ada |

## Transform (stg → curated)

| Bagian | Aturan | Hasil (15 Nov 2026) |
|---|---|---|
| kelurahan | T16: daftar resmi + kode wilayah dari E2 | 52 |
| posyandu | T04, T05: `posyandu_id`, SCD tipe 2 puskesmas | 490 posyandu, 503 versi |
| kunjungan | T03 persen teks → angka, T06/T15 status, T07, T08, T09/T10 `flag_anomali`, T11 | 106.020 baris; 2.073 baris anomali |
| alat | T14: isi kelurahan dari baris atas, pemetaan nama, rekonsiliasi baris total, tensimeter tidak dimuat | 4.900 (490 × 10) |
| kader | T12 masking/penyeragaman, T13 hapus jenis kelamin N/A | 2.542 (+2 T13, +1 ditahan) |
| penduduk E1 | T16–T17, K16 Kelayan Tengah dari urutan baris | 1.664 (52 × 16 × 2) |
| stunting E2 | T16, T18 rekonsiliasi total portal | 104 (52 × 2) |

Semua temuan dicatat di `dq.dq_log` (tahap `transform`); ringkasan per tahun (anomali, T07, T08, status)
ada sebagai entri INFO.

## Load (curated → dw)

Satu transaksi: upsert `dim_kelurahan` dan `dim_posyandu` (surrogate key stabil), hapus lalu sisip ulang
lima fakta dengan lookup surrogate key (versi SCD yang berlaku pada bulan fakta), lalu Q04 (rekonsiliasi
total staging vs fakta), Q05 (FK/baris hilang), Q06 (tidak ada kolom nama/alamat kader). Gagal → ROLLBACK.

## Prinsip staging

- Staging membaca **objek data lake** yang tercatat di manifest batch, bukan berkas lokal.
  Checksum diverifikasi; objek yang tidak cocok → `dq.quarantine`, task gagal.
- Nilai disimpan sebagai **teks asli** (mis. `'68.25'`, `'1.273'`, `'-'`); konversi tipe dilakukan
  di transform agar kegagalan konversi tercatat (T03), bukan hilang diam-diam.
- Struktur sumber divalidasi dulu (header 3 tingkat, nama kolom, kolom tambahan). Bila berbeda →
  `StrukturSumberError`, staging lama tidak berubah.
- Muat ulang penuh per batch: `DELETE` + `INSERT` dalam **satu transaksi**.
- **Privasi kader:** nama → `kader_kode` (HMAC-SHA256 berkunci `PSEUDONYM_KEY`), alamat kader dan
  alamat posyandu tidak dimuat, kolom tanpa header/tak dikenal tidak dimuat.

| Tabel staging | Grain | Baris (15 Nov 2026) |
|---|---|---|
| `stg.kunjungan_bulanan` | posyandu × bulan × kelompok (5 metrik) | 106.020 |
| `stg.antrop` | baris sheet Antrop (490 posyandu + 1 baris total) | 491 |
| `stg.kader` | baris sheet Kader | 2.545 |
| `stg.penduduk_kelompok_umur` | baris Tabel 6, 7, 8–12 buku E1 | 992 |
| `stg.stunting_kelurahan` | kelurahan × tahun + baris total portal | 106 |

## Menjalankan manual

```bash
docker compose exec airflow-scheduler python -m etl.cli semua                # schema → ingestion → staging → transform → gate → load
docker compose exec airflow-scheduler python -m etl.cli staging-kader --batch-id <batch_id>
```
Nama task: `ingest-excel`, `ingest-pdf`, `ingest-web`, `staging-laporan`, `staging-antrop`,
`staging-kader`, `staging-pdf`, `staging-web`, `transform`, `quality-gate`, `load-dw`; kelompok: `schema`,
`ingest`, `staging`, `semua`. Staging membutuhkan ingestion pada `batch_id` yang sama; transform membaca
isi staging yang ada (harus dari satu batch).
