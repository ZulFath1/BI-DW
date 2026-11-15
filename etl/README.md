# etl/

Tahap **Staging → Transform → Curated → Load** (aturan T01–T19).

| Berkas | Fungsi | Status |
|---|---|---|
| `schema.py` | Task `init_schema`: menjalankan semua `sql/ddl/*.sql` dalam satu transaksi (idempotent) | ada |
| `staging_util.py` | Ambil objek dari data lake (verifikasi checksum), konversi sel → teks asli, muat ulang atomik | ada |
| `staging_excel.py` | `staging_laporan` (sheet 2023–2026), `staging_antrop`, `staging_kader` | ada |
| `staging_pdf.py` | `staging_penduduk`: Tabel 6, 7, 8–12 buku Disdukcapil (E1) | ada |
| `staging_web.py` | `staging_stunting`: tabel halaman Satu Data (E2) 2024 & 2025 | ada |
| `cli.py` | Menjalankan task secara manual (lihat di bawah) | ada |
| transform, load DWH | T01–T19 → `curated` → `dw` | belum (minggu 23–29 Nov) |

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
docker compose exec airflow-scheduler python -m etl.cli semua                # schema + ingestion + staging
docker compose exec airflow-scheduler python -m etl.cli staging-kader --batch-id <batch_id>
```
Nama task: `ingest-excel`, `ingest-pdf`, `ingest-web`, `staging-laporan`, `staging-antrop`,
`staging-kader`, `staging-pdf`, `staging-web`; kelompok: `schema`, `ingest`, `staging`, `semua`.
Staging membutuhkan ingestion pada `batch_id` yang sama.
