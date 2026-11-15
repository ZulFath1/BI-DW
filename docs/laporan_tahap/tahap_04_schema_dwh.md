# Laporan Tahap 4 — Schema Data Warehouse (dw)

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 16–22 Nov · **Mode:** otonom
- **Status:** selesai dan dites (tabel fakta dan dimensi berbasis data **belum diisi**; itu tugas
  transform/load minggu 23–29 Nov)

## 1. Tujuan

Membentuk galaxy schema sesuai BI-DW-001 bagian 5.2: tabel fakta dan dimensi dengan PK, FK,
surrogate key, grain yang jelas; mengisi dimensi konstanta; membuat view rasio stunting.

## 2. Yang dibuat

| Berkas | Isi |
|---|---|
| `sql/ddl/03_dw.sql` | 7 dimensi + 5 fakta, PK/FK/CHECK, komentar grain |
| `sql/ddl/04_dw_seed.sql` | `dim_waktu` 48, `dim_kelompok_sasaran` 5, `dim_status_data` 4, `dim_alat` 11, `dim_kelompok_umur` 16 |
| `sql/ddl/05_dw_view.sql` | `dw.v_rasio_stunting_balita` (kelurahan, kecamatan, kota; GROUPING SETS) |
| `sql/validasi/02_uji_view_rasio.sql` | Uji view dengan data staging asli di dalam transaksi yang di-ROLLBACK |
| `sql/README.md`, `README.md` | Dokumentasi schema & cara menjalankan |

## 3. Penjelasan desain (untuk demo)

- **Surrogate key** `*_key` (integer) di setiap dimensi; kunci natural disimpan terpisah
  (`posyandu_id`, `kode_wilayah`). `dim_posyandu` = SCD tipe 2: satu `posyandu_key` per versi,
  `berlaku_mulai/berlaku_sampai/is_current`; indeks unik memastikan hanya satu versi aktif.
- **Grain** fakta kunjungan = posyandu × bulan × kelompok (PK komposit tiga kunci, mendukung upsert).
- `sasaran`/`cakupan` boleh NULL (kosong ≠ 0, T06/T15); `flag_anomali` bitmask 1–64 (T09/T10).
- `fact_kader_snapshot` tidak punya kolom nama/alamat; `jenis_kelamin` NOT NULL + CHECK L/P (T13).
- **View rasio:** Σ stunting ÷ Σ penduduk 0–4 × 100 (bukan rata-rata rasio); stunting 2024 dipasangkan
  dengan penduduk Sem. II 2025 → `is_pendekatan = true`; label "bukan prevalensi".
- Semua DDL idempotent dan dijalankan dalam **satu transaksi** oleh `init_schema`.

## 4. Hasil & bukti

| Pemeriksaan | Hasil | Acuan | Status |
|---|---|---|---|
| Dimensi konstanta | 48 / 5 / 4 / 11 / 16 | 48 / 5 / 4 / 10 / 16 | ✔ kecuali `dim_alat` (K15) |
| DDL dijalankan 2× berturut-turut | sukses, tanpa duplikasi | idempotent | ✔ |
| Relasi | 16 FK di 7 tabel (lihat bukti) | — | ✔ |
| Hak `dashboard_ro` di `dw` | hanya SELECT, termasuk view | read-only | ✔ |
| Uji view (ROLLBACK): rasio kota 2025 | **3,85%** (1.531 ÷ 39.805) | 3,85% | ✔ |
| Uji view: baris kelurahan 2024 / 2025 | 52 / 52; 2024 `is_pendekatan = t` | 52 | ✔ |
| Pemetaan nama kelurahan E1 ↔ E2 | 52/52 cocok (tanpa alias) | 52 | ✔ |
| Setelah ROLLBACK | `dw` kosong kembali | — | ✔ |

Bukti: `docs/bukti/14_schema_dw.txt`, `docs/bukti/12_uji_view_rasio.txt`.

Catatan teknis: sequence `dim_kelurahan_kelurahan_key_seq` maju di dalam transaksi uji (sequence
PostgreSQL tidak ikut di-rollback); sudah dikembalikan ke 1 dengan `setval` (tabel kosong).

## 5. Kelemahan, risiko, keputusan menunggu

- **K15 `dim_alat` 11 baris** — dokumen UTS tidak konsisten (10 baris vs tensimeter dimuat).
  Bila pemilik memilih 10, cukup hapus baris `alat_key = 11` dari `04_dw_seed.sql` sebelum load
  (tabel masih kosong dari fakta, jadi aman).
- **K20 pemetaan kelompok umur** belum diverifikasi ke Panduan posyandu [5].
- Kolom `persen_dilayani` bertipe `numeric(9,2)`; bila transform menemukan nilai di luar rentang
  (mis. dari Sasaran ekstrem A1), load akan gagal dan harus ditangani di quality gate (Q03/T09).
- Perubahan struktur tabel di masa depan perlu `ALTER TABLE ... IF NOT EXISTS` (belum ada alat
  migrasi seperti Alembic) — cukup untuk skala proyek ini.
- Schema `curated` dan `mart` belum berisi tabel (minggu 23 Nov–6 Des).

## 6. Status minggu 16–22 Nov (jadwal 5.6)

| Target | Status |
|---|---|
| Docker Compose, data lake (MinIO → Silo) | selesai (tahap 1) |
| Ingestion | selesai (tahap 2) |
| Staging | selesai (tahap 3) |
| Schema DWH | selesai (tahap 4) |

Target minggu ini **selesai**. Berikutnya (23–29 Nov): transform T01–T19 → `curated`, quality gate
Q01–Q09, DAG Airflow (jadwal tanggal 10, retry 3×, callback), DWH terisi. Sebelum mulai, mohon
putuskan K15, K16, K20, K21.

## 7. Untuk pemilik (git)

```
git add sql/ddl/03_dw.sql sql/ddl/04_dw_seed.sql sql/ddl/05_dw_view.sql sql/validasi/02_uji_view_rasio.sql \
        sql/README.md README.md docs/bukti/12_uji_view_rasio.txt docs/bukti/14_schema_dw.txt \
        docs/laporan_tahap/tahap_04_schema_dwh.md
git status
git diff --cached          # periksa: tidak ada .env, data/, CLAUDE.md
git commit -m "Schema data warehouse galaxy (fakta, dimensi, seed, view rasio stunting)"
git push                   # tanpa --force
```

Setelah ketiga commit, `git status` seharusnya hanya menyisakan berkas yang memang diabaikan
(`.env`, `data/`, `CLAUDE.md`). Bila ada berkas lain yang belum ter-commit, periksa dulu isinya.
