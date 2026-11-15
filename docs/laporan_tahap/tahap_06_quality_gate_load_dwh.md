# Laporan Tahap 6 — Quality Gate Q01–Q09 dan Load DWH

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 23–29 Nov · **Mode:** otonom
- **Status:** selesai dan dites; DWH terisi dari run Airflow resmi

## 1. Tujuan

Memastikan data yang gagal tidak masuk DWH: pemeriksaan Q01–Q09, cabang karantina, dan load ke galaxy
schema `dw` yang atomik (ROLLBACK bila pemeriksaan kritis gagal).

## 2. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `sql/ddl/01_dq.sql` | `dq.quality_result`; `ingest_manifest.sha256_isi_tabel` + status `tidak_berubah` |
| `quality/quality_gate.py`, `quality/__init__.py` | Q01–Q03, Q07–Q09 (sebelum load), `quarantine` |
| `etl/load_dw.py` | Load dimensi (upsert) + fakta (hapus & sisip ulang) + Q04–Q06, satu transaksi |
| `sql/validasi/03_dwh.sql` | Validasi DWH D01–D13 terhadap angka acuan |
| `tests/uji_quality_gate.py` | Uji E6 rollback, E7 quarantine, E8 idempotensi |
| `quality/README.md`, `sql/README.md` | Dokumentasi |

## 3. Cara kerja (untuk demo)

- **Sebelum load** (`quality_gate`): Q01 jumlah baris, Q02 kunci unik/satu versi aktif/versi tidak
  tumpang tindih (**kritis**), Q03 persen non-numerik, Q07 pemetaan kelurahan, Q08 penduduk 0–4,
  Q09 rekonsiliasi kader. Kritis gagal → cabang `quarantine`, DWH tidak disentuh.
- **Dalam load** (`load_dw`): dalam satu transaksi → upsert `dim_kelurahan` & `dim_posyandu` (surrogate key
  stabil) → hapus fakta → sisip fakta dengan lookup versi SCD yang berlaku di bulan fakta → Q04 (Σ Sasaran &
  Cakupan staging = fakta), Q05 (FK yatim, versi SCD salah, baris hilang saat lookup), Q06 (tidak ada kolom
  nama/alamat kader di `dw`/`mart`) — semua **kritis**. Gagal → `ROLLBACK`.
- Hasil Q01–Q09 ditulis ke `dq.quality_result` dengan koneksi autocommit, sehingga tetap ada walaupun
  load di-rollback.

## 4. Hasil uji

| Pemeriksaan | Hasil |
|---|---|
| Q01–Q09 pada run `manual__2026-11-15T15:32:55…` | 9/9 lulus (`docs/bukti/18_quality_gate.txt`) |
| Q04 per tahun | Σ Sasaran & Σ Cakupan fakta = staging di 4 tahun |
| Isi DWH | dim_posyandu 503 (490 posyandu), dim_kelurahan 52, dim_waktu 49, dim_alat 11; fakta kunjungan 106.020, alat 4.900, kader 2.542, penduduk 1.664, stunting 104 |
| Rasio stunting kota 2025 | **3,85%** (acuan 3,85%); kecamatan 2025 Selatan 4,30, Barat 4,26, Tengah 3,87, Timur 3,76, Utara 3,03 (sama dengan UTS) |
| E6 kolom `nama_kader` di mart | load GAGAL, Q06 gagal, **DWH sama persis** sebelum/sesudah ✔ |
| E7 dua versi aktif di curated | gate lulus=False (Q02) → quarantine tercatat & task gagal; DWH tidak berubah ✔ |
| E8 load 2× | jumlah baris, Σ sasaran, Σ surrogate key identik ✔ |
| Uji ulang E1–E5 tahap 3 | 5/5 lulus ✔ |

Bukti: `18_quality_gate.txt`, `19_uji_quality_gate.txt`, `20_validasi_dwh.txt`.

## 5. Keputusan & penyimpangan

K27 (fakta dimuat ulang dalam transaksi), K30 (gate dua fase) — P09.

## 6. Pertanyaan demo yang mungkin muncul

1. **Apa yang terjadi bila total staging dan fakta tidak cocok?** — Q04 gagal di dalam transaksi load →
   ROLLBACK, `load_dw` GAGAL, DWH tetap versi sebelumnya; hasil tercatat di `dq.quality_result`. Dibuktikan
   E6 (dengan Q06).
2. **Kenapa Q04–Q06 tidak dijalankan sebelum load?** — Ketiganya memeriksa isi/struktur DWH itu sendiri
   (fakta hasil lookup, FK, kolom). Dijalankan sebelum COMMIT sehingga tetap "sebelum data terlihat".
3. **Kenapa fakta dihapus lalu disisip ulang, bukan upsert?** — Curated selalu lengkap; hasil identik dengan
   upsert (E8), lebih sederhana, dan tetap atomik. Dimensi di-upsert supaya surrogate key tidak berubah.
4. **Di mana data yang ditolak?** — Baris: `dq.dq_log` (+ `curated.pemetaan_posyandu` untuk nama tak
   terpetakan). Batch: `dq.quarantine`.

## 7. Risiko / kendala

- Q04 membandingkan dengan staging pada batch yang sama; bila staging diubah di luar pipeline, Q04 akan gagal
  (disengaja).
- Load penuh cocok untuk ±115 ribu baris (±4 detik). Untuk volume jauh lebih besar perlu load inkremental per
  partisi tahun (dicatat sebagai pengembangan lanjutan).
- `dq_log` tahap T13 di database memuat `kader_kode` (sesuai UTS T13); database tidak di-commit dan
  `dashboard_ro` tidak bisa membaca schema `dq`.

## 8. Untuk pemilik (git)

```
git add sql/ddl/01_dq.sql quality/__init__.py quality/quality_gate.py quality/README.md \
        etl/load_dw.py sql/validasi/03_dwh.sql sql/README.md tests/uji_quality_gate.py \
        docs/bukti/18_quality_gate.txt docs/bukti/19_uji_quality_gate.txt docs/bukti/20_validasi_dwh.txt \
        docs/laporan_tahap/tahap_06_quality_gate_load_dwh.md
git status
git diff --cached     # periksa: tidak ada .env, data/, CLAUDE.md, berkas .csv
git commit -m "Quality gate Q01-Q09, load DWH atomik dengan rollback, dan uji karantina"
```
