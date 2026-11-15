# Laporan Tahap 5 — Transform (stg → curated), aturan T01–T19

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 23–29 Nov (dikerjakan lebih awal) · **Mode:** otonom
- **Status:** selesai dan dites; seluruh angka acuan bagian 4.6 dicek (lihat bagian 4)

## 1. Tujuan

Mengubah data staging (teks asli) menjadi data bersih, bertipe, dan berkunci natural di schema `curated`,
menerapkan aturan T01–T19 serta keputusan pemilik K15, K16, K20, K21. Curated adalah bahan quality gate
dan load DWH (tahap 6).

## 2. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `sql/ddl/06_curated.sql` (baru) | 8 tabel `curated` (kelurahan, posyandu_versi, pemetaan_posyandu, kunjungan_bulanan, alat_snapshot, kader_snapshot, penduduk_kelurahan, stunting_kelurahan) |
| `sql/ddl/03_dw.sql` | K21: `dim_waktu` boleh memuat anggota "bulan tidak diketahui" (+ CHECK); `fact_kader_snapshot.periode_snapshot`; COMMENT grain kader & `kader_kode` bukan identitas (K22); K16: `fact_penduduk_kelurahan.flag_kualitas`; `fact_alat_snapshot.flag_kualitas` |
| `sql/ddl/04_dw_seed.sql` | anggota waktu `202600`; pemetaan `dim_kelompok_umur` K20 (ON CONFLICT DO UPDATE); komentar K15 |
| `etl/normalisasi.py` (baru) | T04 rapikan nama, T05 `posyandu_id`, pencocokan nama Antrop/Kader |
| `etl/pemetaan_posyandu_manual.csv` (baru, **tidak di-commit**) | 9 pemetaan nama Antrop yang sudah ditinjau |
| `etl/transform.py` (baru) | Task `transform` (T01–T19) |
| `etl/cli.py`, `etl/README.md` | Perintah `transform`, dokumentasi |

## 3. Cara kerja (untuk demo)

1. **Kelurahan (T16):** 52 kelurahan resmi + kode Kemendagri dari tabel E2 2025; alias BENUA ANYAR → BANUA ANYAR.
2. **Posyandu (T04, T05):** nama dirapikan (entitas HTML seperti `SA&#039;ADAH`, spasi, huruf besar), awalan
   `POSYANDU`/`PYD` dan akhiran nama kelurahan dibuang, angka Romawi → Arab. `posyandu_id` =
   `KELURAHAN--KUNCI` (mis. `SUNGAI_BILU--NUSA_INDAH_2`). Versi SCD tipe 2 dibuat setiap kali puskesmas
   berubah antar-tahun; `berlaku_mulai/berlaku_sampai/is_current`.
3. **Kunjungan:** staging sudah berbentuk panjang (T01–T02). Persen teks → angka (T03; gagal → NULL + dq_log).
   Status (T06/T15): Terlapor bila Sasaran terisi; kosong 2023–2025 → Tidak berlaku; kosong 2026 di bulan
   yang sudah berdata (Jan–Sep) → Tidak terisi; Okt–Des 2026 → Belum dilaporkan. T07: Cakupan turunan 0.
   `flag_anomali` bitmask (T09/T10): A1 ≥ 10.000; A2 > ambang kelompok (dibaca dari `dim_kelompok_sasaran`);
   A3 > 5 × median Sasaran>0 posyandu-kelompok setahun (min. 3 nilai); Cakupan > Sasaran; negatif;
   Sasaran 0 tetapi Cakupan > 0; |persen hitung ulang − persen laporan| > 1 poin.
4. **Alat (T14):** kecamatan/kelurahan kosong diisi dari baris atas; nama posyandu dipetakan (kunci → tabel
   manual → fuzzy ≥ 0,85 → ditahan); baris total dipakai rekonsiliasi; tensimeter tidak dimuat; `-` → NULL.
   Bulan snapshot dibaca dari sel CUT OFF ("JUNI 2026" → 202606).
5. **Kader (T12, T13):** jenis kelamin → L/P; "N/A" dihapus (T13, dq_log memuat kader_kode & posyandu, bukan
   nama); usia → kelompok umur; pendidikan diseragamkan; penilaian di luar Ya/Tidak → "Tidak valid";
   pasangan kader-posyandu ganda ditandai. Snapshot `202600` + `periode_snapshot = 'Tahun 2026'` (K21).
6. **Penduduk E1 (T16, T17, K16):** 52 × 16 kelompok umur × L/P. Kelayan Tengah: kelompok umur dari urutan
   baris setelah lolos validasi (16 baris, L+P=Jumlah, Σ = total) → `flag_kualitas`.
7. **Stunting E2 (T18):** Σ baris kelurahan dibandingkan dengan total portal.
8. Semua tabel curated ditulis ulang dalam **satu transaksi** bersama entri dq_log-nya.

## 4. Hasil uji & perbandingan dengan angka acuan (CLAUDE.md 4.6)

| Pemeriksaan | Hasil | Acuan | Status |
|---|---|---|---|
| Baris kunjungan 2023/2024/2025/2026 | 23.700 / 23.700 / 29.220 / 29.400 = 106.020 | 106.020 | ✔ |
| Posyandu unik / versi SCD | 490 / 503 (13 pindah Pekauman → Mantuil 2024) | 490 / 503 | ✔ |
| Baris anomali 2023–2026 / 2025 | 2.073 / 826 | 2.073 / 826 | ✔ |
| A2 / A3 | 1.044 / 568 | 1.044 / 568 | ✔ |
| Cakupan > Sasaran 2023–2026 | 36 / 50 / 84 / 163 | sama | ✔ (setelah K29) |
| T07 | 12 / 18 / 92 / 48 | sama | ✔ |
| Selisih persen > 1 poin | 22 / 3 / 5 / 90 | sama | ✔ |
| T08 2023 | 624 | 625 | ✘ selisih 1 (P10; staging sendiri berisi 624) |
| Terlapor 2025 / Belum dilaporkan 2026 | 28.037 / 7.350 | 28.037 / 7.350 | ✔ |
| Tidak berlaku 2023 / 2024 | 12.083 / 10.861 | 11.728 / 10.785 | beda (P08, K28: 974 baris parsial) |
| Kelengkapan Des 2025 | 475 dari 487 | 475 dari 487 | ✔ |
| Cakupan bayi balita 2025 | 58,72% (166.622 ÷ 283.775) | 58,7% | ✔ |
| Cakupan ibu hamil 2025 | 56,70% (10.577 ÷ 18.655) | 56,6% | ✘ beda pembulatan? (P10, belum diverifikasi) |
| Sasaran bayi balita Des 2025 per kecamatan | Tengah 3.499, Utara 6.281, Barat 5.151, Selatan 5.530, Timur 2.838; kota 23.299 | sama | ✔ |
| Posyandu < 5 kader | 31 dari 482 | 32 dari 483 | beda (P07: baris "N/A" ditahan) |
| Alat: minimal satu rusak/tidak standar; lengkap 5 standar baik | 335 dari 490; 164 | 335; 164 | ✔ |
| Pemetaan Antrop | 481 kunci + 9 manual = 490/490 | UTS: 94 cocok persis | ✔ (K25) |
| Rekonsiliasi Antrop vs baris total | selisih 0 di 20 kolom | 0 | ✔ |
| K16 Kelayan Tengah | 3 syarat lolos; Q08 selisih 0 | — | ✔ |
| E2 total 2024 / 2025 | 1.432 / 1.531 = total portal | sama | ✔ |

Bukti: `docs/bukti/17_transform_ringkasan.txt`, `docs/bukti/20_validasi_dwh.txt`.

**Selama pengujian ditemukan dan diperbaiki:** (1) baris Total Kelayan Dalam yang bergeser membuat parsing
gagal → parse total dibuat toleran dan selisihnya dicatat (T17); (2) baris total kecamatan E1 sempat ikut
dianggap kelurahan → difilter; (3) bit Cakupan > Sasaran semula hanya untuk Sasaran > 0 sehingga 2024/2025
tidak cocok → disesuaikan dengan **definisi** UTS (bukan dipaksa ke angka; lihat K29).

## 5. Keputusan & penyimpangan

K15, K16, K20, K21 (diputuskan pemilik) dan K22–K29, K32–K34, K36 di `docs/KEPUTUSAN.md`; P04–P08, P10 di
`docs/PERUBAHAN_DARI_UTS.md`.

## 6. Pertanyaan demo yang mungkin muncul

1. **Bagaimana posyandu yang namanya ditulis berbeda tiap tahun tetap dianggap sama?** — `nama_normal` +
   `kunci_cocok`: buang awalan/akhiran kelurahan, Romawi→Arab. Teruji: 490 kunci gabungan, tanpa duplikat per tahun.
2. **Kenapa ada 503 baris di dim_posyandu padahal 490 posyandu?** — SCD tipe 2: 13 posyandu pindah dari
   Puskesmas Pekauman ke Mantuil pada 2024, sehingga punya 2 versi; fakta 2023 menunjuk versi 1.
3. **Data anomali dihapus?** — Tidak. Ditandai di `flag_anomali` (bitmask) agar bisa difilter; KPI cakupan
   mengecualikan A1.
4. **Kenapa Kelayan Tengah boleh dipakai padahal label umurnya rusak?** — Karena lolos 3 validasi dan Q08
   (Σ kelurahan = total kecamatan di buku); barisnya ditandai `flag_kualitas`.
5. **Bagaimana nama kader dilindungi?** — Transform tidak pernah melihat nama; staging hanya menyimpan
   `kader_kode` (HMAC berkunci). Kode bukan identitas orang (K19, K22).

## 7. Risiko / kendala

- Pencocokan fuzzy bisa salah untuk nama baru yang mirip; karena itu diberi PERINGATAN dan tabel manual.
- `etl/pemetaan_posyandu_manual.csv` tidak di-commit (K36); di mesin lain tanpa berkas itu, 8 nama tetap cocok
  lewat fuzzy dan 1 posyandu (Lestari IV) ditahan → fakta alat 4.890 baris, bukan 4.900 (sudah diuji).
- T08 2023 (selisih 1) dan ibu hamil 56,70% vs 56,6% belum dapat dijelaskan sepenuhnya.

## 8. Untuk pemilik (git)

Claude **tidak** menjalankan git. Berkas tahap ini:

```
git add sql/ddl/03_dw.sql sql/ddl/04_dw_seed.sql sql/ddl/06_curated.sql \
        etl/normalisasi.py etl/transform.py etl/cli.py etl/README.md \
        docs/bukti/17_transform_ringkasan.txt docs/laporan_tahap/tahap_05_transform.md
git status
git diff --cached     # pastikan TIDAK ada etl/pemetaan_posyandu_manual.csv, .env, data/, CLAUDE.md
git commit -m "Transform T01-T19 ke schema curated (posyandu SCD2, anomali, kader, E1, E2)"
```

Pemeriksaan tanpa git: `etl/pemetaan_posyandu_manual.csv` cocok pola `*.csv` di `.gitignore` (sengaja). Bukti 17
hanya berisi ringkasan agregat; dipindai: tidak ada `KDR-`, nama kader, atau nilai `.env`.
