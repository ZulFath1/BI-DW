# Laporan Tahap 3 — Staging (Raw → stg)

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 16–22 Nov · **Mode:** otonom
- **Status:** selesai dan dites; **3 keputusan menunggu pemilik** (bagian 6)

## 1. Tujuan

Memuat isi berkas raw ke schema `stg` dengan **nilai asli** (teks apa adanya) dan **nomor baris
sumber**, setelah struktur sumber divalidasi, tanpa menyimpan nama/alamat kader.

## 2. Yang dibuat

| Berkas | Isi |
|---|---|
| `sql/ddl/02_stg.sql` | 5 tabel staging |
| `etl/staging_util.py` | Ambil objek raw + verifikasi checksum; konversi sel → teks; DELETE+INSERT atomik |
| `etl/staging_excel.py` | `staging_laporan` (sheet 2023–2026), `staging_antrop`, `staging_kader` |
| `etl/staging_pdf.py` | Parser Tabel 6, 7, 8–12 buku Disdukcapil (pdfplumber `extract_tables`) |
| `etl/staging_web.py` | Parser tabel HTML portal (BeautifulSoup + lxml) |
| `common/privasi.py` | Pseudonim kader: HMAC-SHA256 berkunci `PSEUDONYM_KEY` |
| `sql/validasi/01_staging.sql` | 11 query validasi (V01–V11) |
| `tests/uji_error_handling.py` | 5 skenario error handling (E1–E5) |

## 3. Cara kerja (untuk dijelaskan saat demo)

- **Laporan bulanan:** header 3 tingkat divalidasi sel demi sel (12 bulan × 5 kelompok × 5 metrik;
  ejaan "Nopember" sesuai sumber). Kolom ke-j dipetakan dengan rumus UTS
  `bulan = (j−5)//25, kelompok = ((j−5)%25)//5, metrik = (j−5)%5`. Satu baris staging =
  posyandu × bulan × kelompok berisi 5 metrik sebagai teks, plus `tipe_sel_asli`
  (n = angka, s = teks, _ = kosong) agar T03 tahu persen mana yang tersimpan sebagai teks.
- **Antrop:** header divalidasi; baris berangka tanpa label disimpan dengan `is_baris_total = true`
  (T14); sel "CUT OFF" (`: JUNI 2026`) disimpan untuk menentukan bulan snapshot.
- **Kader:** kolom dicocokkan **berdasarkan nama header**, bukan posisi. Nama → `kader_kode`;
  Alamat Kader, Alamat Posyandu, kolom tanpa header, dan kolom baru tak dikenal **tidak dimuat**.
  Nama hanya ada di memori saat dihitung pseudonimnya; tidak pernah ditulis ke DB/log/pesan error.
- **PDF E1:** setiap baris tabel dibaca sebagai `[kode] [nama] <kelompok umur> <angka…>` atau
  `<label Total> <angka…>`; konteks tabel dari judul bernomor; blok wilayah ditutup oleh baris Total.
- **E2:** tabel dikenali dari header-nya; judul halaman harus menyebut tahun yang sama.

## 4. Hasil & kecocokan dengan angka acuan (CLAUDE.md 4.1, 4.4, 4.6)

| Pemeriksaan | Hasil pipeline | Acuan | Status |
|---|---|---|---|
| Posyandu per sheet 2023/2024/2025/2026 | 395 / 395 / 487 / 490 | 395 / 395 / 487 / 490 | ✔ |
| Baris staging laporan (pratinjau Q01) | 23.700 / 23.700 / 29.220 / 29.400 = 106.020 | 106.020 | ✔ |
| Antrop | 490 posyandu + 1 baris total (baris 498) | 490 + baris 498 | ✔ |
| Rekonsiliasi Antrop 490 baris vs baris total (20 kolom alat) | selisih 0 di semua kolom | — | ✔ |
| Kader | 2.545 baris; "N/A" = 2; kolom ke-16 terisi 1 sel (dicatat K09) | 2.545; 2; 1 | ✔ |
| Pasangan kader-posyandu ganda | 6 kode, 12 baris | 6 pasangan (12 baris), UTS T12 | ✔ |
| E1 kelurahan | 52 × 16 kelompok umur = 832 baris data | 52 | ✔ |
| Pratinjau Q08: Σ 0–4 kelurahan vs Tabel 7 per kecamatan | selisih 0 di 5 kecamatan; kota 39.805 | 39.805 | ✔ |
| E2 total stunting 2024 / 2025 | 1.432 / 1.531 (Σ baris = total portal) | 1.432 / 1.531 | ✔ |
| Uji error handling | 5/5 lulus | — | ✔ |

Bukti: `docs/bukti/10_run_pipeline_minggu1.txt`, `11_validasi_staging.txt`, `13_uji_error_handling.txt`.

## 5. Temuan kualitas data (baru, tidak ada di dokumen UTS)

1. **Buku E1 hal. 21:** baris total Pekauman di dasar halaman tidak berlabel "Total".
   Ditangani: baris angka tanpa label tepat setelah baris `>=75` dianggap total (dicatat `STG06`).
2. **Buku E1 hal. 22, Kelayan Tengah:** kolom kelompok umur berisi angka (334, 417, 349, …),
   bukan label 0-4…>=75. Dikonfirmasi dengan merender halaman. Angka L/P/jumlah konsisten.
   Staging menyimpan nilai asli + `urutan_dalam_blok`; 16 PERINGATAN `STG07`.
3. **Buku E1 hal. 22, baris Total Kelayan Dalam bergeser:** `5.416 | 5.396 | 50,81 | 5.225 | 49,19`,
   kolom jumlah kosong. 16 baris datanya konsisten (Σ L = 5.396, Σ P = 5.225). Total ini tidak
   dipakai fakta; tercatat di V07 (51 dari 52 total kelurahan cocok).
4. **Antrop:** satu baris berisi "-" di 4 kolom timbangan bayi (bahan T03/T14).
5. **Kader:** 2.545 baris hanya menghasilkan 2.049 `kader_kode` unik (202 kode muncul > 1 kali,
   maks. 15). Pseudonim berbasis nama tidak membedakan orang bernama sama → `kader_kode` bukan
   identitas orang (K19).
6. **Persen sebagai teks** (V02): misalnya 2025: 15.564 sel `% Dilayani` bertipe teks vs 12.914 angka.

## 6. Keputusan yang menunggu pemilik (lihat docs/KEPUTUSAN.md)

- **K16:** kelompok umur Kelayan Tengah diambil dari urutan baris (ditandai `is_umur_dari_urutan`)?
  Alternatif: kelurahan ini dikeluarkan dari fakta penduduk (rasio stunting Kelayan Tengah hilang).
- **K21:** `waktu_key` snapshot Kader = 202612 (tidak ada bulan di sumber)?
- **K20:** batas umur kelompok sasaran untuk `dim_kelompok_umur` (belum diverifikasi ke Panduan).

## 7. Kelemahan & risiko

- Parser PDF bergantung pada tata letak buku Sem. II 2025; buku semester lain bisa berbeda dan
  kemungkinan butuh penyesuaian (pipeline akan gagal dengan jelas, bukan salah baca diam-diam).
- Staging dimuat ulang penuh setiap batch; riwayat versi data hanya ada di raw zone.
- `stg.kader` menyimpan `usia_asli` (dibutuhkan T12); usia hanya diubah menjadi kelompok umur di
  `dw`. Schema `stg` tidak dapat dibaca `dashboard_ro`.

## 8. Untuk pemilik (git)

```
git add common/privasi.py etl/staging_util.py etl/staging_excel.py etl/staging_pdf.py etl/staging_web.py etl/README.md \
        sql/ddl/02_stg.sql sql/validasi/01_staging.sql tests/__init__.py tests/uji_error_handling.py data/README.md \
        docs/bukti/10_run_pipeline_minggu1.txt docs/bukti/11_validasi_staging.txt docs/bukti/13_uji_error_handling.txt \
        docs/laporan_tahap/tahap_03_staging.md
git status
git diff --cached          # periksa: tidak ada nama/alamat kader, .env, data/, CLAUDE.md
git commit -m "Staging laporan, Antrop, Kader (pseudonim), PDF E1, dan web E2 beserta validasi dan uji error"
```

Bukti `.txt` hanya memuat agregat dan nama wilayah (data terbuka); sudah dipindai: tidak ada
nilai `.env` di dalamnya. `data/README.md` ikut karena dikecualikan dari `.gitignore`.
