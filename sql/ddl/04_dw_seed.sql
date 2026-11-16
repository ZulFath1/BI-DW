-- =====================================================================
-- 04_dw_seed.sql — isi dimensi konstanta (daftar tetap, bukan dari sumber).
-- ON CONFLICT DO NOTHING: aman dijalankan ulang, tidak menimpa perubahan.
-- dim_kelurahan dan dim_posyandu TIDAK diisi di sini; keduanya dibangun dari data (transform).
-- =====================================================================

-- dim_waktu: 48 bulan (Jan 2023 – Des 2026) + anggota "bulan tidak diketahui" 2026 (K21)
INSERT INTO dw.dim_waktu (waktu_key, tanggal_awal, tahun, semester, kuartal, bulan, nama_bulan)
SELECT to_char(d, 'YYYYMM')::int,
       d::date,
       extract(year FROM d)::smallint,
       CASE WHEN extract(month FROM d) <= 6 THEN 1 ELSE 2 END,
       extract(quarter FROM d)::smallint,
       extract(month FROM d)::smallint,
       (ARRAY['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus',
              'September', 'Oktober', 'November', 'Desember'])[extract(month FROM d)::int]
FROM generate_series(DATE '2023-01-01', DATE '2026-12-01', INTERVAL '1 month') AS g(d)
ON CONFLICT (waktu_key) DO NOTHING;

-- K21: anggota "bulan tidak diketahui" untuk snapshot Kader (sumber hanya menyebut "Tahun 2026").
INSERT INTO dw.dim_waktu (waktu_key, tanggal_awal, tahun, semester, kuartal, bulan, nama_bulan, is_bulan_diketahui)
VALUES (202600, NULL, 2026, NULL, NULL, NULL, 'Tidak diketahui (2026)', false)
ON CONFLICT (waktu_key) DO NOTHING;

-- dim_kelompok_sasaran: urutan = urutan kolom sumber; ambang A2 dari dokumen UTS (T09)
INSERT INTO dw.dim_kelompok_sasaran (kelompok_key, kode_kelompok, nama_kelompok, ambang_a2_sasaran) VALUES
    (1, 'ibu_hamil',            'Ibu Hamil',               14),
    (2, 'bayi_balita',          'Bayi Balita',             163),
    (3, 'usia_sekolah_remaja',  'Usia Sekolah dan Remaja', 551),
    (4, 'usia_produktif',       'Usia Produktif',          1348),
    (5, 'lansia',               'Lansia',                  249)
ON CONFLICT (kelompok_key) DO NOTHING;

-- dim_status_data (T06, T15). K37 (minggu 30 Nov–6 Des): status 5 untuk baris yang Sasarannya
-- kosong tetapi Cakupan/Dirujuk > 0; baris yang hanya berisi nol bawaan (persen 0, Cakupan 0,
-- Dirujuk 0) tetap diperlakukan kosong. DO UPDATE agar keterangan di database lama ikut diperbarui.
INSERT INTO dw.dim_status_data (status_key, nama_status, keterangan) VALUES
    (1, 'Terlapor',         'Sasaran terisi'),
    (2, 'Tidak berlaku',    'Sasaran kosong dan tidak ada Cakupan/Dirujuk > 0 pada 2023–2025 (seluruhnya kosong atau hanya nol bawaan; kategori layanan berbeda tiap tahun)'),
    (3, 'Tidak terisi',     'Sasaran kosong dan tidak ada Cakupan/Dirujuk > 0 pada 2026 di bulan yang sudah ada datanya'),
    (4, 'Belum dilaporkan', 'Bulan tanpa data pada tahun berjalan; tidak dimuat sebagai 0'),
    (5, 'Cakupan tanpa sasaran', 'Sasaran kosong tetapi Cakupan atau Dirujuk > 0: ada pelayanan tercatat, persen tidak dapat dihitung; tidak masuk KPI cakupan')
ON CONFLICT (status_key) DO UPDATE SET nama_status = EXCLUDED.nama_status, keterangan = EXCLUDED.keterangan;

-- dim_alat: 5 alat × (standar, tidak standar) + tensimeter berstatus "data tidak tersedia" (T14).
-- K15 (diputuskan): 11 baris. Dokumen UTS menulis 10 baris tetapi T14 meminta tensimeter ada di
-- dim_alat; angka 10 dikoreksi (PERUBAHAN_DARI_UTS P04). fact_alat_snapshot tetap maks. 490 × 10,
-- karena tensimeter tidak dimuat ke fakta.
INSERT INTO dw.dim_alat (alat_key, kode_alat, nama_alat, kategori_standar, ketersediaan_data, kolom_sumber) VALUES
    (1,  'timbangan_bayi_std',   'Timbangan bayi digital',  'Standar',       'tersedia', 'timbangan_bayi_std'),
    (2,  'timbangan_bayi_tstd',  'Timbangan bayi digital',  'Tidak standar', 'tersedia', 'timbangan_bayi_tstd'),
    (3,  'panjang_bayi_std',     'Pengukur panjang bayi',   'Standar',       'tersedia', 'panjang_bayi_std'),
    (4,  'panjang_bayi_tstd',    'Pengukur panjang bayi',   'Tidak standar', 'tersedia', 'panjang_bayi_tstd'),
    (5,  'timbangan_badan_std',  'Timbangan badan digital', 'Standar',       'tersedia', 'timbangan_badan_std'),
    (6,  'timbangan_badan_tstd', 'Timbangan badan digital', 'Tidak standar', 'tersedia', 'timbangan_badan_tstd'),
    (7,  'tinggi_badan_std',     'Pengukur tinggi badan',   'Standar',       'tersedia', 'tinggi_badan_std'),
    (8,  'tinggi_badan_tstd',    'Pengukur tinggi badan',   'Tidak standar', 'tersedia', 'tinggi_badan_tstd'),
    (9,  'lila_std',             'Pengukur LILA/LP/LK',     'Standar',       'tersedia', 'lila_std'),
    (10, 'lila_tstd',            'Pengukur LILA/LP/LK',     'Tidak standar', 'tersedia', 'lila_tstd'),
    (11, 'tensimeter',           'Tensimeter digital',      NULL,            'data tidak tersedia', 'tensimeter')
ON CONFLICT (alat_key) DO NOTHING;

-- dim_kelompok_umur: 16 kelompok buku Disdukcapil + pemetaan ke kelompok sasaran posyandu.
-- K20 (diputuskan): batas Panduan Kemenkes 2023 [5] seperti dikutip dokumen UTS (Definition and Terms):
--   bayi & anak prasekolah 0–6 th; usia sekolah & remaja >6–18 th; usia dewasa >18–59 th; lansia >= 60 th.
--   bayi balita       -> 0-4            'pendekatan' (balita 0–59 bulan; Panduan memakai 0–6 th)
--   sekolah & remaja  -> 5-9 .. 15-19   'pendekatan' (batas >6–18 tidak jatuh di batas 5 tahunan)
--   usia produktif    -> 20-24 .. 55-59 'pendekatan' (produktif = dewasa >18–59 adalah ASUMSI perancangan)
--   lansia            -> 60-64 .. >=75  'tepat'
--   ibu hamil tidak dapat dipetakan dari kelompok umur.
-- Catatan: definisi BPS "usia produktif" = 15–64 tahun, berbeda dengan pemetaan ini.
-- ON CONFLICT DO UPDATE agar database lama ikut memakai pemetaan terbaru.
INSERT INTO dw.dim_kelompok_umur (kelompok_umur_key, label_umur, umur_min, umur_max, kelompok_key, ketepatan_pemetaan) VALUES
    (1,  '0-4',   0,  4,    2, 'pendekatan'),
    (2,  '5-9',   5,  9,    3, 'pendekatan'),
    (3,  '10-14', 10, 14,   3, 'pendekatan'),
    (4,  '15-19', 15, 19,   3, 'pendekatan'),
    (5,  '20-24', 20, 24,   4, 'pendekatan'),
    (6,  '25-29', 25, 29,   4, 'pendekatan'),
    (7,  '30-34', 30, 34,   4, 'pendekatan'),
    (8,  '35-39', 35, 39,   4, 'pendekatan'),
    (9,  '40-44', 40, 44,   4, 'pendekatan'),
    (10, '45-49', 45, 49,   4, 'pendekatan'),
    (11, '50-54', 50, 54,   4, 'pendekatan'),
    (12, '55-59', 55, 59,   4, 'pendekatan'),
    (13, '60-64', 60, 64,   5, 'tepat'),
    (14, '65-69', 65, 69,   5, 'tepat'),
    (15, '70-74', 70, 74,   5, 'tepat'),
    (16, '>=75',  75, NULL, 5, 'tepat')
ON CONFLICT (kelompok_umur_key) DO UPDATE SET
    label_umur = EXCLUDED.label_umur, umur_min = EXCLUDED.umur_min, umur_max = EXCLUDED.umur_max,
    kelompok_key = EXCLUDED.kelompok_key, ketepatan_pemetaan = EXCLUDED.ketepatan_pemetaan;
