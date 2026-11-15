-- =====================================================================
-- 04_dw_seed.sql — isi dimensi konstanta (daftar tetap, bukan dari sumber).
-- ON CONFLICT DO NOTHING: aman dijalankan ulang, tidak menimpa perubahan.
-- dim_kelurahan dan dim_posyandu TIDAK diisi di sini; keduanya dibangun dari data (transform).
-- =====================================================================

-- dim_waktu: 48 bulan (Jan 2023 – Des 2026)
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

-- dim_kelompok_sasaran: urutan = urutan kolom sumber; ambang A2 dari dokumen UTS (T09)
INSERT INTO dw.dim_kelompok_sasaran (kelompok_key, kode_kelompok, nama_kelompok, ambang_a2_sasaran) VALUES
    (1, 'ibu_hamil',            'Ibu Hamil',               14),
    (2, 'bayi_balita',          'Bayi Balita',             163),
    (3, 'usia_sekolah_remaja',  'Usia Sekolah dan Remaja', 551),
    (4, 'usia_produktif',       'Usia Produktif',          1348),
    (5, 'lansia',               'Lansia',                  249)
ON CONFLICT (kelompok_key) DO NOTHING;

-- dim_status_data (T06, T15)
INSERT INTO dw.dim_status_data (status_key, nama_status, keterangan) VALUES
    (1, 'Terlapor',         'Sasaran terisi'),
    (2, 'Tidak berlaku',    'Seluruh isian kelompok kosong pada 2023–2025 (kategori layanan berbeda tiap tahun)'),
    (3, 'Tidak terisi',     'Seluruh isian kelompok kosong pada 2026 di bulan yang sudah ada datanya'),
    (4, 'Belum dilaporkan', 'Bulan tanpa data pada tahun berjalan; tidak dimuat sebagai 0')
ON CONFLICT (status_key) DO NOTHING;

-- dim_alat: 5 alat × (standar, tidak standar) + tensimeter (T14: data tidak tersedia).
-- Catatan: dokumen UTS menyebut 10 baris sekaligus meminta tensimeter dimuat -> 11 baris.
-- Lihat docs/KEPUTUSAN.md K15 (menunggu konfirmasi pemilik).
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
-- Batas umur kelompok sasaran yang dipakai (BELUM DIVERIFIKASI terhadap Panduan [5] UTS):
--   bayi balita 0–59 bulan; usia sekolah & remaja 6–18 th; usia produktif 19–59 th; lansia >= 60 th.
-- 'tepat' bila rentang 5 tahunan seluruhnya berada di dalam satu kelompok sasaran; selain itu 'pendekatan'.
-- Ibu hamil tidak dapat dipetakan dari kelompok umur.
INSERT INTO dw.dim_kelompok_umur (kelompok_umur_key, label_umur, umur_min, umur_max, kelompok_key, ketepatan_pemetaan) VALUES
    (1,  '0-4',   0,  4,    2, 'tepat'),
    (2,  '5-9',   5,  9,    3, 'pendekatan'),
    (3,  '10-14', 10, 14,   3, 'tepat'),
    (4,  '15-19', 15, 19,   3, 'pendekatan'),
    (5,  '20-24', 20, 24,   4, 'tepat'),
    (6,  '25-29', 25, 29,   4, 'tepat'),
    (7,  '30-34', 30, 34,   4, 'tepat'),
    (8,  '35-39', 35, 39,   4, 'tepat'),
    (9,  '40-44', 40, 44,   4, 'tepat'),
    (10, '45-49', 45, 49,   4, 'tepat'),
    (11, '50-54', 50, 54,   4, 'tepat'),
    (12, '55-59', 55, 59,   4, 'tepat'),
    (13, '60-64', 60, 64,   5, 'tepat'),
    (14, '65-69', 65, 69,   5, 'tepat'),
    (15, '70-74', 70, 74,   5, 'tepat'),
    (16, '>=75',  75, NULL, 5, 'tepat')
ON CONFLICT (kelompok_umur_key) DO NOTHING;
