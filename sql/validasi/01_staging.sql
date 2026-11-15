-- =====================================================================
-- 01_staging.sql — query validasi hasil staging (dijalankan manual / sebagai bukti).
-- Hanya menampilkan agregat; tidak menampilkan data pribadi kader.
-- Jalankan:
--   docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/01_staging.sql
-- =====================================================================
\pset footer off

\echo '== V01. Laporan bulanan: baris staging per tahun (harap posyandu × 60: 23.700; 23.700; 29.220; 29.400)'
SELECT tahun_sheet,
       count(DISTINCT baris_sumber)            AS baris_posyandu,
       count(*)                                AS baris_staging,
       count(DISTINCT baris_sumber) * 60       AS harapan,
       count(*) = count(DISTINCT baris_sumber) * 60 AS cocok
FROM stg.kunjungan_bulanan GROUP BY tahun_sheet ORDER BY tahun_sheet;

\echo '== V02. Laporan bulanan: tipe sel % Dilayani (s = teks, n = angka, _ = kosong) — bahan T03'
SELECT tahun_sheet, substr(tipe_sel_asli, 3, 1) AS tipe_persen_dilayani, count(*)
FROM stg.kunjungan_bulanan GROUP BY 1, 2 ORDER BY 1, 2;

\echo '== V03. Antrop: baris posyandu vs baris total (harap 490 dan 1)'
SELECT count(*) FILTER (WHERE NOT is_baris_total) AS baris_posyandu,
       count(*) FILTER (WHERE is_baris_total)     AS baris_total,
       min(cut_off_asli)                          AS cut_off
FROM stg.antrop;

\echo '== V04. Antrop: rekonsiliasi jumlah 490 baris vs baris total (bahan T14), per kolom alat'
-- Nilai non-angka (mis. '-') tidak dijumlahkan, tetapi dihitung di kolom non_angka.
WITH k AS (
    SELECT is_baris_total, kolom,
           CASE WHEN trim(nilai) ~ '^-?\d+(\.\d+)?$' THEN trim(nilai)::numeric END AS nilai,
           (nilai IS NOT NULL AND trim(nilai) !~ '^-?\d+(\.\d+)?$') AS non_angka
    FROM stg.antrop,
         LATERAL (VALUES
            ('timbangan_bayi_std_baik', timbangan_bayi_std_baik), ('timbangan_bayi_std_rusak', timbangan_bayi_std_rusak),
            ('timbangan_bayi_tstd_baik', timbangan_bayi_tstd_baik), ('timbangan_bayi_tstd_rusak', timbangan_bayi_tstd_rusak),
            ('panjang_bayi_std_baik', panjang_bayi_std_baik), ('panjang_bayi_std_rusak', panjang_bayi_std_rusak),
            ('panjang_bayi_tstd_baik', panjang_bayi_tstd_baik), ('panjang_bayi_tstd_rusak', panjang_bayi_tstd_rusak),
            ('timbangan_badan_std_baik', timbangan_badan_std_baik), ('timbangan_badan_std_rusak', timbangan_badan_std_rusak),
            ('timbangan_badan_tstd_baik', timbangan_badan_tstd_baik), ('timbangan_badan_tstd_rusak', timbangan_badan_tstd_rusak),
            ('tinggi_badan_std_baik', tinggi_badan_std_baik), ('tinggi_badan_std_rusak', tinggi_badan_std_rusak),
            ('tinggi_badan_tstd_baik', tinggi_badan_tstd_baik), ('tinggi_badan_tstd_rusak', tinggi_badan_tstd_rusak),
            ('lila_std_baik', lila_std_baik), ('lila_std_rusak', lila_std_rusak),
            ('lila_tstd_baik', lila_tstd_baik), ('lila_tstd_rusak', lila_tstd_rusak)
         ) AS v(kolom, nilai)
)
SELECT kolom,
       sum(nilai) FILTER (WHERE NOT is_baris_total) AS jumlah_490_baris,
       sum(nilai) FILTER (WHERE is_baris_total)     AS baris_total,
       coalesce(sum(nilai) FILTER (WHERE NOT is_baris_total), 0)
         - coalesce(sum(nilai) FILTER (WHERE is_baris_total), 0) AS selisih,
       count(*) FILTER (WHERE non_angka) AS non_angka
FROM k GROUP BY kolom ORDER BY kolom;

\echo '== V05. Kader: jumlah baris, kode pseudonim, jenis kelamin (harap 2.545; N/A = 2)'
SELECT count(*) AS baris, count(kader_kode) AS kader_kode_terisi,
       count(DISTINCT kader_kode) AS kader_kode_unik,
       count(*) FILTER (WHERE jenis_kelamin_asli = 'N/A') AS jk_na,
       count(*) FILTER (WHERE jenis_kelamin_asli IS NULL) AS jk_kosong
FROM stg.kader;

\echo '== V06. Privasi: kolom alamat di mana pun, atau kolom nama di tabel kader (selain nama posyandu) — harap 0 baris'
SELECT table_schema, table_name, column_name FROM information_schema.columns
WHERE table_schema IN ('stg', 'curated', 'dw', 'mart')
  AND (column_name ILIKE '%alamat%'
       OR (table_name ILIKE '%kader%' AND column_name ILIKE '%nama%' AND column_name NOT ILIKE 'nama_posyandu%'))
ORDER BY 1, 2, 3;

\echo '== V07. E1: per kelurahan, jumlah 16 baris kelompok umur vs baris Total (L, P, jumlah)'
-- Hanya teks berformat ribuan buku ('1.273') yang dikonversi; selain itu (mis. '50,81' akibat
-- kolom bergeser) menjadi NULL sehingga terlihat sebagai tidak cocok.
WITH angka AS (
    SELECT nomor_tabel_buku, wilayah_asli, jenis_baris,
           CASE WHEN laki_jiwa_asli ~ '^\d{1,3}(\.\d{3})*$' THEN replace(laki_jiwa_asli, '.', '')::int END AS l,
           CASE WHEN perempuan_jiwa_asli ~ '^\d{1,3}(\.\d{3})*$' THEN replace(perempuan_jiwa_asli, '.', '')::int END AS p,
           CASE WHEN jumlah_asli ~ '^\d{1,3}(\.\d{3})*$' THEN replace(jumlah_asli, '.', '')::int END AS j
    FROM stg.penduduk_kelompok_umur WHERE tingkat = 'kelurahan' AND jenis_baris IN ('data', 'total_wilayah')
), r AS (
    SELECT nomor_tabel_buku, wilayah_asli,
           sum(l) FILTER (WHERE jenis_baris = 'data') AS l_data, max(l) FILTER (WHERE jenis_baris = 'total_wilayah') AS l_total,
           sum(p) FILTER (WHERE jenis_baris = 'data') AS p_data, max(p) FILTER (WHERE jenis_baris = 'total_wilayah') AS p_total,
           sum(j) FILTER (WHERE jenis_baris = 'data') AS j_data, max(j) FILTER (WHERE jenis_baris = 'total_wilayah') AS j_total
    FROM angka GROUP BY 1, 2
)
SELECT count(*) AS kelurahan,
       count(*) FILTER (WHERE l_data = l_total AND p_data = p_total AND j_data = j_total) AS total_cocok,
       string_agg(wilayah_asli || ' (L ' || l_data || '/' || coalesce(l_total::text, '?') || ', P ' || p_data || '/'
                  || coalesce(p_total::text, '?') || ', J ' || j_data || '/' || coalesce(j_total::text, '?') || ')', '; ')
         FILTER (WHERE l_data IS DISTINCT FROM l_total OR p_data IS DISTINCT FROM p_total
                       OR j_data IS DISTINCT FROM j_total) AS tidak_cocok
FROM r;

\echo '== V08. E1: baris data dengan L + P <> jumlah'
SELECT count(*) AS baris_data,
       count(*) FILTER (WHERE replace(laki_jiwa_asli, '.', '')::int + replace(perempuan_jiwa_asli, '.', '')::int
                              <> replace(jumlah_asli, '.', '')::int) AS l_plus_p_tidak_sama
FROM stg.penduduk_kelompok_umur WHERE jenis_baris = 'data';

\echo '== V09. E1 (pratinjau Q08): penduduk 0–4 = baris urutan ke-1 tiap blok; jumlah kelurahan vs Tabel 7 per kecamatan'
WITH kel AS (
    SELECT kecamatan_asli, sum(replace(jumlah_asli, '.', '')::int) AS balita_kelurahan, count(*) AS n_kelurahan
    FROM stg.penduduk_kelompok_umur
    WHERE tingkat = 'kelurahan' AND jenis_baris = 'data' AND urutan_dalam_blok = 1
    GROUP BY 1
), kec AS (
    SELECT wilayah_asli AS kecamatan_asli, replace(jumlah_asli, '.', '')::int AS balita_tabel7
    FROM stg.penduduk_kelompok_umur
    WHERE nomor_tabel_buku = 7 AND jenis_baris = 'data' AND kelompok_umur_asli = '0-4'
)
SELECT kel.kecamatan_asli, n_kelurahan, balita_kelurahan, balita_tabel7, balita_kelurahan - balita_tabel7 AS selisih
FROM kel JOIN kec USING (kecamatan_asli)
UNION ALL
SELECT 'KOTA (Tabel 6)', sum(n_kelurahan), sum(balita_kelurahan),
       (SELECT replace(jumlah_asli, '.', '')::int FROM stg.penduduk_kelompok_umur
         WHERE nomor_tabel_buku = 6 AND kelompok_umur_asli = '0-4'),
       sum(balita_kelurahan) - (SELECT replace(jumlah_asli, '.', '')::int FROM stg.penduduk_kelompok_umur
         WHERE nomor_tabel_buku = 6 AND kelompok_umur_asli = '0-4')
FROM kel
ORDER BY 1;

\echo '== V10. E2: jumlah kelurahan dan total stunting (baris data vs baris total portal; acuan 1.432 dan 1.531)'
SELECT tahun_data,
       count(*) FILTER (WHERE jenis_baris = 'data') AS kelurahan,
       sum(replace(stunting_asli, '.', '')::int) FILTER (WHERE jenis_baris = 'data') AS stunting_jumlah_baris,
       max(stunting_asli) FILTER (WHERE jenis_baris = 'total') AS stunting_total_portal,
       min(catatan_asli) AS catatan
FROM stg.stunting_kelurahan GROUP BY tahun_data ORDER BY tahun_data;

\echo '== V11. Status task batch terakhir'
SELECT task_name, status, jumlah_baris, left(pesan, 90) AS pesan
FROM dq.batch_log WHERE batch_id = (SELECT batch_id FROM dq.batch_log ORDER BY log_id DESC LIMIT 1)
ORDER BY log_id;
