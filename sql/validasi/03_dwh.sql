-- =====================================================================
-- 03_dwh.sql — query validasi DWH terhadap angka acuan (CLAUDE.md 4.6 / dokumen UTS).
-- Jalankan:  docker compose exec -T postgres psql -U etl_user -d posyandu_dw -f - < sql/validasi/03_dwh.sql
-- Hanya membaca; tidak menampilkan data pribadi (kader hanya agregat).
-- =====================================================================
\echo '== D01 Jumlah baris dimensi & fakta (acuan: dim_posyandu 503, dim_kelurahan 52, dim_waktu 48+1, dim_alat 11, fakta kunjungan 106.020, alat 4.900, kader 2.543 di UTS)'
SELECT 'dim_posyandu' AS tabel, count(*) FROM dw.dim_posyandu
UNION ALL SELECT 'dim_posyandu (posyandu unik)', count(DISTINCT posyandu_id) FROM dw.dim_posyandu
UNION ALL SELECT 'dim_kelurahan', count(*) FROM dw.dim_kelurahan
UNION ALL SELECT 'dim_waktu', count(*) FROM dw.dim_waktu
UNION ALL SELECT 'dim_alat', count(*) FROM dw.dim_alat
UNION ALL SELECT 'fact_kunjungan_bulanan', count(*) FROM dw.fact_kunjungan_bulanan
UNION ALL SELECT 'fact_alat_snapshot', count(*) FROM dw.fact_alat_snapshot
UNION ALL SELECT 'fact_kader_snapshot', count(*) FROM dw.fact_kader_snapshot
UNION ALL SELECT 'fact_penduduk_kelurahan', count(*) FROM dw.fact_penduduk_kelurahan
UNION ALL SELECT 'fact_stunting_kelurahan', count(*) FROM dw.fact_stunting_kelurahan;

\echo '== D02 Baris anomali per tahun (acuan 2023–2026 total 2.073; 2025 = 826) dan per bit'
SELECT w.tahun,
       count(*) FILTER (WHERE flag_anomali > 0)       AS baris_anomali,
       count(*) FILTER (WHERE flag_anomali & 1 > 0)   AS a1_ekstrem,
       count(*) FILTER (WHERE flag_anomali & 2 > 0)   AS a2_tinggi,
       count(*) FILTER (WHERE flag_anomali & 4 > 0)   AS a3_lonjakan,
       count(*) FILTER (WHERE flag_anomali & 8 > 0)   AS cakupan_gt_sasaran,
       count(*) FILTER (WHERE flag_anomali & 16 > 0)  AS negatif,
       count(*) FILTER (WHERE flag_anomali & 32 > 0)  AS sasaran0_cakupan,
       count(*) FILTER (WHERE flag_anomali & 64 > 0)  AS selisih_persen,
       count(*) FILTER (WHERE is_cakupan_turunan)     AS t07_turunan
FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_waktu w USING (waktu_key)
GROUP BY ROLLUP (w.tahun) ORDER BY w.tahun;

\echo '== D03 Status data per tahun'
SELECT w.tahun, s.nama_status, count(*)
FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_waktu w USING (waktu_key) JOIN dw.dim_status_data s USING (status_key)
GROUP BY 1, 2 ORDER BY 1, 2;

\echo '== D04 Kelengkapan Desember 2025 (acuan 475 dari 487)'
SELECT count(DISTINCT p.posyandu_id) FILTER (WHERE f.status_key = 1) AS posyandu_terlapor,
       count(DISTINCT p.posyandu_id) AS posyandu_aktif
FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_posyandu p USING (posyandu_key)
WHERE f.waktu_key = 202512;

\echo '== D05 Cakupan 2025 = Σ cakupan ÷ Σ sasaran, Terlapor, tanpa A1 (acuan bayi balita 58,7%; ibu hamil 56,6%)'
SELECT k.nama_kelompok, sum(cakupan) AS cakupan, sum(sasaran) AS sasaran,
       round(100.0 * sum(cakupan) / sum(sasaran), 2) AS persen
FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_kelompok_sasaran k USING (kelompok_key)
WHERE f.waktu_key BETWEEN 202501 AND 202512 AND f.status_key = 1 AND f.flag_anomali & 1 = 0
GROUP BY k.kelompok_key, k.nama_kelompok ORDER BY k.kelompok_key;

\echo '== D06 Sasaran bayi balita Des 2025 per kecamatan (acuan Tengah 3.499, Utara 6.281, Barat 5.151, Selatan 5.530, Timur 2.838; kota 23.299)'
SELECT coalesce(kl.nama_kecamatan, 'KOTA') AS kecamatan, sum(f.sasaran) AS sasaran_bayi_balita
FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_posyandu p USING (posyandu_key) JOIN dw.dim_kelurahan kl USING (kelurahan_key)
WHERE f.waktu_key = 202512 AND f.kelompok_key = 2
GROUP BY ROLLUP (kl.nama_kecamatan) ORDER BY 1;

\echo '== D07 Posyandu dengan < 5 baris kader (acuan UTS 32 dari 483; lihat P07)'
WITH n AS (SELECT posyandu_key, count(*) AS n FROM dw.fact_kader_snapshot GROUP BY 1)
SELECT count(*) FILTER (WHERE n < 5) AS kurang_dari_5, count(*) AS posyandu_berkader FROM n;

\echo '== D08 Alat: minimal satu alat rusak/tidak standar (acuan 335 dari 490); lengkap 5 alat standar baik (acuan 164)'
WITH a AS (
    SELECT f.posyandu_key,
           bool_or(coalesce(f.jumlah_rusak, 0) > 0 OR (d.kategori_standar = 'Tidak standar' AND coalesce(f.jumlah_baik, 0) > 0)) AS bermasalah,
           count(*) FILTER (WHERE d.kategori_standar = 'Standar' AND f.jumlah_baik > 0) AS standar_baik
    FROM dw.fact_alat_snapshot f JOIN dw.dim_alat d USING (alat_key) GROUP BY 1)
SELECT count(*) FILTER (WHERE bermasalah) AS bermasalah, count(*) FILTER (WHERE standar_baik = 5) AS lengkap_standar,
       count(*) AS posyandu FROM a;

\echo '== D09 Rasio stunting (acuan kota 2025 3,85%) — rasio terhadap penduduk balita, bukan prevalensi'
SELECT tingkat, nama_kecamatan, tahun, jumlah_stunting, penduduk_0_4, rasio_persen, is_pendekatan
FROM dw.v_rasio_stunting_balita WHERE tingkat <> 'kelurahan' ORDER BY tahun, tingkat DESC, nama_kecamatan;

\echo '== D10 SCD tipe 2: posyandu dengan lebih dari satu versi'
SELECT a.puskesmas AS puskesmas_lama, b.puskesmas AS puskesmas_baru, b.berlaku_mulai, count(*) AS posyandu
FROM dw.dim_posyandu a JOIN dw.dim_posyandu b ON a.posyandu_id = b.posyandu_id AND b.versi = a.versi + 1
GROUP BY 1, 2, 3;

\echo '== D11 Kader snapshot: waktu & periode (K21) dan penanda kualitas penduduk (K16)'
SELECT f.waktu_key, w.nama_bulan, w.is_bulan_diketahui, f.periode_snapshot, count(*)
FROM dw.fact_kader_snapshot f JOIN dw.dim_waktu w USING (waktu_key) GROUP BY 1, 2, 3, 4;
SELECT k.nama_kelurahan, f.flag_kualitas, count(*) AS baris, sum(jumlah_penduduk) AS penduduk
FROM dw.fact_penduduk_kelurahan f JOIN dw.dim_kelurahan k USING (kelurahan_key)
WHERE f.flag_kualitas IS NOT NULL GROUP BY 1, 2;

\echo '== D12 Pemetaan kelompok umur -> kelompok sasaran (K20)'
SELECT u.label_umur, s.nama_kelompok, u.ketepatan_pemetaan
FROM dw.dim_kelompok_umur u LEFT JOIN dw.dim_kelompok_sasaran s USING (kelompok_key) ORDER BY u.kelompok_umur_key;

\echo '== D13 Pemetaan nama posyandu Antrop & Kader (T14)'
SELECT sumber, cara, count(*) FROM curated.pemetaan_posyandu WHERE sumber IN ('antrop', 'kader') GROUP BY 1, 2 ORDER BY 1, 2;
