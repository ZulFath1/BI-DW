-- =====================================================================
-- 04_mart.sql — validasi data mart & hasil ML terhadap angka acuan (CLAUDE.md 4.6, K37, K38).
--   docker compose exec -T postgres sh -c 'psql -U "$ETL_USER" -d "$DW_DB"' < sql/validasi/04_mart.sql
-- =====================================================================
\pset footer off

\echo '== M01 Cakupan 2025 per kelompok, filter KPI K38 (acuan ibu hamil 56,6%; bayi balita 58,7%)'
SELECT nama_kelompok, sum(cakupan) AS cakupan, sum(sasaran) AS sasaran,
       round(100.0 * sum(cakupan) / sum(sasaran), 2) AS persen
FROM mart.kpi_cakupan_bulanan WHERE tahun = 2025 GROUP BY kelompok_key, nama_kelompok ORDER BY kelompok_key;

\echo '== M02 Kelengkapan Desember 2025 (acuan 475 dari 487)'
SELECT sum(n_posyandu_terlapor) AS terlapor, sum(n_posyandu) AS posyandu FROM mart.kelengkapan_bulanan WHERE waktu_key = 202512;

\echo '== M03 Baris anomali per tahun (acuan total 2.073; 2025 = 826)'
SELECT tahun, n_baris FROM mart.ringkasan_kualitas WHERE item = 'Total baris anomali'
UNION ALL SELECT NULL, sum(n_baris) FROM mart.ringkasan_kualitas WHERE item = 'Total baris anomali' ORDER BY 1 NULLS LAST;

\echo '== M04 Status data per tahun (K37: status 5 = Cakupan tanpa sasaran; 1 / 5 / 382 / 71)'
SELECT item, sum(n_baris) FILTER (WHERE tahun = 2023) AS "2023", sum(n_baris) FILTER (WHERE tahun = 2024) AS "2024",
       sum(n_baris) FILTER (WHERE tahun = 2025) AS "2025", sum(n_baris) FILTER (WHERE tahun = 2026) AS "2026"
FROM mart.ringkasan_kualitas WHERE kategori = 'status' GROUP BY item ORDER BY item;

\echo '== M05 Kader < 5 (acuan 31 dari 482, P07); alat rusak/tidak standar (335 dari 490); 5 alat standar baik (164)'
SELECT count(*) FILTER (WHERE jumlah_kader BETWEEN 1 AND 4) AS kader_kurang_5, count(*) FILTER (WHERE jumlah_kader > 0) AS berkader,
       count(*) FILTER (WHERE ada_alat_rusak_tdk_standar) AS alat_rusak_tstd, count(*) FILTER (WHERE jenis_alat_std_baik = 5) AS alat_lengkap,
       count(*) AS posyandu, count(*) FILTER (WHERE is_layak_ml) AS layak_ml
FROM mart.posyandu_profil;

\echo '== M06 Sasaran bayi balita Des 2025 per kecamatan (acuan kota 23.299) dan rasio stunting kota 2025 (acuan 3,85%)'
SELECT coalesce(nama_kecamatan, 'KOTA') AS kecamatan, sum(sasaran_bayi_balita_des) AS sasaran_des,
       round(100.0 * sum(jumlah_stunting) / sum(penduduk_0_4), 2) AS rasio_stunting
FROM mart.kpi_kelurahan WHERE tahun = 2025 GROUP BY ROLLUP (nama_kecamatan) ORDER BY 1;

\echo '== M07 Model ML terpilih dan pembanding'
SELECT model_id, k, round(silhouette, 3) AS silhouette, round(davies_bouldin, 3) AS dbi,
       round(stabilitas_ari, 3) AS ari, ukuran_cluster, is_terpilih
FROM mart.ml_evaluasi
WHERE is_terpilih OR algoritma = 'aturan' OR model_id IN ('kmeans_v2_internal+eksternal_k6', 'agglo_v1_internal_k3', 'kmeans_v1_internal_k6')
ORDER BY is_terpilih DESC, model_id;

\echo '== M08 Profil cluster (rata-rata fitur, skala asli)'
SELECT label_cluster, n_posyandu, fitur, round(rata_rata, 2) AS rata_rata, round(rata_rata_kota, 2) AS kota, round(z_score, 2) AS z
FROM mart.cluster_profil ORDER BY label_cluster, fitur;

\echo '== M09 Q06 di mart: tidak ada kolom nama/alamat/usia kader'
SELECT count(*) AS kolom_terlarang FROM information_schema.columns
WHERE table_schema = 'mart' AND (column_name ~* 'alamat' OR column_name ~* 'nama_kader' OR column_name ~* '(^|_)usia($|_)');

\echo '== M10 mart.kunjungan_posyandu_bulanan = fakta (106.020 baris) dan merekonsiliasi KPI & anomali (selisih harus 0)'
SELECT (SELECT count(*) FROM mart.kunjungan_posyandu_bulanan) AS baris_mart,
       (SELECT count(*) FROM dw.fact_kunjungan_bulanan) AS baris_fakta,
       (SELECT sum(cakupan_kpi) FROM mart.kunjungan_posyandu_bulanan) - (SELECT sum(cakupan) FROM mart.kpi_cakupan_bulanan) AS selisih_cakupan,
       (SELECT sum(sasaran_kpi) FROM mart.kunjungan_posyandu_bulanan) - (SELECT sum(sasaran) FROM mart.kpi_cakupan_bulanan) AS selisih_sasaran,
       (SELECT count(*) FILTER (WHERE flag_anomali > 0) FROM mart.kunjungan_posyandu_bulanan)
         - (SELECT sum(n_baris) FROM mart.ringkasan_kualitas WHERE item = 'Total baris anomali') AS selisih_anomali;

\echo '== M11 Daftar prioritas ML: peringkat unik 1..n, tingkat per cluster konsisten'
SELECT count(*) AS posyandu, count(DISTINCT peringkat_prioritas) AS peringkat_unik, min(peringkat_prioritas) AS min,
       max(peringkat_prioritas) AS max, count(*) FILTER (WHERE alasan_prioritas IS NULL) AS tanpa_alasan
FROM mart.hasil_cluster;
SELECT p.label_cluster, p.tingkat_prioritas, p.n_posyandu, count(h.*) AS di_hasil,
       min(h.peringkat_prioritas) AS peringkat_min, max(h.peringkat_prioritas) AS peringkat_maks
FROM (SELECT DISTINCT label_cluster, tingkat_prioritas, n_posyandu FROM mart.cluster_profil) p
JOIN mart.hasil_cluster h ON h.label_cluster = p.label_cluster AND h.tingkat_prioritas = p.tingkat_prioritas
GROUP BY 1, 2, 3 ORDER BY 1;
