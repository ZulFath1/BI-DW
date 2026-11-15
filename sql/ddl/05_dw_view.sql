-- =====================================================================
-- 05_dw_view.sql — v_rasio_stunting_balita (T19)
--
-- Rasio = jumlah balita stunting ÷ penduduk 0–4 tahun × 100, per kelurahan, kecamatan, dan kota.
-- Label: "rasio terhadap penduduk balita", BUKAN prevalensi (stunting hanya menghitung balita
-- yang diukur, penyebutnya penduduk terdaftar DKB).
-- Pasangan tahun: stunting tahun T dengan penduduk semester pada tahun T. Bila tidak ada
-- (mis. 2024, karena hanya buku Sem. II 2025 yang tersedia) dipakai semester terdekat yang ada
-- dan is_pendekatan = true.
-- Rasio kecamatan/kota = Σ stunting ÷ Σ penduduk (bukan rata-rata rasio kelurahan).
-- =====================================================================
CREATE OR REPLACE VIEW dw.v_rasio_stunting_balita AS
WITH balita AS (
    SELECT p.kelurahan_key, p.waktu_key, w.tahun, sum(p.jumlah_penduduk) AS penduduk_0_4
    FROM dw.fact_penduduk_kelurahan p
    JOIN dw.dim_kelompok_umur u ON u.kelompok_umur_key = p.kelompok_umur_key
    JOIN dw.dim_waktu w ON w.waktu_key = p.waktu_key
    WHERE u.label_umur = '0-4'
    GROUP BY p.kelurahan_key, p.waktu_key, w.tahun
), pasangan AS (
    SELECT s.kelurahan_key, ws.tahun, s.stunting, b.penduduk_0_4, b.waktu_key AS waktu_penduduk_key,
           (b.tahun <> ws.tahun) AS is_pendekatan
    FROM dw.fact_stunting_kelurahan s
    JOIN dw.dim_waktu ws ON ws.waktu_key = s.waktu_key
    JOIN LATERAL (
        SELECT * FROM balita b
        WHERE b.kelurahan_key = s.kelurahan_key
        ORDER BY abs(b.tahun - ws.tahun), b.waktu_key DESC
        LIMIT 1
    ) b ON true
)
SELECT CASE WHEN grouping(k.nama_kelurahan) = 0 THEN 'kelurahan'
            WHEN grouping(k.nama_kecamatan) = 0 THEN 'kecamatan'
            ELSE 'kota' END                                   AS tingkat,
       k.nama_kecamatan,
       k.nama_kelurahan,
       p.tahun,
       sum(p.stunting)                                        AS jumlah_stunting,
       sum(p.penduduk_0_4)                                    AS penduduk_0_4,
       round(100.0 * sum(p.stunting) / nullif(sum(p.penduduk_0_4), 0), 2) AS rasio_persen,
       bool_or(p.is_pendekatan)                               AS is_pendekatan,
       max(p.waktu_penduduk_key)                              AS waktu_penduduk_key,
       'rasio terhadap penduduk balita (bukan prevalensi)'::text AS label
FROM pasangan p
JOIN dw.dim_kelurahan k ON k.kelurahan_key = p.kelurahan_key
GROUP BY GROUPING SETS ((p.tahun), (p.tahun, k.nama_kecamatan), (p.tahun, k.nama_kecamatan, k.nama_kelurahan));

COMMENT ON VIEW dw.v_rasio_stunting_balita IS 'T19: stunting ÷ penduduk 0–4 tahun; rasio, bukan prevalensi';
