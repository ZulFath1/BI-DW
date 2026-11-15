-- =====================================================================
-- 02_uji_view_rasio.sql — uji logika dw.v_rasio_stunting_balita dengan data staging asli
-- di dalam transaksi yang DI-ROLLBACK (DWH tidak berubah). Juga pratinjau pemetaan
-- nama kelurahan E1 (Disdukcapil) ↔ E2 (Satu Data) untuk T16/Q07.
-- Acuan dokumen UTS: rasio stunting kota 2025 = 3,85%.
-- =====================================================================
\pset footer off
BEGIN;

-- Alias ejaan (CLAUDE.md 4.2 / UTS T16): nama di sumber -> nama baku
CREATE TEMP TABLE alias (asal text PRIMARY KEY, baku text) ON COMMIT DROP;
INSERT INTO alias VALUES ('BENUA ANYAR', 'BANUA ANYAR'), ('SEB. MESJID', 'SEBERANG MESJID'),
                         ('SEI LULUT', 'SUNGAI LULUT'), ('ANTASAN KECIL', 'ANTASAN KECIL TIMUR');

CREATE TEMP TABLE e2 ON COMMIT DROP AS
SELECT tahun_data, kode_kecamatan_asli, upper(kecamatan_asli) AS kecamatan, kode_kelurahan_asli,
       coalesce(a.baku, upper(trim(kelurahan_asli))) AS kelurahan, stunting_asli::int AS stunting
FROM stg.stunting_kelurahan s LEFT JOIN alias a ON a.asal = upper(trim(s.kelurahan_asli))
WHERE jenis_baris = 'data';

CREATE TEMP TABLE e1 ON COMMIT DROP AS
SELECT kecamatan_asli AS kecamatan, coalesce(a.baku, wilayah_asli) AS kelurahan,
       replace(laki_jiwa_asli, '.', '')::int AS l, replace(perempuan_jiwa_asli, '.', '')::int AS p
FROM stg.penduduk_kelompok_umur s LEFT JOIN alias a ON a.asal = s.wilayah_asli
WHERE tingkat = 'kelurahan' AND jenis_baris = 'data' AND urutan_dalam_blok = 1;   -- 0–4 tahun

\echo '== U01. Pemetaan nama kelurahan E2 2025 ↔ E1 (harap 52 cocok)'
SELECT count(*) AS e2_2025, count(e1.kelurahan) AS cocok_e1,
       string_agg(e2.kelurahan, ', ') FILTER (WHERE e1.kelurahan IS NULL) AS tidak_cocok
FROM e2 LEFT JOIN e1 ON e1.kelurahan = e2.kelurahan AND e1.kecamatan = e2.kecamatan
WHERE e2.tahun_data = 2025;

\echo '== U02. Alias yang benar-benar terpakai'
SELECT 'E2' AS sumber, count(*) FROM stg.stunting_kelurahan s JOIN alias a ON a.asal = upper(trim(s.kelurahan_asli))
UNION ALL SELECT 'E1', count(*) FROM stg.penduduk_kelompok_umur s JOIN alias a ON a.asal = s.wilayah_asli;

-- Isi sementara dimensi & fakta (hanya untuk uji view; di-ROLLBACK)
INSERT INTO dw.dim_kelurahan (kode_wilayah, nama_kelurahan, kode_kecamatan, nama_kecamatan)
SELECT DISTINCT kode_kelurahan_asli, kelurahan, kode_kecamatan_asli, kecamatan FROM e2 WHERE tahun_data = 2025;

INSERT INTO dw.fact_penduduk_kelurahan (kelurahan_key, waktu_key, kelompok_umur_key, jenis_kelamin, jumlah_penduduk, sumber, batch_id)
SELECT k.kelurahan_key, 202512, 1, jk, n, 'uji', 'uji'
FROM e1 JOIN dw.dim_kelurahan k ON k.nama_kelurahan = e1.kelurahan AND k.nama_kecamatan = e1.kecamatan
CROSS JOIN LATERAL (VALUES ('L', e1.l), ('P', e1.p)) v(jk, n);

INSERT INTO dw.fact_stunting_kelurahan (kelurahan_key, waktu_key, stunting, sumber_data, batch_id)
SELECT k.kelurahan_key, (e2.tahun_data * 100 + 12), e2.stunting, 'uji', 'uji'
FROM e2 JOIN dw.dim_kelurahan k ON k.kode_wilayah = e2.kode_kelurahan_asli;

\echo '== U03. View: tingkat kota dan kecamatan (acuan kota 2025 = 3,85%; 2024 = pendekatan)'
SELECT tingkat, nama_kecamatan, tahun, jumlah_stunting, penduduk_0_4, rasio_persen, is_pendekatan
FROM dw.v_rasio_stunting_balita WHERE tingkat <> 'kelurahan' ORDER BY tahun, tingkat DESC, nama_kecamatan;

\echo '== U04. View: jumlah baris tingkat kelurahan per tahun (harap 52)'
SELECT tahun, count(*) FROM dw.v_rasio_stunting_balita WHERE tingkat = 'kelurahan' GROUP BY tahun ORDER BY tahun;

ROLLBACK;
\echo '== U05. Setelah ROLLBACK: isi dw harus kosong kembali'
SELECT (SELECT count(*) FROM dw.dim_kelurahan) AS dim_kelurahan, (SELECT count(*) FROM dw.fact_penduduk_kelurahan) AS fact_penduduk,
       (SELECT count(*) FROM dw.fact_stunting_kelurahan) AS fact_stunting;
