-- =====================================================================
-- 03_dw.sql — Data warehouse (schema dw): galaxy schema (beberapa fakta berbagi dimensi).
-- Rancangan: dokumen UTS BI-DW-001 Rev. 1.3 bagian 5.1–5.2.
--
--   fact_kunjungan_bulanan   posyandu × bulan × kelompok sasaran
--   fact_alat_snapshot       posyandu × alat × snapshot (Juni 2026)
--   fact_kader_snapshot      kader (pseudonim) × posyandu × snapshot
--   fact_penduduk_kelurahan  kelurahan × semester × kelompok umur × jenis kelamin (E1)
--   fact_stunting_kelurahan  kelurahan × tahun (E2)
--   Dimensi bersama: dim_posyandu (SCD tipe 2), dim_kelurahan, dim_waktu, dim_kelompok_sasaran,
--   dim_status_data, dim_alat, dim_kelompok_umur.
--
-- Surrogate key = *_key (integer). Kunci natural disimpan terpisah (posyandu_id, kode_wilayah).
-- Fakta semesteran/tahunan/snapshot memakai waktu_key bulan akhir periode atau bulan snapshot.
-- Idempotent: IF NOT EXISTS (tidak pernah menghapus tabel/data).
-- =====================================================================

-- ------------------------------------------------------------ DIMENSI
CREATE TABLE IF NOT EXISTS dw.dim_waktu (
    waktu_key     integer  PRIMARY KEY,                -- YYYYMM, mis. 202512
    tanggal_awal  date     NOT NULL UNIQUE,
    tahun         smallint NOT NULL,
    semester      smallint NOT NULL CHECK (semester IN (1, 2)),
    kuartal       smallint NOT NULL CHECK (kuartal BETWEEN 1 AND 4),
    bulan         smallint NOT NULL CHECK (bulan BETWEEN 1 AND 12),
    nama_bulan    text     NOT NULL
);
COMMENT ON TABLE dw.dim_waktu IS 'Satu baris per bulan, 2023-01 s.d. 2026-12 (48 bulan); dibangkitkan, bukan dari sumber';

CREATE TABLE IF NOT EXISTS dw.dim_kelompok_sasaran (
    kelompok_key       smallint PRIMARY KEY,           -- 1..5 sesuai urutan kolom sumber
    kode_kelompok      text     NOT NULL UNIQUE,
    nama_kelompok      text     NOT NULL,              -- label seperti di header sumber
    ambang_a2_sasaran  integer  NOT NULL               -- parameter T09 A2 (Q3 + 3×IQR per kelompok, dari UTS)
);
COMMENT ON TABLE dw.dim_kelompok_sasaran IS 'Lima kelompok sasaran posyandu; ambang_a2_sasaran = parameter anomali A2';

CREATE TABLE IF NOT EXISTS dw.dim_status_data (
    status_key   smallint PRIMARY KEY,
    nama_status  text     NOT NULL UNIQUE,
    keterangan   text     NOT NULL
);
COMMENT ON TABLE dw.dim_status_data IS 'Status data per posyandu-bulan-kelompok (aturan T06/T15)';

CREATE TABLE IF NOT EXISTS dw.dim_alat (
    alat_key            smallint PRIMARY KEY,
    kode_alat           text     NOT NULL UNIQUE,
    nama_alat           text     NOT NULL,
    kategori_standar    text     CHECK (kategori_standar IN ('Standar', 'Tidak standar')),
    ketersediaan_data   text     NOT NULL CHECK (ketersediaan_data IN ('tersedia', 'data tidak tersedia')),
    kolom_sumber        text                           -- awalan kolom di stg.antrop
);
COMMENT ON TABLE dw.dim_alat IS 'Lima alat antropometri × kategori standar/tidak standar, ditambah tensimeter (data tidak tersedia, T14)';

CREATE TABLE IF NOT EXISTS dw.dim_kelurahan (
    kelurahan_key   serial  PRIMARY KEY,
    kode_wilayah    text    UNIQUE,                    -- kode Kemendagri, mis. 63.71.01.1001 (dari E2)
    nama_kelurahan  text    NOT NULL,
    kode_kecamatan  text,
    nama_kecamatan  text    NOT NULL,
    UNIQUE (nama_kecamatan, nama_kelurahan)
);
COMMENT ON TABLE dw.dim_kelurahan IS 'Dimensi bersama penghubung data posyandu dan data eksternal (52 kelurahan)';

CREATE TABLE IF NOT EXISTS dw.dim_kelompok_umur (
    kelompok_umur_key   smallint PRIMARY KEY,
    label_umur          text     NOT NULL UNIQUE,      -- label buku Disdukcapil: 0-4 ... >=75
    umur_min            smallint NOT NULL,
    umur_max            smallint,                      -- NULL untuk >=75
    kelompok_key        smallint REFERENCES dw.dim_kelompok_sasaran (kelompok_key),
    ketepatan_pemetaan  text     NOT NULL CHECK (ketepatan_pemetaan IN ('tepat', 'pendekatan'))
);
COMMENT ON TABLE dw.dim_kelompok_umur IS '16 kelompok umur lima tahunan + pemetaan ke kelompok sasaran posyandu (tepat/pendekatan)';

CREATE TABLE IF NOT EXISTS dw.dim_posyandu (
    posyandu_key          serial   PRIMARY KEY,        -- surrogate key, satu per VERSI
    posyandu_id           text     NOT NULL,           -- kunci tetap dari nama ternormalisasi (T05)
    versi                 smallint NOT NULL DEFAULT 1,
    nama_posyandu         text     NOT NULL,
    nama_posyandu_normal  text     NOT NULL,
    kelurahan_key         integer  NOT NULL REFERENCES dw.dim_kelurahan (kelurahan_key),
    puskesmas             text     NOT NULL,           -- atribut SCD tipe 2
    berlaku_mulai         date     NOT NULL,
    berlaku_sampai        date     NOT NULL DEFAULT DATE '9999-12-31',
    is_current            boolean  NOT NULL DEFAULT true,
    tahun_pertama_lapor   smallint,
    tahun_terakhir_lapor  smallint,
    muncul_kembali        boolean  NOT NULL DEFAULT false,
    UNIQUE (posyandu_id, versi),
    CHECK (berlaku_sampai >= berlaku_mulai)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_posyandu_current ON dw.dim_posyandu (posyandu_id) WHERE is_current;
COMMENT ON TABLE dw.dim_posyandu IS 'Posyandu, SCD tipe 2 untuk puskesmas/kecamatan (perkiraan UTS: 490 posyandu, 503 versi)';

-- ------------------------------------------------------------ FAKTA
CREATE TABLE IF NOT EXISTS dw.fact_kunjungan_bulanan (
    posyandu_key             integer       NOT NULL REFERENCES dw.dim_posyandu (posyandu_key),
    waktu_key                integer       NOT NULL REFERENCES dw.dim_waktu (waktu_key),
    kelompok_key             smallint      NOT NULL REFERENCES dw.dim_kelompok_sasaran (kelompok_key),
    status_key               smallint      NOT NULL REFERENCES dw.dim_status_data (status_key),
    sasaran                  integer,                  -- NULL = tidak diisi (bukan 0)
    cakupan                  integer,
    persen_dilayani          numeric(9, 2),            -- dihitung ulang: cakupan / sasaran × 100 (T10)
    persen_dilayani_laporan  numeric(12, 2),           -- nilai asli laporan setelah konversi teks (T03)
    dirujuk                  integer,                  -- apa adanya, reliabilitas rendah (T11)
    persen_dirujuk           numeric(12, 2),
    is_cakupan_turunan       boolean       NOT NULL DEFAULT false,   -- T07
    flag_anomali             integer       NOT NULL DEFAULT 0 CHECK (flag_anomali >= 0),
        -- bitmask T09/T10: 1 A1 ekstrem, 2 A2 tinggi, 4 A3 lonjakan, 8 cakupan>sasaran,
        --                  16 negatif, 32 sasaran 0 tapi cakupan>0, 64 selisih persen > 1 poin
    rujukan_reliabilitas     text          NOT NULL DEFAULT 'rendah',
    tahun_sheet              smallint      NOT NULL,   -- lineage ke stg.kunjungan_bulanan
    baris_sumber             integer       NOT NULL,
    batch_id                 text          NOT NULL,
    PRIMARY KEY (posyandu_key, waktu_key, kelompok_key)
);
CREATE INDEX IF NOT EXISTS ix_fkb_waktu_kelompok ON dw.fact_kunjungan_bulanan (waktu_key, kelompok_key);
COMMENT ON TABLE dw.fact_kunjungan_bulanan IS 'Grain: posyandu × bulan × kelompok sasaran (maks. 106.020 baris)';

CREATE TABLE IF NOT EXISTS dw.fact_alat_snapshot (
    posyandu_key   integer  NOT NULL REFERENCES dw.dim_posyandu (posyandu_key),
    alat_key       smallint NOT NULL REFERENCES dw.dim_alat (alat_key),
    waktu_key      integer  NOT NULL REFERENCES dw.dim_waktu (waktu_key),   -- bulan cut off snapshot
    jumlah_baik    integer  CHECK (jumlah_baik >= 0),
    jumlah_rusak   integer  CHECK (jumlah_rusak >= 0),
    baris_sumber   integer  NOT NULL,
    batch_id       text     NOT NULL,
    PRIMARY KEY (posyandu_key, alat_key, waktu_key)
);
COMMENT ON TABLE dw.fact_alat_snapshot IS 'Grain: posyandu × alat × snapshot (maks. 490 × 10 = 4.900)';

CREATE TABLE IF NOT EXISTS dw.fact_kader_snapshot (
    kader_snapshot_key   bigserial PRIMARY KEY,
    kader_kode           text      NOT NULL,           -- pseudonim HMAC; BUKAN nama, bukan identitas unik orang
    posyandu_key         integer   NOT NULL REFERENCES dw.dim_posyandu (posyandu_key),
    waktu_key            integer   NOT NULL REFERENCES dw.dim_waktu (waktu_key),
    kelompok_umur_kader  text,                         -- usia asli tidak disimpan
    jenis_kelamin        char(1)   NOT NULL CHECK (jenis_kelamin IN ('L', 'P')),   -- T13
    pendidikan           text,
    kepemilikan_jkn      boolean,
    tingkatan_kader      text,
    pelatihan_25         boolean,
    penilaian_25         text      CHECK (penilaian_25 IN ('Ya', 'Tidak', 'Tidak valid')),
    is_pasangan_ganda    boolean   NOT NULL DEFAULT false,
    baris_sumber         integer   NOT NULL,
    batch_id             text      NOT NULL,
    UNIQUE (waktu_key, baris_sumber)
);
COMMENT ON TABLE dw.fact_kader_snapshot IS 'Grain: kader (pseudonim) × posyandu × snapshot; tanpa nama/alamat (Q06)';

CREATE TABLE IF NOT EXISTS dw.fact_penduduk_kelurahan (
    kelurahan_key        integer  NOT NULL REFERENCES dw.dim_kelurahan (kelurahan_key),
    waktu_key            integer  NOT NULL REFERENCES dw.dim_waktu (waktu_key),        -- bulan akhir semester
    kelompok_umur_key    smallint NOT NULL REFERENCES dw.dim_kelompok_umur (kelompok_umur_key),
    jenis_kelamin        char(1)  NOT NULL CHECK (jenis_kelamin IN ('L', 'P')),
    jumlah_penduduk      integer  NOT NULL CHECK (jumlah_penduduk >= 0),
    is_umur_dari_urutan  boolean  NOT NULL DEFAULT false,  -- label umur sumber rusak, dipetakan dari urutan baris
    sumber               text     NOT NULL,
    batch_id             text     NOT NULL,
    PRIMARY KEY (kelurahan_key, waktu_key, kelompok_umur_key, jenis_kelamin)
);
COMMENT ON TABLE dw.fact_penduduk_kelurahan IS 'E1 Disdukcapil: kelurahan × semester × kelompok umur × jenis kelamin';

CREATE TABLE IF NOT EXISTS dw.fact_stunting_kelurahan (
    kelurahan_key          integer  NOT NULL REFERENCES dw.dim_kelurahan (kelurahan_key),
    waktu_key              integer  NOT NULL REFERENCES dw.dim_waktu (waktu_key),      -- Desember tahun data
    stunting               integer  CHECK (stunting >= 0),     -- JUMLAH balita, bukan prevalensi
    wasting                integer  CHECK (wasting >= 0),
    underweight            integer  CHECK (underweight >= 0),
    sumber_data            text     NOT NULL,
    tanggal_data_stunting  text,                               -- dari catatan portal
    tanggal_data_gizi      text,
    flag_kualitas          text,
    batch_id               text     NOT NULL,
    PRIMARY KEY (kelurahan_key, waktu_key)
);
COMMENT ON TABLE dw.fact_stunting_kelurahan IS 'E2 Dinkes (Satu Data): jumlah balita stunting/wasting/underweight per kelurahan per tahun';

-- =====================================================================
-- Tambahan minggu 23–29 Nov (keputusan K16, K21, dan grain kader; lihat docs/KEPUTUSAN.md).
-- ALTER ... IF [NOT] EXISTS agar database yang sudah dibuat ikut diperbarui (idempotent).
-- =====================================================================

-- K21: dim_waktu memuat anggota "bulan tidak diketahui" per tahun (waktu_key YYYY00) untuk
-- snapshot yang sumbernya hanya menyebut tahun (sheet Kader: "Tahun 2026"). Anggota ini tidak
-- punya tanggal/bulan/semester/kuartal, sehingga kolom tersebut boleh NULL. Bulan Desember 2026
-- TIDAK dipakai karena bulan itu belum terjadi saat data dibuat.
ALTER TABLE dw.dim_waktu ALTER COLUMN tanggal_awal DROP NOT NULL;
ALTER TABLE dw.dim_waktu ALTER COLUMN semester     DROP NOT NULL;
ALTER TABLE dw.dim_waktu ALTER COLUMN kuartal      DROP NOT NULL;
ALTER TABLE dw.dim_waktu ALTER COLUMN bulan        DROP NOT NULL;
ALTER TABLE dw.dim_waktu ADD COLUMN IF NOT EXISTS is_bulan_diketahui boolean NOT NULL DEFAULT true;
ALTER TABLE dw.dim_waktu DROP CONSTRAINT IF EXISTS ck_dim_waktu_anggota;
ALTER TABLE dw.dim_waktu ADD CONSTRAINT ck_dim_waktu_anggota CHECK (
    (is_bulan_diketahui AND tanggal_awal IS NOT NULL AND bulan IS NOT NULL AND semester IS NOT NULL AND kuartal IS NOT NULL)
    OR (NOT is_bulan_diketahui AND tanggal_awal IS NULL AND bulan IS NULL AND waktu_key % 100 = 0));
COMMENT ON TABLE dw.dim_waktu IS
    '48 bulan (2023-01 s.d. 2026-12) + anggota "bulan tidak diketahui" (YYYY00) untuk snapshot tanpa bulan (K21)';

-- K21 + grain kader: satu baris = satu baris kader per posyandu pada snapshot (surrogate key
-- kader_snapshot_key). kader_kode hanya untuk mendeteksi pasangan kader-posyandu ganda; kode
-- BUKAN identitas orang (nama sama -> kode sama; lihat K19).
ALTER TABLE dw.fact_kader_snapshot ADD COLUMN IF NOT EXISTS periode_snapshot text;
COMMENT ON TABLE dw.fact_kader_snapshot IS
    'Grain: satu baris kader per posyandu per snapshot (kader_snapshot_key). kader_kode bukan identitas orang; tanpa nama/alamat (Q06)';
COMMENT ON COLUMN dw.fact_kader_snapshot.kader_kode IS
    'Pseudonim HMAC dari nama; hanya untuk mendeteksi pasangan ganda. Bukan identitas unik orang (K19)';
COMMENT ON COLUMN dw.fact_kader_snapshot.periode_snapshot IS
    'Periode snapshot sesuai sumber (mis. ''Tahun 2026''); waktu_key menunjuk anggota bulan tidak diketahui';

-- K16: penanda kualitas baris penduduk (mis. 'label umur dari urutan baris' untuk Kelayan Tengah).
ALTER TABLE dw.fact_penduduk_kelurahan ADD COLUMN IF NOT EXISTS flag_kualitas text;

-- Lineage fakta alat ke baris sumber dan penanda nilai '-' (T03/T14).
ALTER TABLE dw.fact_alat_snapshot ADD COLUMN IF NOT EXISTS flag_kualitas text;
