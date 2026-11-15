-- =====================================================================
-- 06_curated.sql — schema curated: hasil Transform (T01–T19) yang sudah bersih dan bertipe,
-- memakai KUNCI NATURAL (posyandu_id, nama kelurahan, kode wilayah), belum surrogate key.
--
-- Alur: stg (nilai asli) -> transform -> curated -> quality gate (Q01–Q09) -> load -> dw.
-- Curated dimuat ulang penuh setiap batch dalam SATU transaksi; quality gate membaca curated,
-- dan hanya bila lolos, task load_dw memindahkan isinya ke dw (dengan surrogate key).
-- PRIVASI: tidak ada nama/alamat kader; usia hanya sebagai kelompok umur.
-- Idempotent: IF NOT EXISTS.
-- =====================================================================

-- Daftar kelurahan resmi (kode Kemendagri dari E2) — sumber dim_kelurahan.
CREATE TABLE IF NOT EXISTS curated.kelurahan (
    kode_wilayah    text PRIMARY KEY,
    nama_kelurahan  text NOT NULL UNIQUE,
    kode_kecamatan  text NOT NULL,
    nama_kecamatan  text NOT NULL,
    batch_id        text NOT NULL
);

-- Versi posyandu (SCD tipe 2, T05) — sumber dim_posyandu.
CREATE TABLE IF NOT EXISTS curated.posyandu_versi (
    posyandu_id           text     NOT NULL,
    versi                 smallint NOT NULL,
    nama_posyandu         text     NOT NULL,     -- nama tampilan (T04) dari laporan terbaru
    nama_posyandu_normal  text     NOT NULL,     -- tanpa awalan POSYANDU/PYD dan akhiran kelurahan
    nama_kelurahan        text     NOT NULL REFERENCES curated.kelurahan (nama_kelurahan),
    puskesmas             text     NOT NULL,
    berlaku_mulai         date     NOT NULL,
    berlaku_sampai        date     NOT NULL,
    is_current            boolean  NOT NULL,
    tahun_pertama_lapor   smallint NOT NULL,
    tahun_terakhir_lapor  smallint NOT NULL,
    muncul_kembali        boolean  NOT NULL,
    batch_id              text     NOT NULL,
    PRIMARY KEY (posyandu_id, versi)
);

-- Tabel pemetaan nama posyandu sumber -> posyandu_id (T05, T14): untuk ditinjau manual.
CREATE TABLE IF NOT EXISTS curated.pemetaan_posyandu (
    sumber              text     NOT NULL,       -- laporan_<tahun> | antrop | kader
    baris_sumber        integer  NOT NULL,
    kelurahan_asli      text,
    nama_posyandu_asli  text,
    kunci_cocok         text,                    -- bentuk kanonik yang dibandingkan
    posyandu_id         text,                    -- NULL = tidak terpetakan (ditahan)
    cara                text     NOT NULL CHECK (cara IN ('kunci', 'manual', 'fuzzy', 'tidak_terpetakan')),
    skor                numeric(4, 3),           -- kemiripan (fuzzy)
    keterangan          text,
    batch_id            text     NOT NULL,
    PRIMARY KEY (sumber, baris_sumber)
);

-- Kunjungan bulanan (T01–T11, T15) — grain posyandu × bulan × kelompok.
CREATE TABLE IF NOT EXISTS curated.kunjungan_bulanan (
    posyandu_id              text          NOT NULL,
    waktu_key                integer       NOT NULL,       -- YYYYMM
    kelompok_key             smallint      NOT NULL,
    status_key               smallint      NOT NULL,
    sasaran                  integer,
    cakupan                  integer,
    persen_dilayani          numeric(9, 2),
    persen_dilayani_laporan  numeric(12, 2),
    dirujuk                  integer,
    persen_dirujuk           numeric(12, 2),
    is_cakupan_turunan       boolean       NOT NULL,
    flag_anomali             integer       NOT NULL,
    tahun_sheet              smallint      NOT NULL,
    baris_sumber             integer       NOT NULL,
    batch_id                 text          NOT NULL,
    PRIMARY KEY (posyandu_id, waktu_key, kelompok_key)
);

-- Alat antropometri (T14) — grain posyandu × alat × snapshot.
CREATE TABLE IF NOT EXISTS curated.alat_snapshot (
    posyandu_id    text     NOT NULL,
    alat_key       smallint NOT NULL,
    waktu_key      integer  NOT NULL,
    jumlah_baik    integer,
    jumlah_rusak   integer,
    flag_kualitas  text,
    baris_sumber   integer  NOT NULL,
    batch_id       text     NOT NULL,
    PRIMARY KEY (posyandu_id, alat_key, waktu_key)
);

-- Kader (T12–T13) — grain: satu baris kader per posyandu per snapshot (baris_sumber unik).
CREATE TABLE IF NOT EXISTS curated.kader_snapshot (
    baris_sumber         integer  NOT NULL,
    waktu_key            integer  NOT NULL,
    periode_snapshot     text     NOT NULL,
    kader_kode           text     NOT NULL,
    posyandu_id          text     NOT NULL,
    kelompok_umur_kader  text,
    jenis_kelamin        char(1)  NOT NULL,
    pendidikan           text,
    kepemilikan_jkn      boolean,
    tingkatan_kader      text,
    pelatihan_25         boolean,
    penilaian_25         text,
    is_pasangan_ganda    boolean  NOT NULL,
    batch_id             text     NOT NULL,
    PRIMARY KEY (waktu_key, baris_sumber)
);

-- Penduduk per kelurahan (E1, T16–T17) — grain kelurahan × semester × kelompok umur × jenis kelamin.
CREATE TABLE IF NOT EXISTS curated.penduduk_kelurahan (
    nama_kelurahan     text     NOT NULL,
    waktu_key          integer  NOT NULL,          -- bulan akhir semester (Sem. II 2025 -> 202512)
    kelompok_umur_key  smallint NOT NULL,
    jenis_kelamin      char(1)  NOT NULL,
    jumlah_penduduk    integer  NOT NULL,
    is_umur_dari_urutan boolean NOT NULL,
    flag_kualitas      text,
    sumber             text     NOT NULL,
    batch_id           text     NOT NULL,
    PRIMARY KEY (nama_kelurahan, waktu_key, kelompok_umur_key, jenis_kelamin)
);

-- Stunting per kelurahan (E2, T16, T18) — grain kelurahan × tahun.
CREATE TABLE IF NOT EXISTS curated.stunting_kelurahan (
    kode_wilayah           text     NOT NULL,
    waktu_key              integer  NOT NULL,      -- Desember tahun data
    stunting               integer,
    wasting                integer,
    underweight            integer,
    sumber_data            text     NOT NULL,
    tanggal_data_stunting  text,
    tanggal_data_gizi      text,
    flag_kualitas          text,
    batch_id               text     NOT NULL,
    PRIMARY KEY (kode_wilayah, waktu_key)
);
