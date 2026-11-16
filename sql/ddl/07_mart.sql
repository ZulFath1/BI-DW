-- =====================================================================
-- 07_mart.sql — Data mart (schema mart) untuk dashboard dan machine learning.
--
-- Semua tabel mart DIBANGUN ULANG dari schema dw oleh task `build_mart` (etl/build_mart.py)
-- dan `train_ml` (ml/clustering.py) dalam satu transaksi per task. Mart tidak menyimpan
-- data yang tidak ada di dw, sehingga selalu dapat dibangun ulang.
--
--   kpi_cakupan_bulanan   kecamatan × bulan × kelompok sasaran: Σ cakupan, Σ sasaran, persen (K38)
--   posyandu_profil       satu baris per posyandu (versi aktif): fitur 2025 + alat + kader + stunting
--   kpi_kelurahan         kelurahan × tahun: stunting, penduduk 0–4, rasio (T19), sasaran & cakupan balita
--   ringkasan_kualitas    tahun × kategori × item: jumlah baris per status data dan per jenis anomali
--   quality_gate_terakhir hasil Q01–Q09 batch terakhir (monitoring di dashboard)
--   hasil_cluster         posyandu × model terpilih: cluster dan labelnya (ML)
--   cluster_profil        rata-rata fitur per cluster model terpilih (interpretasi)
--   ml_evaluasi           setiap model yang dicoba: metrik Silhouette, Davies-Bouldin, inertia
--   dashboard_refresh     penanda refresh: dashboard membersihkan cache bila batch berubah
--   kunjungan_posyandu_bulanan  posyandu × bulan × kelompok (filter dashboard per puskesmas/cluster)
--
-- Tidak ada kolom nama/alamat/usia kader di mart (diperiksa Q06).
-- Idempotent: IF NOT EXISTS.
-- =====================================================================

CREATE TABLE IF NOT EXISTS mart.kpi_cakupan_bulanan (
    waktu_key                     integer  NOT NULL,
    tahun                         smallint NOT NULL,
    bulan                         smallint NOT NULL,
    nama_kecamatan                text     NOT NULL,
    kelompok_key                  smallint NOT NULL,
    nama_kelompok                 text     NOT NULL,
    n_posyandu                    integer  NOT NULL,   -- posyandu yang punya baris pada bulan tsb.
    n_posyandu_terlapor           integer  NOT NULL,   -- status Terlapor
    cakupan                       bigint,              -- Σ cakupan, filter KPI (K38)
    sasaran                       bigint,              -- Σ sasaran, filter KPI (K38)
    persen_cakupan                numeric(7, 2),       -- Σ cakupan ÷ Σ sasaran × 100
    baris_anomali                 integer  NOT NULL,   -- baris Terlapor dengan flag_anomali > 0
    baris_cakupan_tanpa_sasaran   integer  NOT NULL,   -- status 5 (K37)
    batch_id                      text     NOT NULL,
    PRIMARY KEY (waktu_key, nama_kecamatan, kelompok_key)
);
COMMENT ON TABLE mart.kpi_cakupan_bulanan IS
    'KPI cakupan: filter Terlapor, bukan A1, cakupan tidak NULL, sasaran > 0, termasuk cakupan turunan T07 (K38)';

CREATE TABLE IF NOT EXISTS mart.kelengkapan_bulanan (
    waktu_key            integer  NOT NULL,
    tahun                smallint NOT NULL,
    bulan                smallint NOT NULL,
    nama_kecamatan       text     NOT NULL,
    n_posyandu           integer  NOT NULL,   -- posyandu di sheet tahun tsb.
    n_posyandu_terlapor  integer  NOT NULL,   -- >= 1 kelompok berstatus Terlapor pada bulan tsb.
    batch_id             text     NOT NULL,
    PRIMARY KEY (waktu_key, nama_kecamatan)
);
COMMENT ON TABLE mart.kelengkapan_bulanan IS 'Kelengkapan laporan: posyandu terlapor (kelompok mana pun) per bulan';

CREATE TABLE IF NOT EXISTS mart.posyandu_profil (
    posyandu_key                 integer  PRIMARY KEY,  -- versi aktif di dw.dim_posyandu
    posyandu_id                  text     NOT NULL UNIQUE,
    nama_posyandu                text     NOT NULL,
    nama_kelurahan               text     NOT NULL,
    nama_kecamatan               text     NOT NULL,
    puskesmas                    text     NOT NULL,
    tahun_pertama_lapor          smallint,
    -- fitur kunjungan, periode 2025 (tahun lengkap terakhir)
    bulan_terlapor_2025          smallint NOT NULL,     -- bulan dengan >= 1 kelompok Terlapor (0–12)
    kelengkapan_2025             numeric(5, 4) NOT NULL,-- bulan_terlapor ÷ 12
    sasaran_ibu_hamil_2025       integer,
    cakupan_ibu_hamil_2025       integer,
    persen_ibu_hamil_2025        numeric(7, 2),         -- filter KPI (K38); NULL bila tanpa sasaran
    sasaran_bayi_balita_2025     integer,
    cakupan_bayi_balita_2025     integer,
    persen_bayi_balita_2025      numeric(7, 2),
    baris_terlapor_2025          integer  NOT NULL,
    baris_anomali_2025           integer  NOT NULL,     -- baris Terlapor dengan flag_anomali > 0
    persen_anomali_2025          numeric(7, 2),
    -- alat (snapshot Juni 2026)
    jenis_alat_std_baik          smallint,              -- 0–5 jenis alat yang punya >= 1 unit standar baik
    rasio_alat_baik              numeric(5, 4),         -- jenis_alat_std_baik ÷ 5
    unit_alat_total              integer,
    unit_alat_rusak              integer,
    ada_alat_rusak_tdk_standar   boolean,               -- >= 1 unit rusak atau tidak standar
    -- kader (snapshot Tahun 2026), dihitung per baris kader (K22)
    jumlah_kader                 integer  NOT NULL,
    kader_terlatih               integer  NOT NULL,     -- pelatihan 25 keterampilan = Ya
    persen_kader_terlatih        numeric(7, 2),
    -- eksternal (kelurahan)
    stunting_kelurahan_2025      integer,
    rasio_stunting_kelurahan_2025 numeric(7, 2),        -- rasio, bukan prevalensi (T19)
    is_layak_ml                  boolean  NOT NULL,     -- punya data 2025 (posyandu baru 2026 tidak)
    batch_id                     text     NOT NULL
);
COMMENT ON TABLE mart.posyandu_profil IS
    'Profil per posyandu (fitur ML & dashboard). Periode fitur kunjungan 2025; alat Juni 2026; kader Tahun 2026';

CREATE TABLE IF NOT EXISTS mart.kpi_kelurahan (
    kelurahan_key              integer  NOT NULL,
    nama_kelurahan             text     NOT NULL,
    nama_kecamatan             text     NOT NULL,
    tahun                      smallint NOT NULL,
    jumlah_stunting            integer,
    penduduk_0_4               integer,
    rasio_stunting_persen      numeric(7, 2),
    is_pendekatan              boolean,                 -- penyebut bukan dari tahun yang sama
    n_posyandu                 integer,                 -- posyandu yang melapor pada tahun tsb.
    sasaran_bayi_balita_des    integer,                 -- Σ sasaran bayi balita bulan Desember
    persen_bayi_balita         numeric(7, 2),           -- KPI cakupan bayi balita setahun (K38)
    batch_id                   text     NOT NULL,
    PRIMARY KEY (kelurahan_key, tahun)
);

CREATE TABLE IF NOT EXISTS mart.ringkasan_kualitas (
    tahun     smallint NOT NULL,
    kategori  text     NOT NULL,                        -- 'status' | 'anomali'
    item      text     NOT NULL,
    n_baris   integer  NOT NULL,
    batch_id  text     NOT NULL,
    PRIMARY KEY (tahun, kategori, item)
);

CREATE TABLE IF NOT EXISTS mart.quality_gate_terakhir (
    kode_cek    text    PRIMARY KEY,
    fase        text    NOT NULL,
    is_kritis   boolean NOT NULL,
    lulus       boolean NOT NULL,
    nilai       text,
    harapan     text,
    batch_id    text    NOT NULL,
    dicatat_pada timestamptz
);

CREATE TABLE IF NOT EXISTS mart.ml_evaluasi (
    model_id         text     PRIMARY KEY,              -- mis. kmeans_internal_k4
    algoritma        text     NOT NULL,                 -- aturan | kmeans | agglomerative
    set_fitur        text     NOT NULL,                 -- internal | internal+eksternal
    k                smallint NOT NULL,
    silhouette       numeric(8, 4),
    davies_bouldin   numeric(8, 4),
    calinski_harabasz numeric(12, 2),
    inertia          numeric(14, 4),
    stabilitas_ari   numeric(8, 4),                     -- rata-rata ARI antar 10 seed (K-Means)
    ukuran_cluster   text,                              -- jumlah anggota per cluster
    is_terpilih      boolean  NOT NULL DEFAULT false,
    catatan          text,
    batch_id         text     NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.hasil_cluster (
    posyandu_key     integer  PRIMARY KEY,
    posyandu_id      text     NOT NULL,
    model_id         text     NOT NULL,
    cluster          smallint NOT NULL,
    label_cluster    text     NOT NULL,
    kelompok_aturan  text,                              -- hasil baseline berbasis aturan
    jarak_centroid   numeric(10, 4),
    batch_id         text     NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.cluster_profil (
    model_id     text     NOT NULL,
    cluster      smallint NOT NULL,
    label_cluster text    NOT NULL,
    deskripsi    text     NOT NULL,
    n_posyandu   integer  NOT NULL,
    fitur        text     NOT NULL,
    rata_rata    numeric(12, 4),
    rata_rata_kota numeric(12, 4),
    z_score      numeric(8, 4),
    batch_id     text     NOT NULL,
    PRIMARY KEY (model_id, cluster, fitur)
);

CREATE TABLE IF NOT EXISTS mart.dashboard_refresh (
    refresh_id   bigserial   PRIMARY KEY,
    batch_id     text        NOT NULL,
    dibuat_pada  timestamptz NOT NULL DEFAULT now(),
    keterangan   text
);

-- ---------------------------------------------------------------------
-- Tambahan 16 Nov 2026 (tindak lanjut ML & dashboard). ALTER ... IF NOT EXISTS agar idempotent
-- pada database yang sudah berisi.
-- ---------------------------------------------------------------------
-- Interpretasi cluster bagi dinas (aturan penamaan di ml/clustering.py) dan daftar prioritas per posyandu
ALTER TABLE mart.cluster_profil ADD COLUMN IF NOT EXISTS tingkat_prioritas text;   -- Tinggi | Sedang | Rendah
ALTER TABLE mart.cluster_profil ADD COLUMN IF NOT EXISTS karakter          text;   -- satu kalimat, skala asli
ALTER TABLE mart.cluster_profil ADD COLUMN IF NOT EXISTS rekomendasi       text;
ALTER TABLE mart.hasil_cluster  ADD COLUMN IF NOT EXISTS tingkat_prioritas   text;
ALTER TABLE mart.hasil_cluster  ADD COLUMN IF NOT EXISTS skor_kebutuhan      numeric(8, 4);  -- makin besar makin perlu
ALTER TABLE mart.hasil_cluster  ADD COLUMN IF NOT EXISTS peringkat_prioritas integer;        -- 1 = paling prioritas
ALTER TABLE mart.hasil_cluster  ADD COLUMN IF NOT EXISTS alasan_prioritas    text;           -- 2 indikator terburuk

-- Grain posyandu (versi SCD 2) × bulan × kelompok: sumber semua visual dashboard yang harus bisa
-- difilter tahun, kecamatan, puskesmas, dan cluster. Hanya kolom agregatif/kode; tanpa data kader.
--   cakupan_kpi / sasaran_kpi berisi nilai HANYA bila baris lolos filter KPI K38, selain itu NULL,
--   sehingga Σ cakupan_kpi ÷ Σ sasaran_kpi = KPI cakupan resmi untuk irisan filter apa pun.
CREATE TABLE IF NOT EXISTS mart.kunjungan_posyandu_bulanan (
    posyandu_key     integer  NOT NULL,   -- versi posyandu saat itu (puskesmas bisa berbeda antar versi)
    posyandu_id      text     NOT NULL,   -- kunci tetap: penghubung ke hasil_cluster
    nama_kecamatan   text     NOT NULL,
    puskesmas        text     NOT NULL,
    waktu_key        integer  NOT NULL,
    tahun            smallint NOT NULL,
    bulan            smallint NOT NULL,
    kelompok_key     smallint NOT NULL,
    nama_kelompok    text     NOT NULL,
    status_key       smallint NOT NULL,
    nama_status      text     NOT NULL,
    cakupan_kpi      integer,
    sasaran_kpi      integer,
    flag_anomali     integer  NOT NULL,   -- bitmask 4.5 (1 A1 ... 64 selisih persen)
    is_cakupan_turunan boolean NOT NULL,
    batch_id         text     NOT NULL,
    PRIMARY KEY (posyandu_key, waktu_key, kelompok_key)
);
COMMENT ON TABLE mart.kunjungan_posyandu_bulanan IS
    'Fakta kunjungan siap filter dashboard; cakupan_kpi/sasaran_kpi hanya terisi bila lolos filter KPI K38';
