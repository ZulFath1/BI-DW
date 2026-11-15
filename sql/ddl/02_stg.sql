-- =====================================================================
-- 02_stg.sql — schema stg: data sumber dengan NILAI ASLI (teks apa adanya)
-- dan nomor baris sumber, untuk lineage dan rekonsiliasi.
--
-- Prinsip:
--   * nilai disimpan sebagai teks persis seperti di sumber; konversi tipe dilakukan di
--     tahap transform (T03) agar kegagalan konversi tercatat, bukan hilang diam-diam;
--   * setiap baris membawa batch_id, manifest_id (objek data lake asal), dan baris_sumber;
--   * staging dimuat ulang penuh setiap batch (DELETE + INSERT dalam satu transaksi);
--     riwayat berkas tetap ada di data lake (raw zone);
--   * PRIVASI: nama kader hanya disimpan sebagai kader_kode (pseudonim HMAC);
--     alamat kader dan alamat posyandu TIDAK dimuat.
-- =====================================================================

-- Laporan kunjungan bulanan (sheet 2023–2026). Satu baris = satu posyandu × bulan × kelompok
-- (5 metrik), hasil pemetaan posisi kolom yang sudah divalidasi terhadap header 3 tingkat.
CREATE TABLE IF NOT EXISTS stg.kunjungan_bulanan (
    batch_id              text     NOT NULL,
    manifest_id           bigint   NOT NULL REFERENCES dq.ingest_manifest (manifest_id),
    tahun_sheet           smallint NOT NULL,
    baris_sumber          integer  NOT NULL,      -- nomor baris Excel (1-based)
    kolom_sumber_awal     integer  NOT NULL,      -- nomor kolom Excel metrik Sasaran (1-based)
    no_asli               text,
    kecamatan_asli        text,
    kelurahan_asli        text,
    puskesmas_asli        text,
    posyandu_asli         text,
    bulan_ke              smallint NOT NULL CHECK (bulan_ke BETWEEN 1 AND 12),
    kelompok_ke           smallint NOT NULL CHECK (kelompok_ke BETWEEN 1 AND 5),
    sasaran_asli          text,
    cakupan_asli          text,
    persen_dilayani_asli  text,
    dirujuk_asli          text,
    persen_dirujuk_asli   text,
    tipe_sel_asli         char(5)  NOT NULL,      -- tipe sel per metrik: n=angka, s=teks, d=tanggal, _=kosong
    PRIMARY KEY (tahun_sheet, baris_sumber, bulan_ke, kelompok_ke)
);

-- Antropometri (sheet Antrop, snapshot). Baris total (tanpa label) ikut disimpan dengan
-- is_baris_total = true untuk rekonsiliasi (T14), tetapi tidak diteruskan ke curated/DWH.
CREATE TABLE IF NOT EXISTS stg.antrop (
    batch_id                     text     NOT NULL,
    manifest_id                  bigint   NOT NULL REFERENCES dq.ingest_manifest (manifest_id),
    baris_sumber                 integer  PRIMARY KEY,
    cut_off_asli                 text,             -- isi sel "CUT OFF" di kepala sheet
    no_asli                      text,
    kecamatan_asli               text,
    puskesmas_asli               text,
    kelurahan_asli               text,
    no_kelurahan_asli            text,
    nama_posyandu_asli           text,
    timbangan_bayi_std_baik      text, timbangan_bayi_std_rusak    text,
    timbangan_bayi_tstd_baik     text, timbangan_bayi_tstd_rusak   text,
    panjang_bayi_std_baik        text, panjang_bayi_std_rusak      text,
    panjang_bayi_tstd_baik       text, panjang_bayi_tstd_rusak     text,
    timbangan_badan_std_baik     text, timbangan_badan_std_rusak   text,
    timbangan_badan_tstd_baik    text, timbangan_badan_tstd_rusak  text,
    tinggi_badan_std_baik        text, tinggi_badan_std_rusak      text,
    tinggi_badan_tstd_baik       text, tinggi_badan_tstd_rusak     text,
    lila_std_baik                text, lila_std_rusak              text,
    lila_tstd_baik               text, lila_tstd_rusak             text,
    tensimeter_ada_asli          text,
    tensimeter_tidak_ada_asli    text,
    is_baris_total               boolean  NOT NULL DEFAULT false
);

-- Kader (sheet Kader). Nama -> kader_kode (pseudonim). Alamat kader & alamat posyandu tidak dimuat.
CREATE TABLE IF NOT EXISTS stg.kader (
    batch_id               text     NOT NULL,
    manifest_id            bigint   NOT NULL REFERENCES dq.ingest_manifest (manifest_id),
    baris_sumber           integer  PRIMARY KEY,
    no_asli                text,
    kecamatan_asli         text,
    puskesmas_asli         text,
    nama_posyandu_asli     text,
    kelurahan_asli         text,
    kader_kode             text,                   -- 'KDR-' + 16 heks HMAC-SHA256; NULL bila nama kosong
    usia_asli              text,
    jenis_kelamin_asli     text,
    pendidikan_asli        text,
    kepemilikan_jkn_asli   text,
    tingkatan_kader_asli   text,
    pelatihan_25_asli      text,
    penilaian_25_asli      text
);

-- E1: penduduk menurut kelompok umur (buku Disdukcapil). Tabel 6 (kota), Tabel 7 (kecamatan),
-- Tabel 8–12 (kelurahan per kecamatan). Angka masih format buku ('1.273', '51,01').
CREATE TABLE IF NOT EXISTS stg.penduduk_kelompok_umur (
    batch_id               text     NOT NULL,
    manifest_id            bigint   NOT NULL REFERENCES dq.ingest_manifest (manifest_id),
    halaman                smallint NOT NULL,      -- nomor halaman PDF (1-based)
    tabel_halaman          smallint NOT NULL,      -- urutan tabel di halaman (0-based, pdfplumber)
    baris_tabel            smallint NOT NULL,      -- urutan baris di tabel (0-based)
    nomor_tabel_buku       smallint NOT NULL,      -- 6, 7, 8–12
    tingkat                text     NOT NULL CHECK (tingkat IN ('kota', 'kecamatan', 'kelurahan')),
    kode_wilayah_asli      text,
    kecamatan_asli         text,
    wilayah_asli           text,                   -- nama kelurahan/kecamatan/kota
    jenis_baris            text     NOT NULL CHECK (jenis_baris IN ('data', 'total_wilayah', 'total_kecamatan', 'total_kota')),
    kelompok_umur_asli     text,
    laki_jiwa_asli         text,
    laki_persen_asli       text,
    perempuan_jiwa_asli    text,
    perempuan_persen_asli  text,
    jumlah_asli            text,
    jumlah_persen_asli     text,
    urutan_dalam_blok      smallint,               -- urutan baris data dalam blok wilayah (1–16)
    PRIMARY KEY (halaman, tabel_halaman, baris_tabel)
);
-- Kolom tambahan untuk database yang sudah dibuat sebelum kolom ini ada (idempotent).
ALTER TABLE stg.penduduk_kelompok_umur ADD COLUMN IF NOT EXISTS urutan_dalam_blok smallint;

-- E2: jumlah stunting/wasting/underweight per kelurahan (halaman portal Satu Data).
CREATE TABLE IF NOT EXISTS stg.stunting_kelurahan (
    batch_id               text     NOT NULL,
    manifest_id            bigint   NOT NULL REFERENCES dq.ingest_manifest (manifest_id),
    sumber                 text     NOT NULL,
    tahun_data             smallint NOT NULL,
    baris_tabel            smallint NOT NULL,      -- urutan baris di tabel HTML (1-based; total = 0)
    jenis_baris            text     NOT NULL CHECK (jenis_baris IN ('data', 'total')),
    judul_asli             text,
    no_asli                text,
    kode_kecamatan_asli    text,
    kecamatan_asli         text,
    kode_kelurahan_asli    text,
    kelurahan_asli         text,
    stunting_asli          text,
    wasting_asli           text,
    underweight_asli       text,
    catatan_asli           text,
    PRIMARY KEY (tahun_data, baris_tabel)
);
