"""Task load_dw: curated -> dw (galaxy schema) dalam SATU transaksi.

Urutan (dokumen UTS 4.3: dimensi dulu, lalu fakta):
  1. dim_kelurahan  upsert berdasarkan kode_wilayah (surrogate key tetap stabil antar-batch)
  2. fakta lama dihapus (DELETE, masih di dalam transaksi yang sama)
  3. dim_posyandu   upsert berdasarkan (posyandu_id, versi) -> posyandu_key stabil; versi yang
                    tidak ada lagi di curated dihapus
  4. lima fakta disisipkan dari curated dengan lookup surrogate key:
       kunjungan  -> versi posyandu yang berlaku pada bulan tsb. (SCD tipe 2)
       alat       -> versi yang berlaku pada bulan snapshot (Juni 2026)
       kader      -> versi aktif (snapshot "Tahun 2026", bulan tidak diketahui, K21)
       penduduk/stunting -> dim_kelurahan
  5. pemeriksaan kritis Q04 (rekonsiliasi total), Q05 (kunci asing / baris hilang saat lookup),
     Q06 (tidak ada kolom nama/alamat kader di dw & mart)
  6. semua lulus -> COMMIT; satu saja gagal -> ROLLBACK (DWH persis seperti sebelum batch) dan
     task GAGAL. Hasil Q04–Q06 tetap tersimpan di dq.quality_result (koneksi terpisah).

Kenapa fakta dimuat ulang (DELETE + INSERT) dan bukan upsert baris demi baris: seluruh isi fakta
selalu dibangun ulang dari curated yang lengkap, sehingga hasilnya sama dengan upsert, lebih
sederhana, dan tetap atomik karena berada dalam satu transaksi (lihat KEPUTUSAN K27).
"""

from __future__ import annotations

import logging

from common.batch import catat_task
from common.db import buka_koneksi
from quality.quality_gate import HasilCek, simpan_hasil

log = logging.getLogger(__name__)


class QualityGateKritisError(RuntimeError):
    """Pemeriksaan kritis gagal di dalam transaksi load; perubahan DWH dibatalkan."""


SQL_DIMENSI = [
    # 1. dim_kelurahan
    """INSERT INTO dw.dim_kelurahan (kode_wilayah, nama_kelurahan, kode_kecamatan, nama_kecamatan)
       SELECT kode_wilayah, nama_kelurahan, kode_kecamatan, nama_kecamatan FROM curated.kelurahan
       ON CONFLICT (kode_wilayah) DO UPDATE SET nama_kelurahan = EXCLUDED.nama_kelurahan,
           kode_kecamatan = EXCLUDED.kode_kecamatan, nama_kecamatan = EXCLUDED.nama_kecamatan""",
    # 2. fakta lama dihapus
    "DELETE FROM dw.fact_kunjungan_bulanan",
    "DELETE FROM dw.fact_alat_snapshot",
    "DELETE FROM dw.fact_kader_snapshot",
    "DELETE FROM dw.fact_penduduk_kelurahan",
    "DELETE FROM dw.fact_stunting_kelurahan",
    # 3. dim_posyandu: matikan is_current dulu agar indeks unik versi aktif tidak bentrok saat upsert
    "UPDATE dw.dim_posyandu SET is_current = false WHERE is_current",
    """INSERT INTO dw.dim_posyandu (posyandu_id, versi, nama_posyandu, nama_posyandu_normal, kelurahan_key, puskesmas,
                                   berlaku_mulai, berlaku_sampai, is_current, tahun_pertama_lapor,
                                   tahun_terakhir_lapor, muncul_kembali)
       SELECT v.posyandu_id, v.versi, v.nama_posyandu, v.nama_posyandu_normal, k.kelurahan_key, v.puskesmas,
              v.berlaku_mulai, v.berlaku_sampai, v.is_current, v.tahun_pertama_lapor, v.tahun_terakhir_lapor,
              v.muncul_kembali
       FROM curated.posyandu_versi v JOIN dw.dim_kelurahan k ON k.nama_kelurahan = v.nama_kelurahan
       ON CONFLICT (posyandu_id, versi) DO UPDATE SET nama_posyandu = EXCLUDED.nama_posyandu,
           nama_posyandu_normal = EXCLUDED.nama_posyandu_normal, kelurahan_key = EXCLUDED.kelurahan_key,
           puskesmas = EXCLUDED.puskesmas, berlaku_mulai = EXCLUDED.berlaku_mulai,
           berlaku_sampai = EXCLUDED.berlaku_sampai, is_current = EXCLUDED.is_current,
           tahun_pertama_lapor = EXCLUDED.tahun_pertama_lapor, tahun_terakhir_lapor = EXCLUDED.tahun_terakhir_lapor,
           muncul_kembali = EXCLUDED.muncul_kembali""",
    """DELETE FROM dw.dim_posyandu d WHERE NOT EXISTS
         (SELECT 1 FROM curated.posyandu_versi v WHERE v.posyandu_id = d.posyandu_id AND v.versi = d.versi)""",
    """DELETE FROM dw.dim_kelurahan d WHERE NOT EXISTS (SELECT 1 FROM curated.kelurahan c WHERE c.kode_wilayah = d.kode_wilayah)
         AND NOT EXISTS (SELECT 1 FROM dw.dim_posyandu p WHERE p.kelurahan_key = d.kelurahan_key)""",
]

SQL_FAKTA = {
    "fact_kunjungan_bulanan": """
        INSERT INTO dw.fact_kunjungan_bulanan (posyandu_key, waktu_key, kelompok_key, status_key, sasaran, cakupan,
            persen_dilayani, persen_dilayani_laporan, dirujuk, persen_dirujuk, is_cakupan_turunan, flag_anomali,
            rujukan_reliabilitas, tahun_sheet, baris_sumber, batch_id)
        SELECT p.posyandu_key, c.waktu_key, c.kelompok_key, c.status_key, c.sasaran, c.cakupan, c.persen_dilayani,
               c.persen_dilayani_laporan, c.dirujuk, c.persen_dirujuk, c.is_cakupan_turunan, c.flag_anomali, 'rendah',
               c.tahun_sheet, c.baris_sumber, %(batch_id)s
        FROM curated.kunjungan_bulanan c
        JOIN dw.dim_waktu w ON w.waktu_key = c.waktu_key
        JOIN dw.dim_posyandu p ON p.posyandu_id = c.posyandu_id
             AND w.tanggal_awal BETWEEN p.berlaku_mulai AND p.berlaku_sampai""",
    "fact_alat_snapshot": """
        INSERT INTO dw.fact_alat_snapshot (posyandu_key, alat_key, waktu_key, jumlah_baik, jumlah_rusak, flag_kualitas,
                                           baris_sumber, batch_id)
        SELECT p.posyandu_key, c.alat_key, c.waktu_key, c.jumlah_baik, c.jumlah_rusak, c.flag_kualitas,
               c.baris_sumber, %(batch_id)s
        FROM curated.alat_snapshot c
        JOIN dw.dim_waktu w ON w.waktu_key = c.waktu_key
        JOIN dw.dim_posyandu p ON p.posyandu_id = c.posyandu_id
             AND w.tanggal_awal BETWEEN p.berlaku_mulai AND p.berlaku_sampai""",
    "fact_kader_snapshot": """
        INSERT INTO dw.fact_kader_snapshot (kader_kode, posyandu_key, waktu_key, periode_snapshot, kelompok_umur_kader,
            jenis_kelamin, pendidikan, kepemilikan_jkn, tingkatan_kader, pelatihan_25, penilaian_25,
            is_pasangan_ganda, baris_sumber, batch_id)
        SELECT c.kader_kode, p.posyandu_key, c.waktu_key, c.periode_snapshot, c.kelompok_umur_kader, c.jenis_kelamin,
               c.pendidikan, c.kepemilikan_jkn, c.tingkatan_kader, c.pelatihan_25, c.penilaian_25,
               c.is_pasangan_ganda, c.baris_sumber, %(batch_id)s
        FROM curated.kader_snapshot c
        JOIN dw.dim_posyandu p ON p.posyandu_id = c.posyandu_id AND p.is_current""",
    "fact_penduduk_kelurahan": """
        INSERT INTO dw.fact_penduduk_kelurahan (kelurahan_key, waktu_key, kelompok_umur_key, jenis_kelamin,
            jumlah_penduduk, is_umur_dari_urutan, flag_kualitas, sumber, batch_id)
        SELECT k.kelurahan_key, c.waktu_key, c.kelompok_umur_key, c.jenis_kelamin, c.jumlah_penduduk,
               c.is_umur_dari_urutan, c.flag_kualitas, c.sumber, %(batch_id)s
        FROM curated.penduduk_kelurahan c JOIN dw.dim_kelurahan k ON k.nama_kelurahan = c.nama_kelurahan""",
    "fact_stunting_kelurahan": """
        INSERT INTO dw.fact_stunting_kelurahan (kelurahan_key, waktu_key, stunting, wasting, underweight, sumber_data,
            tanggal_data_stunting, tanggal_data_gizi, flag_kualitas, batch_id)
        SELECT k.kelurahan_key, c.waktu_key, c.stunting, c.wasting, c.underweight, c.sumber_data,
               c.tanggal_data_stunting, c.tanggal_data_gizi, c.flag_kualitas, %(batch_id)s
        FROM curated.stunting_kelurahan c JOIN dw.dim_kelurahan k ON k.kode_wilayah = c.kode_wilayah""",
}
CURATED_ASAL = {"fact_kunjungan_bulanan": "kunjungan_bulanan", "fact_alat_snapshot": "alat_snapshot",
                "fact_kader_snapshot": "kader_snapshot", "fact_penduduk_kelurahan": "penduduk_kelurahan",
                "fact_stunting_kelurahan": "stunting_kelurahan"}


# ---------------------------------------------------------------- pemeriksaan kritis (dalam transaksi)
def cek_q04(cur) -> HasilCek:
    """Σ Sasaran dan Σ Cakupan staging (nilai asli) = fakta, per tahun. Cakupan turunan (T07)
    bernilai 0 sehingga tidak mengubah jumlah; jumlahnya dilaporkan terpisah."""
    cur.execute(r"""
        WITH s AS (SELECT tahun_sheet,
                          sum(CASE WHEN sasaran_asli ~ '^\s*-?\d+(\.\d+)?\s*$' THEN round(sasaran_asli::numeric) END) AS sas,
                          sum(CASE WHEN cakupan_asli ~ '^\s*-?\d+(\.\d+)?\s*$' THEN round(cakupan_asli::numeric) END) AS cak
                   FROM stg.kunjungan_bulanan GROUP BY 1),
             f AS (SELECT tahun_sheet, sum(sasaran) AS sas, sum(cakupan) AS cak,
                          count(*) FILTER (WHERE is_cakupan_turunan) AS turunan
                   FROM dw.fact_kunjungan_bulanan GROUP BY 1)
        SELECT s.tahun_sheet, s.sas, f.sas, s.cak, f.cak, f.turunan FROM s LEFT JOIN f USING (tahun_sheet) ORDER BY 1""")
    rows = cur.fetchall()
    lulus = bool(rows) and all(ss == fs and sc == fc for _, ss, fs, sc, fc, _ in rows)
    return HasilCek("Q04", "dalam_load", True, lulus,
                    "; ".join(f"{t}: sasaran {fs}/{ss}, cakupan {fc}/{sc} (turunan {tu} sel)"
                              for t, ss, fs, sc, fc, tu in rows),
                    "fakta = staging (selisih 0) per tahun", "format: fakta/staging")


def cek_q05(cur) -> HasilCek:
    """Kunci asing tanpa pasangan + baris curated yang hilang saat lookup surrogate key."""
    hilang = {}
    for fakta, asal in CURATED_ASAL.items():
        cur.execute(f"SELECT (SELECT count(*) FROM curated.{asal}) - (SELECT count(*) FROM dw.{fakta})")
        hilang[fakta] = cur.fetchone()[0]
    cur.execute("""
        SELECT (SELECT count(*) FROM dw.fact_kunjungan_bulanan f WHERE NOT EXISTS
                    (SELECT 1 FROM dw.dim_posyandu d WHERE d.posyandu_key = f.posyandu_key))
             + (SELECT count(*) FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_waktu w USING (waktu_key)
                JOIN dw.dim_posyandu d USING (posyandu_key)
                WHERE w.tanggal_awal NOT BETWEEN d.berlaku_mulai AND d.berlaku_sampai)
             + (SELECT count(*) FROM dw.dim_posyandu d WHERE NOT EXISTS
                    (SELECT 1 FROM dw.dim_kelurahan k WHERE k.kelurahan_key = d.kelurahan_key))""")
    yatim = cur.fetchone()[0]
    lulus = yatim == 0 and all(v == 0 for v in hilang.values())
    return HasilCek("Q05", "dalam_load", True, lulus,
                    f"FK tanpa pasangan / versi SCD tidak berlaku {yatim}; baris hilang saat lookup {hilang}",
                    "semua 0")


def cek_q06(cur) -> HasilCek:
    """Tidak boleh ada kolom nama/alamat/usia kader di schema dw dan mart."""
    cur.execute(r"""
        SELECT table_schema || '.' || table_name || '.' || column_name FROM information_schema.columns
        WHERE table_schema IN ('dw', 'mart')
          AND (column_name ~* 'alamat'
               OR (table_name ~* 'kader' AND column_name ~* '(^|_)(nama|usia|umur_asli|nik)($|_)')
               OR column_name ~* 'nama_kader')""")
    kolom = [r[0] for r in cur.fetchall()]
    return HasilCek("Q06", "dalam_load", True, not kolom, f"kolom terlarang: {kolom or 'tidak ada'}", "tidak ada")


def load_dw(batch_id: str, percobaan_ke: int | None = None) -> dict:
    with catat_task(batch_id, "load_dw", "load", percobaan_ke) as hasil_task:
        conn = buka_koneksi()
        hasil_cek: list[HasilCek] = []
        try:
            with conn.cursor() as cur:
                for sql in SQL_DIMENSI:
                    cur.execute(sql)
                n = {}
                for fakta, sql in SQL_FAKTA.items():
                    cur.execute(sql, {"batch_id": batch_id})
                    n[fakta] = cur.rowcount
                cur.execute("SELECT count(*) FROM dw.dim_posyandu")
                n["dim_posyandu"] = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM dw.dim_kelurahan")
                n["dim_kelurahan"] = cur.fetchone()[0]
                hasil_cek = [cek_q04(cur), cek_q05(cur), cek_q06(cur)]
            gagal = [h.kode for h in hasil_cek if not h.lulus]
            if gagal:
                conn.rollback()
                raise QualityGateKritisError(f"pemeriksaan kritis gagal {gagal}; load dibatalkan (ROLLBACK), DWH tidak berubah")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
            if hasil_cek:
                simpan_hasil(batch_id, hasil_cek)
        hasil_task.jumlah_baris = sum(v for k, v in n.items() if k.startswith("fact"))
        hasil_task.pesan = "COMMIT; Q04/Q05/Q06 lulus; " + ", ".join(f"{k}={v}" for k, v in n.items())
        return n
