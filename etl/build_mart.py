"""Task build_mart: dw -> mart (tabel siap pakai untuk dashboard dan machine learning).

Semua tabel mart (kecuali hasil ML dan penanda refresh) dikosongkan lalu diisi ulang dari dw
dalam SATU transaksi: bila satu query gagal, mart lama tetap utuh (dashboard tetap bisa dipakai).

Aturan KPI cakupan (K38, sama dengan prototipe UTS):
    Σ cakupan ÷ Σ sasaran × 100, hanya baris
      status Terlapor  AND  bukan A1 (Sasaran >= 10.000)  AND  cakupan tidak NULL  AND  sasaran > 0,
    cakupan turunan T07 (= 0) ikut dihitung. Baris Sasaran 0 atau negatif tidak dipakai karena
    persen tidak terdefinisi/tidak bermakna untuk penyebut <= 0.

Mart hanya berisi agregat dan atribut posyandu; tidak ada kolom nama/alamat/usia kader (Q06).
"""

from __future__ import annotations

import logging

from common.batch import catat_task
from common.db import buka_koneksi

log = logging.getLogger(__name__)

# Filter KPI cakupan (K38) dipakai ulang di beberapa query; alias tabel fakta = f.
KPI = "(f.status_key = 1 AND f.flag_anomali & 1 = 0 AND f.cakupan IS NOT NULL AND f.sasaran > 0)"
TAHUN_FITUR = 2025  # tahun lengkap terakhir; dipakai untuk fitur posyandu & ML

SQL_MART = {
    "kpi_cakupan_bulanan": f"""
        INSERT INTO mart.kpi_cakupan_bulanan
        SELECT f.waktu_key, w.tahun, w.bulan, kl.nama_kecamatan, k.kelompok_key, k.nama_kelompok,
               count(DISTINCT p.posyandu_id),
               count(DISTINCT p.posyandu_id) FILTER (WHERE f.status_key = 1),
               sum(f.cakupan) FILTER (WHERE {KPI}),
               sum(f.sasaran) FILTER (WHERE {KPI}),
               round(100.0 * sum(f.cakupan) FILTER (WHERE {KPI}) / nullif(sum(f.sasaran) FILTER (WHERE {KPI}), 0), 2),
               count(*) FILTER (WHERE f.status_key = 1 AND f.flag_anomali > 0),
               count(*) FILTER (WHERE f.status_key = 5),
               %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f
        JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
        JOIN dw.dim_kelurahan kl ON kl.kelurahan_key = p.kelurahan_key
        JOIN dw.dim_kelompok_sasaran k ON k.kelompok_key = f.kelompok_key
        GROUP BY f.waktu_key, w.tahun, w.bulan, kl.nama_kecamatan, k.kelompok_key, k.nama_kelompok""",

    "kelengkapan_bulanan": """
        INSERT INTO mart.kelengkapan_bulanan
        SELECT f.waktu_key, w.tahun, w.bulan, kl.nama_kecamatan, count(DISTINCT p.posyandu_id),
               count(DISTINCT p.posyandu_id) FILTER (WHERE f.status_key = 1), %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f
        JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
        JOIN dw.dim_kelurahan kl ON kl.kelurahan_key = p.kelurahan_key
        GROUP BY f.waktu_key, w.tahun, w.bulan, kl.nama_kecamatan""",

    "posyandu_profil": f"""
        INSERT INTO mart.posyandu_profil
        WITH cur AS (
            SELECT p.posyandu_key, p.posyandu_id, p.nama_posyandu, kl.nama_kelurahan, kl.nama_kecamatan,
                   kl.kelurahan_key, p.puskesmas, p.tahun_pertama_lapor
            FROM dw.dim_posyandu p JOIN dw.dim_kelurahan kl ON kl.kelurahan_key = p.kelurahan_key
            WHERE p.is_current
        ), fit AS (
            SELECT p.posyandu_id,
                   count(DISTINCT w.bulan) FILTER (WHERE f.status_key = 1)                        AS bulan_terlapor,
                   sum(f.sasaran) FILTER (WHERE f.kelompok_key = 1 AND {KPI})                      AS sas_ih,
                   sum(f.cakupan) FILTER (WHERE f.kelompok_key = 1 AND {KPI})                      AS cak_ih,
                   sum(f.sasaran) FILTER (WHERE f.kelompok_key = 2 AND {KPI})                      AS sas_bb,
                   sum(f.cakupan) FILTER (WHERE f.kelompok_key = 2 AND {KPI})                      AS cak_bb,
                   count(*) FILTER (WHERE f.status_key = 1)                                        AS baris_terlapor,
                   count(*) FILTER (WHERE f.status_key = 1 AND f.flag_anomali > 0)                 AS baris_anomali
            FROM dw.fact_kunjungan_bulanan f
            JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
            JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
            WHERE w.tahun = {TAHUN_FITUR}
            GROUP BY p.posyandu_id
        ), alat AS (
            SELECT p.posyandu_id,
                   count(DISTINCT a.nama_alat) FILTER (WHERE a.kategori_standar = 'Standar' AND f.jumlah_baik > 0) AS jenis_std_baik,
                   sum(coalesce(f.jumlah_baik, 0) + coalesce(f.jumlah_rusak, 0))                    AS unit_total,
                   sum(coalesce(f.jumlah_rusak, 0))                                                 AS unit_rusak,
                   bool_or(coalesce(f.jumlah_rusak, 0) > 0
                           OR (a.kategori_standar = 'Tidak standar' AND coalesce(f.jumlah_baik, 0) > 0)) AS ada_rusak_tstd
            FROM dw.fact_alat_snapshot f
            JOIN dw.dim_alat a ON a.alat_key = f.alat_key
            JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
            GROUP BY p.posyandu_id
        ), kader AS (
            SELECT p.posyandu_id, count(*) AS n, count(*) FILTER (WHERE f.pelatihan_25) AS terlatih
            FROM dw.fact_kader_snapshot f JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
            GROUP BY p.posyandu_id
        ), st AS (
            SELECT nama_kecamatan, nama_kelurahan, jumlah_stunting, rasio_persen
            FROM dw.v_rasio_stunting_balita WHERE tingkat = 'kelurahan' AND tahun = {TAHUN_FITUR}
        )
        SELECT c.posyandu_key, c.posyandu_id, c.nama_posyandu, c.nama_kelurahan, c.nama_kecamatan, c.puskesmas,
               c.tahun_pertama_lapor,
               coalesce(fit.bulan_terlapor, 0), round(coalesce(fit.bulan_terlapor, 0) / 12.0, 4),
               fit.sas_ih, fit.cak_ih, round(100.0 * fit.cak_ih / nullif(fit.sas_ih, 0), 2),
               fit.sas_bb, fit.cak_bb, round(100.0 * fit.cak_bb / nullif(fit.sas_bb, 0), 2),
               coalesce(fit.baris_terlapor, 0), coalesce(fit.baris_anomali, 0),
               round(100.0 * fit.baris_anomali / nullif(fit.baris_terlapor, 0), 2),
               alat.jenis_std_baik, round(alat.jenis_std_baik / 5.0, 4), alat.unit_total, alat.unit_rusak,
               alat.ada_rusak_tstd,
               coalesce(kader.n, 0), coalesce(kader.terlatih, 0),
               round(100.0 * kader.terlatih / nullif(kader.n, 0), 2),
               st.jumlah_stunting, st.rasio_persen,
               coalesce(fit.bulan_terlapor, 0) > 0,
               %(batch_id)s
        FROM cur c
        LEFT JOIN fit ON fit.posyandu_id = c.posyandu_id
        LEFT JOIN alat ON alat.posyandu_id = c.posyandu_id
        LEFT JOIN kader ON kader.posyandu_id = c.posyandu_id
        LEFT JOIN st ON st.nama_kelurahan = c.nama_kelurahan AND st.nama_kecamatan = c.nama_kecamatan""",

    "kpi_kelurahan": f"""
        INSERT INTO mart.kpi_kelurahan
        WITH r AS (
            SELECT nama_kecamatan, nama_kelurahan, tahun, jumlah_stunting, penduduk_0_4, rasio_persen, is_pendekatan
            FROM dw.v_rasio_stunting_balita WHERE tingkat = 'kelurahan'
        ), lap AS (
            SELECT p.kelurahan_key, w.tahun,
                   count(DISTINCT p.posyandu_id) FILTER (WHERE f.status_key = 1)                    AS n_pos,
                   sum(f.sasaran) FILTER (WHERE w.bulan = 12 AND f.kelompok_key = 2)                AS sas_des,
                   round(100.0 * sum(f.cakupan) FILTER (WHERE f.kelompok_key = 2 AND {KPI})
                         / nullif(sum(f.sasaran) FILTER (WHERE f.kelompok_key = 2 AND {KPI}), 0), 2) AS pct_bb
            FROM dw.fact_kunjungan_bulanan f
            JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
            JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
            GROUP BY p.kelurahan_key, w.tahun
        )
        SELECT kl.kelurahan_key, kl.nama_kelurahan, kl.nama_kecamatan, r.tahun, r.jumlah_stunting, r.penduduk_0_4,
               r.rasio_persen, r.is_pendekatan, lap.n_pos, lap.sas_des, lap.pct_bb, %(batch_id)s
        FROM r
        JOIN dw.dim_kelurahan kl ON kl.nama_kelurahan = r.nama_kelurahan AND kl.nama_kecamatan = r.nama_kecamatan
        LEFT JOIN lap ON lap.kelurahan_key = kl.kelurahan_key AND lap.tahun = r.tahun""",

    "ringkasan_kualitas": """
        INSERT INTO mart.ringkasan_kualitas
        SELECT w.tahun, 'status', s.nama_status, count(*), %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f
        JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        JOIN dw.dim_status_data s ON s.status_key = f.status_key
        GROUP BY w.tahun, s.nama_status
        UNION ALL
        SELECT w.tahun, 'anomali', b.item, count(*) FILTER (WHERE f.flag_anomali & b.bit > 0), %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f
        JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        CROSS JOIN (VALUES (1, 'A1 Sasaran >= 10.000'), (2, 'A2 Sasaran di atas ambang kelompok'),
                           (4, 'A3 lonjakan > 5x median'), (8, 'Cakupan > Sasaran'), (16, 'Nilai negatif'),
                           (32, 'Sasaran 0, Cakupan > 0'), (64, 'Selisih persen > 1 poin'),
                           (127, 'Total baris anomali')) AS b(bit, item)
        GROUP BY w.tahun, b.item
        UNION ALL
        SELECT w.tahun, 'koreksi', 'Cakupan turunan T07', count(*) FILTER (WHERE f.is_cakupan_turunan), %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        GROUP BY w.tahun""",

    # Grain fakta (posyandu versi × bulan × kelompok) agar dashboard bisa memfilter per puskesmas & cluster.
    "kunjungan_posyandu_bulanan": f"""
        INSERT INTO mart.kunjungan_posyandu_bulanan
        SELECT f.posyandu_key, p.posyandu_id, kl.nama_kecamatan, p.puskesmas, f.waktu_key, w.tahun, w.bulan,
               k.kelompok_key, k.nama_kelompok, s.status_key, s.nama_status,
               CASE WHEN {KPI} THEN f.cakupan END, CASE WHEN {KPI} THEN f.sasaran END,
               f.flag_anomali, f.is_cakupan_turunan, %(batch_id)s
        FROM dw.fact_kunjungan_bulanan f
        JOIN dw.dim_waktu w ON w.waktu_key = f.waktu_key
        JOIN dw.dim_posyandu p ON p.posyandu_key = f.posyandu_key
        JOIN dw.dim_kelurahan kl ON kl.kelurahan_key = p.kelurahan_key
        JOIN dw.dim_kelompok_sasaran k ON k.kelompok_key = f.kelompok_key
        JOIN dw.dim_status_data s ON s.status_key = f.status_key""",

    "quality_gate_terakhir": """
        INSERT INTO mart.quality_gate_terakhir
        SELECT DISTINCT ON (kode_cek) kode_cek, fase, is_kritis, lulus, nilai, harapan, batch_id, dicatat_pada
        FROM dq.quality_result WHERE batch_id = %(batch_id)s
        ORDER BY kode_cek, qr_id DESC""",
}


def build_mart(batch_id: str, percobaan_ke: int | None = None) -> dict:
    with catat_task(batch_id, "build_mart", "mart", percobaan_ke) as hasil:
        conn = buka_koneksi()
        n = {}
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM dw.fact_kunjungan_bulanan WHERE batch_id = %s", (batch_id,))
                if cur.fetchone()[0] == 0:
                    # Mart hanya boleh dibangun dari DWH batch ini (load_dw harus sukses lebih dulu).
                    raise RuntimeError(f"dw.fact_kunjungan_bulanan tidak berisi batch {batch_id}; mart tidak dibangun")
                for tabel, sql in SQL_MART.items():
                    cur.execute(f"DELETE FROM mart.{tabel}")
                    cur.execute(sql, {"batch_id": batch_id})
                    n[tabel] = cur.rowcount
        finally:
            conn.close()
        hasil.jumlah_baris = sum(n.values())
        hasil.pesan = ", ".join(f"{k}={v}" for k, v in n.items())
        return n
