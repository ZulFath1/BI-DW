"""Quality gate Q01–Q09 (dokumen UTS bagian 4.2, CLAUDE.md 4.4).

Dua fase:
  sebelum_load  Q01, Q02, Q03, Q07, Q08, Q09 dijalankan pada curated (+ staging sebagai pembanding)
                oleh task `quality_gate`. Bila ada pemeriksaan KRITIS yang gagal (Q02), DAG
                bercabang ke task `quarantine` dan DWH tidak disentuh.
  dalam_load    Q04, Q05, Q06 dijalankan oleh task `load_dw` DI DALAM transaksi load, setelah
                data disisipkan tetapi sebelum COMMIT. Bila ada yang gagal -> ROLLBACK, DWH
                kembali seperti sebelum batch (lihat etl/load_dw.py).

Hasil setiap pemeriksaan ditulis ke dq.quality_result (koneksi autocommit sendiri, sehingga tetap
tersimpan walaupun transaksi load di-rollback) dan ringkasannya ke dq.dq_log.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from common.batch import catat_quarantine, catat_task, tulis_dq_log
from common.db import buka_koneksi

log = logging.getLogger(__name__)


@dataclass
class HasilCek:
    kode: str
    fase: str
    kritis: bool
    lulus: bool
    nilai: str
    harapan: str
    keterangan: str = ""


def simpan_hasil(batch_id: str, hasil: list[HasilCek]) -> None:
    """Menulis hasil ke dq.quality_result dan dq.dq_log dengan koneksi autocommit sendiri."""
    conn = buka_koneksi(autocommit=True)
    try:
        with conn.cursor() as cur:
            for h in hasil:
                cur.execute(
                    "INSERT INTO dq.quality_result (batch_id, kode_cek, fase, is_kritis, lulus, nilai, harapan, keterangan) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (batch_id, h.kode, h.fase, h.kritis, h.lulus, h.nilai, h.harapan, h.keterangan))
            tulis_dq_log(cur, batch_id, "quality", [{
                "kode_aturan": h.kode,
                "tingkat": "INFO" if h.lulus else ("KRITIS" if h.kritis else "PERINGATAN"),
                "objek": h.fase, "nilai": h.nilai[:500],
                "keterangan": f"{'LULUS' if h.lulus else 'GAGAL'}; harapan {h.harapan}. {h.keterangan}"[:2000],
            } for h in hasil])
    finally:
        conn.close()


def _satu(cur, sql: str, param=None):
    cur.execute(sql, param)
    return cur.fetchone()


# ---------------------------------------------------------------- sebelum load
def cek_q01(cur) -> HasilCek:
    cur.execute("""
        WITH s AS (SELECT tahun_sheet, count(DISTINCT baris_sumber) AS n_pos FROM stg.kunjungan_bulanan GROUP BY 1),
             c AS (SELECT tahun_sheet, count(*) AS n FROM curated.kunjungan_bulanan GROUP BY 1)
        SELECT s.tahun_sheet, s.n_pos * 60, coalesce(c.n, 0) FROM s LEFT JOIN c USING (tahun_sheet) ORDER BY 1""")
    rows = cur.fetchall()
    lulus = all(h == n for _, h, n in rows)
    return HasilCek("Q01", "sebelum_load", False, lulus,
                    "; ".join(f"{t}: {n}" for t, _, n in rows) + f"; total {sum(r[2] for r in rows)}",
                    "; ".join(f"{t}: {h}" for t, h, _ in rows) + " (posyandu × 12 × 5)")


def cek_q02(cur) -> HasilCek:
    ganda_tahun = _satu(cur, """SELECT count(*) FROM (SELECT posyandu_id, tahun_sheet FROM curated.kunjungan_bulanan
                                GROUP BY 1, 2 HAVING count(DISTINCT baris_sumber) > 1) x""")[0]
    ganda_current = _satu(cur, """SELECT count(*) FROM (SELECT posyandu_id FROM curated.posyandu_versi WHERE is_current
                                  GROUP BY 1 HAVING count(*) > 1) x""")[0]
    tanpa_current = _satu(cur, """SELECT count(*) FROM (SELECT posyandu_id FROM curated.posyandu_versi
                                  GROUP BY 1 HAVING NOT bool_or(is_current)) x""")[0]
    tumpang = _satu(cur, """SELECT count(*) FROM curated.posyandu_versi a JOIN curated.posyandu_versi b
                            ON a.posyandu_id = b.posyandu_id AND a.versi < b.versi AND a.berlaku_sampai >= b.berlaku_mulai""")[0]
    # Dua baris sumber pada sheet yang sama dipetakan ke posyandu_id yang sama (transform hanya
    # memuat baris pertama ke curated, sehingga ganda harus dicari di tabel pemetaan).
    ganda_sumber = _satu(cur, """SELECT count(*) FROM (SELECT sumber, posyandu_id FROM curated.pemetaan_posyandu
                                 WHERE sumber LIKE 'laporan\\_%%' GROUP BY 1, 2 HAVING count(*) > 1) x""")[0]
    n_pos, n_versi = _satu(cur, "SELECT count(DISTINCT posyandu_id), count(*) FROM curated.posyandu_versi")
    lulus = ganda_tahun == ganda_sumber == tanpa_current == ganda_current == tumpang == 0
    return HasilCek("Q02", "sebelum_load", True, lulus,
                    f"kunci ganda per tahun {ganda_tahun + ganda_sumber}; versi aktif ganda {ganda_current}; "
                    f"tanpa versi aktif {tanpa_current}; rentang versi tumpang tindih {tumpang}", "semua 0",
                    f"{n_pos} posyandu, {n_versi} versi")


def cek_q03(cur) -> HasilCek:
    # Persen teks yang masih tidak bisa dibaca sebagai angka (sudah dicatat T03 dan dimuat NULL).
    n = _satu(cur, r"""SELECT count(*) FILTER (WHERE persen_dilayani_asli IS NOT NULL AND persen_dilayani_asli !~ '^\s*-?\d+(\.\d+)?\s*$')
                            + count(*) FILTER (WHERE persen_dirujuk_asli IS NOT NULL AND persen_dirujuk_asli !~ '^\s*-?\d+(\.\d+)?\s*$')
                       FROM stg.kunjungan_bulanan""")[0]
    # Dan setiap persen yang terbaca di staging memang terisi di curated.
    hilang = _satu(cur, r"""SELECT count(*) FROM stg.kunjungan_bulanan s
                            JOIN curated.kunjungan_bulanan c ON c.tahun_sheet = s.tahun_sheet AND c.baris_sumber = s.baris_sumber
                             AND c.waktu_key = s.tahun_sheet * 100 + s.bulan_ke AND c.kelompok_key = s.kelompok_ke
                            WHERE s.persen_dilayani_asli ~ '^\s*-?\d+(\.\d+)?\s*$' AND c.persen_dilayani_laporan IS NULL""")[0]
    return HasilCek("Q03", "sebelum_load", False, n == 0 and hilang == 0,
                    f"persen non-numerik tersisa {n}; persen terbaca tetapi hilang di curated {hilang}", "0 dan 0")


def cek_q07(cur) -> HasilCek:
    e1 = _satu(cur, """SELECT count(DISTINCT wilayah_asli) FROM stg.penduduk_kelompok_umur
                       WHERE tingkat = 'kelurahan' AND jenis_baris = 'data'""")[0]
    e1_masuk = _satu(cur, "SELECT count(DISTINCT nama_kelurahan) FROM curated.penduduk_kelurahan")[0]
    e2 = _satu(cur, """SELECT count(*) FROM stg.stunting_kelurahan s WHERE jenis_baris = 'data'
                       AND NOT EXISTS (SELECT 1 FROM curated.kelurahan k WHERE k.kode_wilayah = s.kode_kelurahan_asli)""")[0]
    lap = _satu(cur, """SELECT count(*) FROM curated.posyandu_versi v
                        WHERE NOT EXISTS (SELECT 1 FROM curated.kelurahan k WHERE k.nama_kelurahan = v.nama_kelurahan)""")[0]
    n_kel = _satu(cur, "SELECT count(*) FROM curated.kelurahan")[0]
    tak = (e1 - e1_masuk) + e2 + lap
    return HasilCek("Q07", "sebelum_load", False, tak == 0,
                    f"E1 tidak terpetakan {e1 - e1_masuk} dari {e1}; E2 baris tidak terpetakan {e2}; "
                    f"kelurahan posyandu tidak terpetakan {lap}", f"0 (atau tercatat di dq_log); {n_kel} kelurahan resmi")


def cek_q08(cur) -> HasilCek:
    cur.execute("""
        WITH kel AS (SELECT k.nama_kecamatan, sum(p.jumlah_penduduk) AS n
                     FROM curated.penduduk_kelurahan p JOIN curated.kelurahan k USING (nama_kelurahan)
                     WHERE p.kelompok_umur_key = 1 GROUP BY 1),
             buku AS (SELECT upper(wilayah_asli) AS nama_kecamatan, replace(jumlah_asli, '.', '')::int AS n
                      FROM stg.penduduk_kelompok_umur
                      WHERE tingkat = 'kecamatan' AND jenis_baris = 'data' AND kelompok_umur_asli = '0-4')
        SELECT buku.nama_kecamatan, buku.n, coalesce(kel.n, 0) FROM buku LEFT JOIN kel USING (nama_kecamatan) ORDER BY 1""")
    rows = cur.fetchall()
    kota = _satu(cur, """SELECT replace(jumlah_asli, '.', '')::int FROM stg.penduduk_kelompok_umur
                         WHERE tingkat = 'kota' AND jenis_baris = 'data' AND kelompok_umur_asli = '0-4'""")
    total_kel = sum(r[2] for r in rows)
    lulus = len(rows) == 5 and all(b == k for _, b, k in rows) and kota is not None and kota[0] == total_kel
    return HasilCek("Q08", "sebelum_load", False, lulus,
                    "; ".join(f"{n}: Σkel {k} vs buku {b}" for n, b, k in rows) + f"; kota Σ {total_kel} vs Tabel 6 "
                    f"{kota[0] if kota else None}", "selisih 0 di 5 kecamatan dan kota",
                    "termasuk Kelayan Tengah (kelompok umur dari urutan baris, K16)")


def cek_q09(cur) -> HasilCek:
    jk_buruk = _satu(cur, "SELECT count(*) FROM curated.kader_snapshot WHERE jenis_kelamin IS NULL OR jenis_kelamin NOT IN ('L','P')")[0]
    stg = _satu(cur, "SELECT count(*) FROM stg.kader")[0]
    fakta = _satu(cur, "SELECT count(*) FROM curated.kader_snapshot")[0]
    batch = _satu(cur, "SELECT max(batch_id) FROM curated.kader_snapshot")[0]
    t13 = _satu(cur, "SELECT count(*) FROM dq.dq_log WHERE batch_id = %s AND kode_aturan = 'T13' AND tingkat = 'PERINGATAN'",
                (batch,))[0]
    # Baris tak terpetakan yang sekaligus terkena T13 tidak dihitung dua kali.
    ditahan = _satu(cur, """SELECT count(*) FROM curated.pemetaan_posyandu p WHERE sumber = 'kader' AND posyandu_id IS NULL
                            AND NOT EXISTS (SELECT 1 FROM dq.dq_log d WHERE d.batch_id = %s AND d.kode_aturan = 'T13'
                                            AND d.tingkat = 'PERINGATAN' AND d.baris_sumber = p.baris_sumber)""", (batch,))[0]
    lulus = jk_buruk == 0 and fakta + t13 + ditahan == stg
    return HasilCek("Q09", "sebelum_load", False, lulus,
                    f"jenis kelamin kosong/N/A di fakta {jk_buruk}; fakta {fakta} + dihapus T13 {t13} + ditahan "
                    f"(posyandu tak terpetakan) {ditahan} = {fakta + t13 + ditahan} vs staging {stg}",
                    "0; jumlah = staging",
                    "UTS: 2.543 + 2 = 2.545; implementasi menahan 1 baris berposyandu 'N/A' (lihat P07)")


CEK_SEBELUM_LOAD = [cek_q01, cek_q02, cek_q03, cek_q07, cek_q08, cek_q09]


def quality_gate(batch_id: str, percobaan_ke: int | None = None) -> dict:
    """Menjalankan pemeriksaan sebelum load. Mengembalikan {'lulus': bool, 'gagal_kritis': [...], ...}."""
    with catat_task(batch_id, "quality_gate", "quality", percobaan_ke) as hasil_task:
        conn = buka_koneksi()
        try:
            with conn, conn.cursor() as cur:
                n_curated = _satu(cur, "SELECT count(*) FROM curated.kunjungan_bulanan")[0]
                if n_curated == 0:
                    hasil = [HasilCek("Q01", "sebelum_load", True, False, "curated kosong", "> 0",
                                      "transform belum dijalankan")]
                else:
                    hasil = [f(cur) for f in CEK_SEBELUM_LOAD]
        finally:
            conn.close()
        simpan_hasil(batch_id, hasil)
        gagal_kritis = [h.kode for h in hasil if h.kritis and not h.lulus]
        peringatan = [h.kode for h in hasil if not h.kritis and not h.lulus]
        hasil_task.jumlah_baris = len(hasil)
        hasil_task.pesan = (f"lulus {sum(h.lulus for h in hasil)}/{len(hasil)}; gagal kritis {gagal_kritis}; "
                            f"peringatan {peringatan}")
        return {"lulus": not gagal_kritis, "gagal_kritis": gagal_kritis, "peringatan": peringatan,
                "hasil": {h.kode: h.lulus for h in hasil}}


def quarantine(batch_id: str, gagal_kritis: list[str] | None = None, percobaan_ke: int | None = None) -> None:
    """Cabang bila quality gate gagal: batch dicatat di dq.quarantine, DWH TIDAK diubah,
    lalu task sengaja gagal agar DAG run terlihat GAGAL di Airflow (monitoring)."""
    with catat_task(batch_id, "quarantine", "quality", percobaan_ke) as hasil_task:
        conn = buka_koneksi()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""SELECT kode_cek, nilai, harapan, is_kritis FROM dq.quality_result
                               WHERE batch_id = %s AND NOT lulus ORDER BY qr_id""", (batch_id,))
                gagal = [{"kode": k, "nilai": n, "harapan": h, "kritis": kr} for k, n, h, kr in cur.fetchall()]
                if gagal_kritis is None:
                    gagal_kritis = sorted({g["kode"] for g in gagal if g["kritis"]})
                cur.execute("SELECT count(*) FROM curated.kunjungan_bulanan")
                n = cur.fetchone()[0]
                # Rincian baris sumber berkunci ganda (bila ada) ikut disimpan untuk ditindaklanjuti.
                cur.execute("""SELECT sumber, posyandu_id, array_agg(baris_sumber ORDER BY baris_sumber)
                               FROM curated.pemetaan_posyandu WHERE sumber LIKE 'laporan\\_%%'
                               GROUP BY 1, 2 HAVING count(*) > 1""")
                ganda = [{"sumber": s, "posyandu_id": p, "baris_sumber": b} for s, p, b in cur.fetchall()]
        finally:
            conn.close()
        catat_quarantine(batch_id, "quality", "curated", "batch",
                         f"quality gate gagal (kritis: {gagal_kritis}); curated tidak dimuat ke DWH",
                         {"pemeriksaan_gagal": gagal, "baris_curated_kunjungan": n, "kunci_ganda": ganda})
        hasil_task.pesan = f"batch dikarantina; kritis gagal {gagal_kritis}"
    raise RuntimeError(f"batch {batch_id} dikarantina: quality gate kritis gagal {gagal_kritis}; DWH tidak diubah")
