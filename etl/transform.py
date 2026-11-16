"""Task transform: stg (nilai asli) -> curated (bersih, bertipe, kunci natural). Aturan T01–T19.

Ringkas per bagian (detail aturan di dokumen UTS bagian 4.2 dan CLAUDE.md 4.3):
  kelurahan       daftar resmi 52 kelurahan + kode wilayah dari E2 (T16)
  posyandu        T04 rapikan nama, T05 posyandu_id + SCD tipe 2 (puskesmas), muncul_kembali
  kunjungan       T01–T02 (sudah dalam bentuk panjang di staging), T03 persen teks -> angka,
                  T06/T15 status data, T07 cakupan turunan, T08, T09 anomali, T10 hitung ulang
                  persen, T11 Dirujuk apa adanya
  alat            T14: isi kecamatan/kelurahan dari baris atas, petakan nama posyandu, baris total
                  hanya untuk rekonsiliasi, tensimeter tidak dimuat, '-' -> NULL
  kader           T12 masking/penyeragaman, T13 hapus jenis kelamin 'N/A', pasangan ganda
  penduduk (E1)   T16–T17 + K16 (Kelayan Tengah: kelompok umur dari urutan baris, tervalidasi)
  stunting (E2)   T16, T18 rekonsiliasi total portal

Semua tabel curated ditulis ulang dalam SATU transaksi bersama catatan dq_log-nya: bila satu
bagian gagal, curated lama tetap utuh. Tidak ada nama/alamat kader yang dibaca di sini
(staging tidak menyimpannya).
"""

from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import date

import pandas as pd
from psycopg2.extras import execute_values

from common.batch import catat_task, tulis_dq_log
from common.db import buka_koneksi
from common.errors import StrukturSumberError
from etl.normalisasi import (PencocokPosyandu, buat_posyandu_id, kelurahan_resmi, kunci_cocok, nama_normal,
                             puskesmas_resmi, rapikan)

log = logging.getLogger(__name__)
TAHAP = "transform"

NAMA_BULAN = {"JANUARI": 1, "FEBRUARI": 2, "MARET": 3, "APRIL": 4, "MEI": 5, "JUNI": 6, "JULI": 7,
              "AGUSTUS": 8, "SEPTEMBER": 9, "OKTOBER": 10, "NOVEMBER": 11, "NOPEMBER": 11, "DESEMBER": 12}
ALAT = ["timbangan_bayi", "panjang_bayi", "timbangan_badan", "tinggi_badan", "lila"]  # urutan = dim_alat
# Kader: sheet hanya menyebut "Tahun 2026" -> anggota waktu "bulan tidak diketahui" (K21).
KADER_WAKTU_KEY, KADER_PERIODE = 202600, "Tahun 2026"
# E1: buku Data Agregat Kependudukan Semester II 2025 -> bulan akhir semester (aturan UTS).
E1_WAKTU_KEY, E1_SUMBER = 202512, "Disdukcapil DKB Semester II 2025"
URUTAN_UMUR = ["0-4", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", ">=75"]
FLAG_URUTAN = "label umur dari urutan baris"
# Bit flag_anomali (T09/T10)
A1, A2, A3, CAK_GT_SAS, NEGATIF, SAS0_CAK, SELISIH_PERSEN = 1, 2, 4, 8, 16, 32, 64
A1_AMBANG, A3_KALI, A3_MIN_NILAI, SELISIH_POIN = 10_000, 5, 3, 1.0


def _angka(teks) -> float | None:
    """Teks sel -> angka. None bila kosong. ValueError bila bukan angka."""
    if teks is None:
        return None
    s = str(teks).strip()
    if s == "":
        return None
    return float(s)


def _angka_buku(teks) -> int | None:
    """Angka format buku Disdukcapil ('1.273') -> 1273."""
    if teks is None or str(teks).strip() == "":
        return None
    s = str(teks).replace(".", "").strip()
    if not s.isdigit():
        raise ValueError(f"bukan angka bulat: {teks!r}")
    return int(s)


def _angka_buku_aman(teks) -> int | None:
    """Seperti _angka_buku, tetapi None bila bukan angka bulat (dipakai untuk baris Total yang
    bisa bergeser kolom di buku, mis. Kelayan Dalam hal. 22)."""
    try:
        return _angka_buku(teks)
    except ValueError:
        return None


class Konteks:
    """Menampung catatan dq_log dan ringkasan selama transform."""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        self.dq: list[dict] = []
        self.ringkas: dict[str, object] = {}

    def catat(self, kode, tingkat, keterangan, objek=None, baris=None, kolom=None, nilai=None):
        self.dq.append({"kode_aturan": kode, "tingkat": tingkat, "objek": objek, "baris_sumber": baris,
                        "kolom": kolom, "nilai": None if nilai is None else str(nilai)[:500],
                        "keterangan": keterangan})


# ---------------------------------------------------------------- kelurahan (E2, T16)
def transform_kelurahan(cur, ktx: Konteks) -> pd.DataFrame:
    cur.execute("""SELECT tahun_data, kode_kecamatan_asli, kecamatan_asli, kode_kelurahan_asli, kelurahan_asli
                   FROM stg.stunting_kelurahan WHERE jenis_baris = 'data' ORDER BY tahun_data DESC, baris_tabel""")
    rows = cur.fetchall()
    if not rows:
        raise StrukturSumberError("stg.stunting_kelurahan kosong: daftar kelurahan resmi tidak dapat dibentuk")
    tahun_terbaru = rows[0][0]
    kel = {}
    for th, kkec, kec, kkel, nkel in rows:
        if th != tahun_terbaru:
            lama = kel.get(kkel)
            if lama is None or lama["nama_kelurahan"] != kelurahan_resmi(nkel):
                ktx.catat("T16", "PERINGATAN", f"kode/nama kelurahan E2 {th} berbeda dari {tahun_terbaru}",
                          f"E2 {th}", kolom="kode_kelurahan", nilai=f"{kkel} {nkel}")
            continue
        kel[kkel] = {"kode_wilayah": kkel, "nama_kelurahan": kelurahan_resmi(nkel),
                     "kode_kecamatan": kkec, "nama_kecamatan": rapikan(kec)}
    df = pd.DataFrame(kel.values())
    if df.nama_kelurahan.duplicated().any():
        raise StrukturSumberError("nama kelurahan ganda di daftar resmi E2")
    ktx.catat("T16", "INFO", f"daftar kelurahan resmi dari E2 {tahun_terbaru}", "dim_kelurahan", nilai=len(df))
    ktx.ringkas["kelurahan"] = len(df)
    return df


# ---------------------------------------------------------------- posyandu (T04, T05)
def transform_posyandu(cur, ktx: Konteks, kelurahan: pd.DataFrame):
    cur.execute("""SELECT DISTINCT tahun_sheet, baris_sumber, kecamatan_asli, kelurahan_asli, puskesmas_asli,
                          posyandu_asli FROM stg.kunjungan_bulanan""")
    lap = pd.DataFrame(cur.fetchall(), columns=["tahun", "baris", "kec", "kel", "pkm", "pos"])
    lap["kel_r"] = lap.kel.map(kelurahan_resmi)
    lap["kec_r"] = lap.kec.map(rapikan)
    lap["pkm_r"] = lap.pkm.map(puskesmas_resmi)
    lap["nama_n"] = [nama_normal(p, k) for p, k in zip(lap.pos, lap.kel_r)]
    lap["kunci"] = [kunci_cocok(n, k) for n, k in zip(lap.nama_n, lap.kel_r)]
    lap["posyandu_id"] = [buat_posyandu_id(k, c) for k, c in zip(lap.kel_r, lap.kunci)]

    # Kelurahan laporan harus ada di daftar resmi, dan kecamatannya harus sama.
    resmi = dict(zip(kelurahan.nama_kelurahan, kelurahan.nama_kecamatan))
    for kel, kec in lap[["kel_r", "kec_r"]].drop_duplicates().itertuples(index=False):
        if kel not in resmi:
            raise StrukturSumberError(f"kelurahan laporan '{kel}' tidak ada di daftar resmi (tambahkan alias T16)")
        if resmi[kel] != kec:
            ktx.catat("T04", "PERINGATAN", f"kecamatan laporan '{kec}' ≠ kecamatan resmi '{resmi[kel]}'",
                      "laporan", kolom="kecamatan", nilai=kel)

    # Q02 (pratinjau): kunci ganda dalam satu tahun berarti dua baris dianggap posyandu sama.
    ganda = lap[lap.duplicated(["tahun", "posyandu_id"], keep=False)]
    for r in ganda.itertuples():
        ktx.catat("T05", "KRITIS", "posyandu_id ganda dalam satu tahun", f"sheet {r.tahun}", r.baris,
                  "Posyandu", r.posyandu_id)

    versi_rows = []
    for pid, g in lap.sort_values("tahun").groupby("posyandu_id", sort=True):
        tahun = sorted(g.tahun.unique())
        muncul_kembali = any(b - a > 1 for a, b in zip(tahun, tahun[1:]))
        terbaru = g[g.tahun == tahun[-1]].iloc[0]
        segmen = []  # (tahun_mulai, puskesmas)
        for th in tahun:
            pkm = g[g.tahun == th].pkm_r.iloc[0]
            if not segmen or segmen[-1][1] != pkm:
                segmen.append((th, pkm))
        for i, (th, pkm) in enumerate(segmen):
            akhir = date(segmen[i + 1][0], 1, 1).replace(day=1) if i + 1 < len(segmen) else None
            versi_rows.append({
                "posyandu_id": pid, "versi": i + 1, "nama_posyandu": rapikan(terbaru.pos),
                "nama_posyandu_normal": terbaru.nama_n, "nama_kelurahan": terbaru.kel_r, "puskesmas": pkm,
                "berlaku_mulai": date(th, 1, 1),
                "berlaku_sampai": date(akhir.year - 1, 12, 31) if akhir else date(9999, 12, 31),
                "is_current": i + 1 == len(segmen), "tahun_pertama_lapor": tahun[0],
                "tahun_terakhir_lapor": tahun[-1], "muncul_kembali": muncul_kembali,
            })
        if muncul_kembali:
            ktx.catat("T05", "INFO", f"posyandu muncul kembali setelah absen (tahun lapor {tahun})", "dim_posyandu",
                      nilai=pid)
    versi = pd.DataFrame(versi_rows)
    pindah = versi[versi.versi > 1]
    for r in pindah.itertuples():
        lama = versi[(versi.posyandu_id == r.posyandu_id) & (versi.versi == r.versi - 1)].puskesmas.iloc[0]
        ktx.catat("T05", "INFO", f"SCD2: puskesmas berubah {lama} -> {r.puskesmas} mulai {r.berlaku_mulai.year}",
                  "dim_posyandu", nilai=r.posyandu_id)
    ktx.ringkas["posyandu"] = versi.posyandu_id.nunique()
    ktx.ringkas["versi_posyandu"] = len(versi)
    ktx.catat("T05", "INFO", f"{versi.posyandu_id.nunique()} posyandu unik, {len(versi)} versi (SCD2); "
              f"per tahun: {lap.groupby('tahun').posyandu_id.nunique().to_dict()}", "dim_posyandu", nilai=len(versi))

    pemetaan = [{"sumber": f"laporan_{r.tahun}", "baris_sumber": int(r.baris), "kelurahan_asli": r.kel,
                 "nama_posyandu_asli": r.pos, "kunci_cocok": r.kunci, "posyandu_id": r.posyandu_id,
                 "cara": "kunci", "skor": None, "keterangan": None} for r in lap.itertuples()]
    katalog = {pid: (k, c) for pid, k, c in lap[["posyandu_id", "kel_r", "kunci"]].drop_duplicates().itertuples(index=False)}
    peta_baris = {(int(t), int(b)): pid for t, b, pid in lap[["tahun", "baris", "posyandu_id"]].itertuples(index=False)}
    return versi, pemetaan, katalog, peta_baris


# ---------------------------------------------------------------- kunjungan (T03, T06–T11, T15)
def _ke_angka_kolom(df: pd.DataFrame, kolom: str, ktx: Konteks, aturan: str) -> pd.Series:
    """Konversi teks -> angka; yang gagal menjadi NULL dan dicatat di dq_log (T03)."""
    hasil = pd.to_numeric(df[kolom], errors="coerce")
    gagal = df[df[kolom].notna() & hasil.isna()]
    for r in gagal.itertuples():
        ktx.catat(aturan, "PERINGATAN", "nilai tidak dapat diubah menjadi angka; dimuat sebagai NULL",
                  f"sheet {r.tahun_sheet}", r.baris_sumber, kolom, getattr(r, kolom))
    ktx.ringkas.setdefault("q03_gagal_konversi", 0)
    if kolom.startswith("persen"):
        ktx.ringkas["q03_gagal_konversi"] += len(gagal)
    return hasil


def transform_kunjungan(cur, ktx: Konteks, peta_baris: dict) -> pd.DataFrame:
    cur.execute("SELECT kelompok_key, ambang_a2_sasaran FROM dw.dim_kelompok_sasaran")
    ambang = dict(cur.fetchall())
    if len(ambang) != 5:
        raise StrukturSumberError("dw.dim_kelompok_sasaran belum berisi 5 kelompok (jalankan init_schema)")
    cur.execute("""SELECT tahun_sheet, baris_sumber, bulan_ke, kelompok_ke, sasaran_asli, cakupan_asli,
                          persen_dilayani_asli, dirujuk_asli, persen_dirujuk_asli
                   FROM stg.kunjungan_bulanan""")
    df = pd.DataFrame(cur.fetchall(), columns=["tahun_sheet", "baris_sumber", "bulan_ke", "kelompok_ke", "sasaran_asli",
                                               "cakupan_asli", "persen_dilayani_asli", "dirujuk_asli",
                                               "persen_dirujuk_asli"])
    df["posyandu_id"] = [peta_baris[(t, b)] for t, b in zip(df.tahun_sheet, df.baris_sumber)]
    df["waktu_key"] = df.tahun_sheet.astype(int) * 100 + df.bulan_ke.astype(int)
    df["kelompok_key"] = df.kelompok_ke.astype(int)

    # T03: teks -> angka
    sas = _ke_angka_kolom(df, "sasaran_asli", ktx, "T03")
    cak = _ke_angka_kolom(df, "cakupan_asli", ktx, "T03")
    pdl = _ke_angka_kolom(df, "persen_dilayani_asli", ktx, "T03")
    dir_ = _ke_angka_kolom(df, "dirujuk_asli", ktx, "T03")
    pdr = _ke_angka_kolom(df, "persen_dirujuk_asli", ktx, "T03")
    for nama, s in (("sasaran", sas), ("cakupan", cak), ("dirujuk", dir_)):
        bukan_bulat = s.notna() & (s != s.round())
        if bukan_bulat.any():
            ktx.catat("T03", "PERINGATAN", f"{int(bukan_bulat.sum())} nilai {nama} bukan bilangan bulat; dibulatkan",
                      "laporan", kolom=nama)
    semua_kosong = df[["sasaran_asli", "cakupan_asli", "persen_dilayani_asli", "dirujuk_asli",
                       "persen_dirujuk_asli"]].isna().all(axis=1)

    # T06/T15: status data
    bulan_berdata_2026 = set(df[(df.tahun_sheet == 2026) & ~semua_kosong].bulan_ke)
    status = pd.Series(1, index=df.index)  # Terlapor
    kosong_sas = sas.isna()
    lama = df.tahun_sheet < 2026
    status[kosong_sas & lama] = 2                                             # Tidak berlaku
    status[kosong_sas & ~lama & df.bulan_ke.isin(bulan_berdata_2026)] = 3       # Tidak terisi
    status[kosong_sas & ~lama & ~df.bulan_ke.isin(bulan_berdata_2026)] = 4      # Belum dilaporkan
    # K37: Sasaran kosong tetapi ada pelayanan nyata (Cakupan/Dirujuk > 0) -> status 5. Baris yang
    # hanya berisi nol bawaan templat (persen 0, Cakupan 0, Dirujuk 0) tidak memuat informasi
    # pelayanan, sehingga tetap diperlakukan kosong (status 2/3 di atas).
    bermakna = kosong_sas & ((cak > 0) | (dir_ > 0))
    status[bermakna] = 5                                                      # Cakupan tanpa sasaran
    sebagian = kosong_sas & ~semua_kosong
    if sebagian.any():
        def _pt(m):
            return {int(k): int(v) for k, v in df[m].groupby("tahun_sheet").size().items()}
        hanya_nol = sebagian & ~bermakna
        ktx.catat("T06", "INFO", "Sasaran kosong, metrik lain hanya nol bawaan (persen/Cakupan/Dirujuk = 0): "
                  f"diperlakukan kosong (Tidak berlaku/Tidak terisi); per tahun {_pt(hanya_nol)}", "laporan",
                  kolom="sasaran", nilai=int(hanya_nol.sum()))
        ktx.catat("T06", "PERINGATAN", "Sasaran kosong tetapi Cakupan/Dirujuk > 0: status 'Cakupan tanpa sasaran' "
                  f"(K37), nilai dimuat apa adanya, persen NULL; per tahun {_pt(bermakna)}", "laporan",
                  kolom="sasaran", nilai=int(bermakna.sum()))
    ktx.catat("T15", "INFO", f"bulan 2026 yang sudah berisi data: {sorted(bulan_berdata_2026)}; bulan lain "
              "berstatus Belum dilaporkan (tidak dimuat sebagai 0)", "sheet 2026")

    # T07: Sasaran > 0, Cakupan kosong, % Dilayani = 0 -> Cakupan = 0 (turunan)
    turunan = (sas > 0) & cak.isna() & (pdl == 0)
    cak = cak.where(~turunan, 0)
    # T08: Sasaran = 0 dan Cakupan kosong -> tetap NULL (tidak ada tindakan; dihitung untuk laporan)
    t08 = (sas == 0) & cak.isna()

    # T10: hitung ulang persen
    persen = (cak / sas * 100).where((sas > 0) & cak.notna()).round(2)
    terlalu_besar = persen.abs() >= 1e7
    if terlalu_besar.any():
        ktx.catat("T10", "PERINGATAN", "persen hitung ulang melebihi kapasitas kolom; dimuat NULL", "laporan",
                  kolom="persen_dilayani", nilai=int(terlalu_besar.sum()))
        persen = persen.where(~terlalu_besar)

    # T09/T10: penanda anomali (bitmask)
    flag = pd.Series(0, index=df.index)
    flag += A1 * (sas >= A1_AMBANG)
    flag += A2 * (sas > df.kelompok_key.map(ambang))
    positif = sas.where(sas > 0)
    grup = [df.posyandu_id, df.tahun_sheet, df.kelompok_key]
    median = positif.groupby(grup).transform("median")
    n_pos = positif.groupby(grup).transform("count")
    flag += A3 * ((n_pos >= A3_MIN_NILAI) & (sas > A3_KALI * median))
    # Cakupan > Sasaran dihitung harfiah seperti definisi UTS (termasuk Sasaran 0/negatif); bit boleh
    # tumpang tindih dengan bit 32 (Sasaran 0 tetapi Cakupan > 0), sama seperti A2/A3.
    flag += CAK_GT_SAS * (cak > sas)
    negatif = pd.concat([sas, cak, pdl, dir_, pdr], axis=1).lt(0).any(axis=1)
    flag += NEGATIF * negatif
    flag += SAS0_CAK * ((sas == 0) & (cak > 0))
    flag += SELISIH_PERSEN * ((persen - pdl).abs() > SELISIH_POIN)
    flag = flag.astype(int)

    out = pd.DataFrame({
        "posyandu_id": df.posyandu_id, "waktu_key": df.waktu_key, "kelompok_key": df.kelompok_key,
        "status_key": status.astype(int), "sasaran": sas.round(), "cakupan": cak.round(),
        "persen_dilayani": persen, "persen_dilayani_laporan": pdl.round(2), "dirujuk": dir_.round(),
        "persen_dirujuk": pdr.round(2), "is_cakupan_turunan": turunan, "flag_anomali": flag,
        "tahun_sheet": df.tahun_sheet.astype(int), "baris_sumber": df.baris_sumber.astype(int),
    })

    # Ringkasan per tahun ke dq_log (INFO) — dicocokkan dengan angka acuan UTS di laporan tahap.
    th = out.tahun_sheet
    def per_tahun(mask):
        return {int(k): int(v) for k, v in mask.groupby(th).sum().items()}
    ringkas = {
        "baris": per_tahun(pd.Series(True, index=out.index)),
        "anomali": per_tahun(flag > 0), "A1": per_tahun((flag & A1) > 0), "A2": per_tahun((flag & A2) > 0),
        "A3": per_tahun((flag & A3) > 0), "cakupan>sasaran": per_tahun((flag & CAK_GT_SAS) > 0),
        "negatif": per_tahun((flag & NEGATIF) > 0), "sasaran0_cakupan>0": per_tahun((flag & SAS0_CAK) > 0),
        "selisih_persen": per_tahun((flag & SELISIH_PERSEN) > 0), "T07": per_tahun(turunan), "T08": per_tahun(t08),
        "status": {int(k): v for k, v in out.groupby(["tahun_sheet"]).status_key.value_counts().unstack(fill_value=0)
                   .to_dict("index").items()},
    }
    for kode, isi in ringkas.items():
        ktx.catat("T09" if kode not in ("baris", "T07", "T08", "status") else ("T06" if kode == "status" else
                  ("T02" if kode == "baris" else kode)), "INFO", f"ringkasan per tahun: {kode} = {isi}",
                  "kunjungan_bulanan", kolom=kode)
    ktx.ringkas["kunjungan"] = ringkas

    # Kunci ganda (dua baris sumber -> posyandu sama pada tahun sama, sudah dicatat T05 KRITIS):
    # curated menyimpan baris sumber pertama saja agar transform tetap selesai, lalu Q02 (kritis)
    # menemukan kunci ganda di curated.pemetaan_posyandu -> batch dikarantina, DWH tidak berubah.
    ganda = out.sort_values("baris_sumber").duplicated(["posyandu_id", "waktu_key", "kelompok_key"], keep="first")
    if ganda.any():
        dibuang = out.loc[ganda[ganda].index]
        ktx.catat("T05", "KRITIS", f"{int(ganda.sum())} sel dari {dibuang.baris_sumber.nunique()} baris sumber berkunci "
                  "posyandu ganda tidak dimuat ke curated (baris pertama dipertahankan); batch akan gagal Q02",
                  "kunjungan_bulanan", kolom="posyandu_id", nilai=sorted(set(map(int, dibuang.baris_sumber))))
        out = out.drop(index=dibuang.index)
    return out


# ---------------------------------------------------------------- alat (T14)
def transform_alat(cur, ktx: Konteks, pencocok: PencocokPosyandu):
    kol_alat = [f"{a}_{k}" for a in ALAT for k in ("std_baik", "std_rusak", "tstd_baik", "tstd_rusak")]
    cur.execute(f"""SELECT baris_sumber, cut_off_asli, kecamatan_asli, puskesmas_asli, kelurahan_asli,
                           nama_posyandu_asli, {', '.join(kol_alat)}, tensimeter_ada_asli, tensimeter_tidak_ada_asli,
                           is_baris_total
                    FROM stg.antrop ORDER BY baris_sumber""")
    kolom = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=kolom)
    if df.empty:
        raise StrukturSumberError("stg.antrop kosong")

    # Bulan snapshot dari sel CUT OFF (mis. ': JUNI 2026')
    cut = rapikan(df.cut_off_asli.dropna().iloc[0] if df.cut_off_asli.notna().any() else None) or ""
    m = re.search(r"([A-Z]+)\s+(\d{4})", cut)
    if not m or m.group(1) not in NAMA_BULAN:
        raise StrukturSumberError(f"bulan CUT OFF Antrop tidak dapat dibaca: {cut!r}")
    waktu_key = int(m.group(2)) * 100 + NAMA_BULAN[m.group(1)]

    total = df[df.is_baris_total]
    data = df[~df.is_baris_total].copy()
    data[["kecamatan_asli", "puskesmas_asli", "kelurahan_asli"]] = \
        data[["kecamatan_asli", "puskesmas_asli", "kelurahan_asli"]].ffill()   # T14: isi dari baris atas

    # Nilai jumlah alat: angka >= 0; '-' -> NULL (tanda kosong di sumber); lainnya -> NULL + PERINGATAN
    nilai = {}
    flag_baris: dict[int, str] = {}
    for c in kol_alat:
        s = data[c]
        num = pd.to_numeric(s, errors="coerce")
        strip = s.where(s.isna(), s.astype(str).str.strip())
        for r_idx in data.index[s.notna() & num.isna()]:
            b = int(data.at[r_idx, "baris_sumber"])
            if strip[r_idx] == "-":
                flag_baris[b] = "nilai '-' di sumber dimuat NULL"
            else:
                ktx.catat("T03", "PERINGATAN", "jumlah alat bukan angka; dimuat NULL", "sheet Antrop", b, c, s[r_idx])
        nilai[c] = num
    n_strip = sum(1 for _ in flag_baris)
    if n_strip:
        ktx.catat("T14", "INFO", f"{n_strip} baris berisi '-' pada kolom jumlah alat; dimuat sebagai NULL",
                  "sheet Antrop", nilai=sorted(flag_baris))

    # Rekonsiliasi dengan baris total (T14)
    if len(total) == 1:
        beda = []
        for c in kol_alat:
            t = pd.to_numeric(total[c], errors="coerce").iloc[0]
            if pd.isna(t) or nilai[c].sum() != t:
                beda.append(f"{c}: Σ={nilai[c].sum():.0f} total={t}")
        ktx.catat("T14", "INFO" if not beda else "PERINGATAN",
                  "rekonsiliasi 490 baris vs baris total: " + ("selisih 0 di 20 kolom" if not beda else "; ".join(beda)),
                  "sheet Antrop", int(total.baris_sumber.iloc[0]))
    else:
        ktx.catat("T14", "PERINGATAN", f"jumlah baris total = {len(total)} (harap 1); rekonsiliasi dilewati", "sheet Antrop")

    # Tensimeter: tidak dimuat ke fakta (T14)
    ada = data.tensimeter_ada_asli.notna().sum()
    tidak = pd.to_numeric(data.tensimeter_tidak_ada_asli, errors="coerce").fillna(0)
    ktx.catat("T14", "INFO", f"tensimeter tidak dimuat ke fakta (data tidak tersedia): kolom 'Ada' terisi {ada} baris, "
              f"kolom 'Tdk Ada' bernilai bukan 0 pada {(tidak != 0).sum()} baris", "sheet Antrop", kolom="tensimeter")

    hasil_cocok = pencocok.cocokkan_semua("antrop", list(zip(data.baris_sumber.astype(int), data.kelurahan_asli,
                                                             data.nama_posyandu_asli)))
    pemetaan, rows = [], []
    for r in data.itertuples():
        b = int(r.baris_sumber)
        h = hasil_cocok[b]
        pemetaan.append({"sumber": "antrop", "baris_sumber": b, "kelurahan_asli": r.kelurahan_asli,
                         "nama_posyandu_asli": r.nama_posyandu_asli, "kunci_cocok": h.kunci,
                         "posyandu_id": h.posyandu_id, "cara": h.cara, "skor": h.skor, "keterangan": h.keterangan})
        if h.cara == "fuzzy":
            ktx.catat("T14", "PERINGATAN", f"nama posyandu dicocokkan fuzzy (skor {h.skor}); tinjau dan tambahkan ke "
                      "etl/pemetaan_posyandu_manual.csv", "sheet Antrop", b, "NAMA POSYANDU", h.posyandu_id)
        if h.posyandu_id is None:
            ktx.catat("T14", "PERINGATAN", f"posyandu tidak terpetakan, ditahan (tidak masuk fakta): {h.keterangan}",
                      "sheet Antrop", b, "NAMA POSYANDU", r.nama_posyandu_asli)
            continue
        for i, a in enumerate(ALAT):
            for j, std in enumerate(("std", "tstd")):
                rows.append({"posyandu_id": h.posyandu_id, "alat_key": 2 * i + j + 1, "waktu_key": waktu_key,
                             "jumlah_baik": nilai[f"{a}_{std}_baik"][r.Index],
                             "jumlah_rusak": nilai[f"{a}_{std}_rusak"][r.Index],
                             "flag_kualitas": flag_baris.get(b), "baris_sumber": b})
    out = pd.DataFrame(rows)
    ganda = out.duplicated(["posyandu_id", "alat_key"])
    if ganda.any():
        raise StrukturSumberError(f"Antrop: {int(ganda.sum() / 10)} posyandu terpetakan lebih dari sekali")
    cara = Counter(p["cara"] for p in pemetaan)
    ktx.catat("T14", "INFO", f"pemetaan nama posyandu Antrop: {dict(cara)}; snapshot waktu_key {waktu_key}",
              "sheet Antrop", nilai=len(data))
    ktx.ringkas["alat"] = {"baris": len(out), "pemetaan": dict(cara), "waktu_key": waktu_key}
    return out, pemetaan


# ---------------------------------------------------------------- kader (T12, T13)
PENDIDIKAN = {"TIDAK SEKOLAH": "Tidak sekolah", "SD/SEDERAJAT": "SD/sederajat", "SMP/SEDERAJAT": "SMP/sederajat",
              "SMA/SEDERAJAT": "SMA/sederajat", "D3": "D3", "S1": "S1", "S2": "S2"}
JENIS_KELAMIN = {"PEREMPUAN": "P", "P": "P", "LAKI-LAKI": "L", "LAKI LAKI": "L", "L": "L"}
YA_TIDAK = {"YA": True, "TIDAK": False}


def kelompok_umur_kader(usia) -> str | None:
    if usia is None or pd.isna(usia):
        return None
    u = int(usia)
    if u < 30:
        return "<30"
    if u >= 60:
        return ">=60"
    lo = u // 10 * 10
    return f"{lo}-{lo + 9}"


def transform_kader(cur, ktx: Konteks, pencocok: PencocokPosyandu):
    cur.execute("""SELECT baris_sumber, kelurahan_asli, nama_posyandu_asli, kader_kode, usia_asli, jenis_kelamin_asli,
                          pendidikan_asli, kepemilikan_jkn_asli, tingkatan_kader_asli, pelatihan_25_asli,
                          penilaian_25_asli FROM stg.kader ORDER BY baris_sumber""")
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description], dtype=object)
    hasil_cocok = pencocok.cocokkan_semua("kader", list(zip(df.baris_sumber.astype(int), df.kelurahan_asli,
                                                            df.nama_posyandu_asli)))
    df["posyandu_id"] = pd.Series([hasil_cocok[int(b)].posyandu_id for b in df.baris_sumber], index=df.index, dtype=object)
    pemetaan = [{"sumber": "kader", "baris_sumber": int(r.baris_sumber), "kelurahan_asli": r.kelurahan_asli,
                 "nama_posyandu_asli": r.nama_posyandu_asli, "kunci_cocok": hasil_cocok[int(r.baris_sumber)].kunci,
                 "posyandu_id": r.posyandu_id, "cara": hasil_cocok[int(r.baris_sumber)].cara,
                 "skor": hasil_cocok[int(r.baris_sumber)].skor, "keterangan": hasil_cocok[int(r.baris_sumber)].keterangan}
                for r in df.itertuples()]

    # T12: pasangan kader-posyandu ganda (kode sama di posyandu sama). Kode bukan identitas orang (K19).
    ada_pos = df[df.posyandu_id.notna() & df.kader_kode.notna()]
    n_pasangan = ada_pos.groupby(["kader_kode", "posyandu_id"]).size()
    ganda = set(n_pasangan[n_pasangan > 1].index)
    df["is_pasangan_ganda"] = [(k, p) in ganda for k, p in zip(df.kader_kode, df.posyandu_id)]

    rows, dihapus_t13, tak_terpetakan = [], 0, 0
    tdk_valid = Counter()
    for r in df.itertuples():
        b = int(r.baris_sumber)
        jk = JENIS_KELAMIN.get(rapikan(r.jenis_kelamin_asli) or "")
        if jk is None:  # T13
            dihapus_t13 += 1
            ktx.catat("T13", "PERINGATAN", "jenis kelamin tidak tersedia; baris dihapus dari fakta kader",
                      "sheet Kader", b, "Jenis Kelamin",
                      f"kader_kode={r.kader_kode}; posyandu={r.posyandu_id}; nilai={r.jenis_kelamin_asli}")
            continue
        if r.posyandu_id is None:
            tak_terpetakan += 1
            ktx.catat("T14", "PERINGATAN", "posyandu kader tidak terpetakan; baris ditahan (tidak masuk fakta): "
                      f"{hasil_cocok[b].keterangan}", "sheet Kader", b, "Nama Posyandu", r.nama_posyandu_asli)
            continue
        if r.kader_kode is None:
            ktx.catat("T12", "PERINGATAN", "kader_kode kosong (nama kosong); baris ditahan", "sheet Kader", b)
            continue
        pend_raw = (rapikan(r.pendidikan_asli) or "").replace("S 1", "S1").replace("S 2", "S2")
        pend = PENDIDIKAN.get(pend_raw)
        if pend is None and pend_raw:
            pend = "Tidak valid"
            tdk_valid["pendidikan"] += 1
            ktx.catat("T12", "PERINGATAN", "pendidikan tidak dikenali; diseragamkan menjadi 'Tidak valid'",
                      "sheet Kader", b, "Pendidikan", r.pendidikan_asli)
        pen = rapikan(r.penilaian_25_asli)
        penilaian = {"YA": "Ya", "TIDAK": "Tidak"}.get(pen or "", "Tidak valid")
        if penilaian == "Tidak valid":
            tdk_valid["penilaian"] += 1
            ktx.catat("T12", "INFO", "penilaian 25 keterampilan di luar Ya/Tidak -> 'Tidak valid'", "sheet Kader",
                      b, "Penilaian 25 Keterampilan", r.penilaian_25_asli)
        try:
            usia = _angka(r.usia_asli)
        except ValueError:
            usia = None
            ktx.catat("T12", "PERINGATAN", "usia bukan angka; kelompok umur NULL", "sheet Kader", b, "Usia")
        tingkat = rapikan(r.tingkatan_kader_asli)
        rows.append({
            "baris_sumber": b, "waktu_key": KADER_WAKTU_KEY, "periode_snapshot": KADER_PERIODE,
            "kader_kode": r.kader_kode, "posyandu_id": r.posyandu_id, "kelompok_umur_kader": kelompok_umur_kader(usia),
            "jenis_kelamin": jk, "pendidikan": pend, "kepemilikan_jkn": YA_TIDAK.get(rapikan(r.kepemilikan_jkn_asli) or ""),
            "tingkatan_kader": tingkat.capitalize() if tingkat else None,
            "pelatihan_25": YA_TIDAK.get(rapikan(r.pelatihan_25_asli) or ""), "penilaian_25": penilaian,
            "is_pasangan_ganda": bool(r.is_pasangan_ganda),
        })
    out = pd.DataFrame(rows)
    ktx.catat("T12", "INFO", f"pasangan kader-posyandu ganda: {len(ganda)} pasangan, "
              f"{int(df.is_pasangan_ganda.sum())} baris; tidak valid: {dict(tdk_valid)}", "sheet Kader")
    ktx.catat("T13", "INFO", f"staging {len(df)} = fakta {len(out)} + dihapus T13 {dihapus_t13} + "
              f"tidak terpetakan {tak_terpetakan} + lainnya {len(df) - len(out) - dihapus_t13 - tak_terpetakan}",
              "sheet Kader", nilai=len(out))
    ktx.ringkas["kader"] = {"staging": len(df), "fakta": len(out), "t13": dihapus_t13, "tak_terpetakan": tak_terpetakan,
                            "posyandu": int(out.posyandu_id.nunique()), "pasangan_ganda": len(ganda)}
    return out, pemetaan


# ---------------------------------------------------------------- penduduk E1 (T16, T17, K16)
def transform_penduduk(cur, ktx: Konteks, kelurahan: pd.DataFrame) -> pd.DataFrame:
    cur.execute("""SELECT halaman, nomor_tabel_buku, wilayah_asli, jenis_baris, kelompok_umur_asli, urutan_dalam_blok,
                          laki_jiwa_asli, perempuan_jiwa_asli, jumlah_asli, batch_id
                   FROM stg.penduduk_kelompok_umur WHERE tingkat = 'kelurahan'
                   ORDER BY halaman, tabel_halaman, baris_tabel""")
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    if df.empty:
        raise StrukturSumberError("stg.penduduk_kelompok_umur tidak berisi baris kelurahan")
    resmi = set(kelurahan.nama_kelurahan)
    rows = []
    # Hanya blok kelurahan (punya baris data); baris total kecamatan (jenis total_kecamatan) dilewati.
    df = df[df.jenis_baris.isin(["data", "total_wilayah"])]
    for wil, g in df.groupby("wilayah_asli", sort=False):
        kel = kelurahan_resmi(wil)
        if kel not in resmi:
            ktx.catat("T16", "PERINGATAN", "kelurahan E1 tidak terpetakan ke daftar resmi; ditahan", "E1", kolom="kelurahan",
                      nilai=wil)
            continue
        data = g[g.jenis_baris == "data"].sort_values("urutan_dalam_blok")
        total = g[g.jenis_baris == "total_wilayah"]
        L = data.laki_jiwa_asli.map(_angka_buku)
        P = data.perempuan_jiwa_asli.map(_angka_buku)
        J = data.jumlah_asli.map(_angka_buku)
        label_standar = data.kelompok_umur_asli.isin(URUTAN_UMUR)
        # L + P = Jumlah setiap baris (berlaku untuk semua kelurahan)
        salah = data[(L + P) != J]
        for r in salah.itertuples():
            ktx.catat("T17", "PERINGATAN", "L + P ≠ Jumlah pada baris kelompok umur", f"E1 hal. {r.halaman}",
                      r.halaman, "jumlah", f"{kel} {r.kelompok_umur_asli}")
        # Rekonsiliasi dengan baris total kelurahan
        tot_j = _angka_buku_aman(total.jumlah_asli.iloc[0]) if len(total) == 1 else None
        tot_l = _angka_buku_aman(total.laki_jiwa_asli.iloc[0]) if len(total) == 1 else None
        if tot_j != J.sum():
            ktx.catat("T17", "PERINGATAN",
                      f"baris Total kelurahan di buku tidak cocok dengan Σ 16 kelompok umur (Σ L={L.sum()}, Σ P={P.sum()}, "
                      f"Σ jumlah={J.sum()}; baris Total: L={total.laki_jiwa_asli.iloc[0] if len(total) else None}, "
                      f"P={total.perempuan_jiwa_asli.iloc[0] if len(total) else None}, "
                      f"jumlah={total.jumlah_asli.iloc[0] if len(total) else None}); fakta memakai 16 baris data",
                      f"E1 hal. {g.halaman.iloc[0]}", int(g.halaman.iloc[-1]), "Total", kel)

        flag, dari_urutan = None, False
        if not label_standar.all():
            # K16: label umur rusak (Kelayan Tengah). Diterima HANYA bila: tepat 16 baris berurutan 1..16,
            # L+P=Jumlah tiap baris, Σ jumlah = total kelurahan. Q08 diperiksa di quality gate.
            syarat = {
                "tepat 16 baris": len(data) == 16 and list(data.urutan_dalam_blok) == list(range(1, 17)),
                "L+P=Jumlah tiap baris": bool(((L + P) == J).all()),
                "Σ = total kelurahan": bool(tot_j is not None and tot_j == J.sum() and tot_l == L.sum()),
            }
            if not all(syarat.values()):
                ktx.catat("K16", "PERINGATAN", f"label umur rusak dan validasi gagal {syarat}; kelurahan ditahan "
                          "(tidak masuk fakta penduduk)", "E1", kolom="kelompok umur", nilai=kel)
                continue
            flag, dari_urutan = FLAG_URUTAN, True
            ktx.catat("K16", "PERINGATAN", f"label kelompok umur di buku berisi angka; kelompok umur diambil dari "
                      f"urutan baris; validasi {syarat} lolos", "E1", kolom="kelompok umur", nilai=kel)
        elif list(data.kelompok_umur_asli) != URUTAN_UMUR:
            ktx.catat("T17", "PERINGATAN", "urutan/kelengkapan kelompok umur tidak baku; kelurahan ditahan", "E1",
                      nilai=kel)
            continue
        for (_, r), l, p in zip(data.iterrows(), L, P):
            umur_key = int(r.urutan_dalam_blok) if dari_urutan else URUTAN_UMUR.index(r.kelompok_umur_asli) + 1
            for jk, v in (("L", l), ("P", p)):
                rows.append({"nama_kelurahan": kel, "waktu_key": E1_WAKTU_KEY, "kelompok_umur_key": umur_key,
                             "jenis_kelamin": jk, "jumlah_penduduk": int(v), "is_umur_dari_urutan": dari_urutan,
                             "flag_kualitas": flag, "sumber": E1_SUMBER})

    # Baris total tanpa label 'Total' (Pekauman, hal. 21) sudah dikenali di staging (STG06); dicatat ulang di sini.
    cur.execute("SELECT keterangan FROM dq.dq_log WHERE kode_aturan = 'STG06' AND batch_id = %s",
                (df.batch_id.iloc[0],))
    for (ket,) in cur.fetchall():
        ktx.catat("T17", "INFO", f"baris total kelurahan tanpa label 'Total' di buku (dikenali saat staging): {ket}",
                  "E1")
    out = pd.DataFrame(rows)
    ktx.ringkas["penduduk"] = {"baris": len(out), "kelurahan": int(out.nama_kelurahan.nunique()) if len(out) else 0}
    return out


# ---------------------------------------------------------------- stunting E2 (T16, T18)
def _pisah_catatan(catatan: str | None) -> tuple[str | None, str | None]:
    if not catatan:
        return None, None
    m = re.search(r"1\.\s*(.*?)\s*2\.\s*(.*)", catatan)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return catatan, catatan


def transform_stunting(cur, ktx: Konteks, kelurahan: pd.DataFrame) -> pd.DataFrame:
    cur.execute("""SELECT tahun_data, baris_tabel, jenis_baris, kode_kelurahan_asli, kelurahan_asli, stunting_asli,
                          wasting_asli, underweight_asli, catatan_asli FROM stg.stunting_kelurahan
                   ORDER BY tahun_data, baris_tabel""")
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    kode_resmi = dict(zip(kelurahan.kode_wilayah, kelurahan.nama_kelurahan))
    rows = []
    for th, g in df.groupby("tahun_data"):
        data, total = g[g.jenis_baris == "data"], g[g.jenis_baris == "total"]
        nilai = {c: data[f"{c}_asli"].map(lambda v: _angka_buku(v)) for c in ("stunting", "wasting", "underweight")}
        flag = None
        if len(total) == 1:
            beda = [f"{c}: Σ={nilai[c].sum()} portal={total[f'{c}_asli'].iloc[0]}" for c in nilai
                    if _angka_buku(total[f"{c}_asli"].iloc[0]) != nilai[c].sum()]
            if beda:
                flag = "total portal tidak cocok dengan jumlah baris (T18)"
            ktx.catat("T18", "PERINGATAN" if beda else "INFO", f"rekonsiliasi total {th}: " +
                      ("; ".join(beda) if beda else f"cocok (stunting {nilai['stunting'].sum()})"), f"E2 {th}")
        tgl_stunting, tgl_gizi = _pisah_catatan(data.catatan_asli.iloc[0])
        for (_, r), s, w, u in zip(data.iterrows(), nilai["stunting"], nilai["wasting"], nilai["underweight"]):
            if r.kode_kelurahan_asli not in kode_resmi:
                ktx.catat("Q07", "PERINGATAN", "kode kelurahan E2 tidak ada di daftar resmi; ditahan", f"E2 {th}",
                          int(r.baris_tabel), "kode", r.kode_kelurahan_asli)
                continue
            if kode_resmi[r.kode_kelurahan_asli] != kelurahan_resmi(r.kelurahan_asli):
                ktx.catat("T16", "PERINGATAN", f"nama kelurahan berbeda untuk kode yang sama", f"E2 {th}",
                          int(r.baris_tabel), "kelurahan", r.kelurahan_asli)
            rows.append({"kode_wilayah": r.kode_kelurahan_asli, "waktu_key": int(th) * 100 + 12, "stunting": s,
                         "wasting": w, "underweight": u, "sumber_data": f"Dinkes Kota Banjarmasin (Satu Data) {th}",
                         "tanggal_data_stunting": tgl_stunting, "tanggal_data_gizi": tgl_gizi, "flag_kualitas": flag})
    out = pd.DataFrame(rows)
    ktx.ringkas["stunting"] = out.groupby("waktu_key").stunting.sum().to_dict()
    return out


# ---------------------------------------------------------------- tulis curated
URUTAN_HAPUS = ["curated.kunjungan_bulanan", "curated.alat_snapshot", "curated.kader_snapshot",
                "curated.pemetaan_posyandu", "curated.posyandu_versi", "curated.penduduk_kelurahan",
                "curated.stunting_kelurahan", "curated.kelurahan"]


def _sisip(cur, tabel: str, df: pd.DataFrame, batch_id: str) -> int:
    if df.empty:
        return 0
    d = df.astype(object).where(df.notna(), None)
    kolom = list(d.columns) + ["batch_id"]
    nilai = [tuple(_py(v) for v in row) + (batch_id,) for row in d.itertuples(index=False, name=None)]
    execute_values(cur, f"INSERT INTO {tabel} ({', '.join(kolom)}) VALUES %s", nilai, page_size=5000)
    return len(nilai)


def _py(v):
    """numpy/pandas -> tipe Python biasa untuk psycopg2."""
    if v is None:
        return None
    if hasattr(v, "item"):
        return v.item()
    return v


def cek_batch_staging(cur) -> str:
    """Semua tabel staging harus berasal dari satu batch (konsistensi lineage)."""
    batch = set()
    for t in ("kunjungan_bulanan", "antrop", "kader", "penduduk_kelompok_umur", "stunting_kelurahan"):
        cur.execute(f"SELECT DISTINCT batch_id FROM stg.{t}")
        b = [x[0] for x in cur.fetchall()]
        if not b:
            raise StrukturSumberError(f"stg.{t} kosong — jalankan staging dulu")
        batch.update(b)
    if len(batch) != 1:
        raise StrukturSumberError(f"staging berisi lebih dari satu batch {sorted(batch)}; jalankan ulang staging")
    return batch.pop()


def transform(batch_id: str, percobaan_ke: int | None = None) -> dict:
    with catat_task(batch_id, "transform", TAHAP, percobaan_ke) as hasil:
        ktx = Konteks(batch_id)
        conn = buka_koneksi()
        try:
            with conn, conn.cursor() as cur:
                batch_stg = cek_batch_staging(cur)
                kelurahan = transform_kelurahan(cur, ktx)
                versi, pem_lap, katalog, peta_baris = transform_posyandu(cur, ktx, kelurahan)
                pencocok = PencocokPosyandu(katalog)
                if not pencocok.manual:
                    ktx.catat("T14", "PERINGATAN", "berkas etl/pemetaan_posyandu_manual.csv tidak ada/kosong; "
                              "hanya pencocokan kunci + fuzzy yang dipakai", "pemetaan posyandu")
                kunjungan = transform_kunjungan(cur, ktx, peta_baris)
                alat, pem_antrop = transform_alat(cur, ktx, pencocok)
                kader, pem_kader = transform_kader(cur, ktx, pencocok)
                penduduk = transform_penduduk(cur, ktx, kelurahan)
                stunting = transform_stunting(cur, ktx, kelurahan)

                for t in URUTAN_HAPUS:
                    cur.execute(f"DELETE FROM {t}")
                n = {
                    "kelurahan": _sisip(cur, "curated.kelurahan", kelurahan, batch_id),
                    "posyandu_versi": _sisip(cur, "curated.posyandu_versi", versi, batch_id),
                    "pemetaan": _sisip(cur, "curated.pemetaan_posyandu",
                                       pd.DataFrame(pem_lap + pem_antrop + pem_kader), batch_id),
                    "kunjungan": _sisip(cur, "curated.kunjungan_bulanan", kunjungan, batch_id),
                    "alat": _sisip(cur, "curated.alat_snapshot", alat, batch_id),
                    "kader": _sisip(cur, "curated.kader_snapshot", kader, batch_id),
                    "penduduk": _sisip(cur, "curated.penduduk_kelurahan", penduduk, batch_id),
                    "stunting": _sisip(cur, "curated.stunting_kelurahan", stunting, batch_id),
                }
                tulis_dq_log(cur, batch_id, TAHAP, ktx.dq)
        finally:
            conn.close()
        hasil.jumlah_baris = sum(n.values())
        hasil.pesan = f"staging batch {batch_stg}; curated: " + ", ".join(f"{k}={v}" for k, v in n.items())
        ktx.ringkas["curated"] = n
        return ktx.ringkas
