"""Staging E1: buku Data Agregat Kependudukan Disdukcapil Semester II 2025 (PDF) -> stg.

Tabel yang diambil:
  Tabel 6      penduduk kota menurut kelompok umur (total kota, mis. 0–4 = 39.805)
  Tabel 7      penduduk per kecamatan menurut kelompok umur (kontrol Q08)
  Tabel 8–12   penduduk per kelurahan menurut kelompok umur, satu tabel per kecamatan

Kenapa ekstraksi TABEL (pdfplumber.extract_tables), bukan teks biasa: ekstraksi teks
menggeser kolom (angka kelurahan berikutnya tercampur). extract_tables memberi sel per sel.

Cara parse setiap baris tabel (sel kosong dibuang):
  [kode?] [nama wilayah?] <kelompok umur> <angka...>   -> baris data
  <label ... Total/JUMLAH> <angka...>                   -> baris total
Nama wilayah ditulis hanya di baris pertama (0–4) dan dapat terpotong ke baris berikutnya
(mis. 'BANJARMASIN' / 'SELATAN'); potongan digabung, lalu nama final diterapkan ke seluruh
baris blok tersebut. Konteks nomor tabel dibaca dari judul ('8. Jumlah Penduduk ...') dan
berlanjut ke halaman berikutnya sampai judul tabel lain ditemukan.
"""

from __future__ import annotations

import io
import logging
import re
from collections import defaultdict

import pdfplumber

from common.batch import catat_task
from common.errors import StrukturSumberError
from etl.staging_util import ambil_sumber, muat_ulang

log = logging.getLogger(__name__)

SUMBER = "e1_disdukcapil_agregat_2025s2"
HALAMAN_CARI = range(17, 40)  # rentang halaman yang dipindai (tabel 6–12 ada di hal. 19–34)
TABEL_DIPAKAI = {6: "kota", 7: "kecamatan", 8: "kelurahan", 9: "kelurahan", 10: "kelurahan", 11: "kelurahan", 12: "kelurahan"}
# Kecamatan untuk Tabel 8–12 (Bab buku); divalidasi terhadap label baris "Banjarmasin X Total".
KECAMATAN_TABEL = {8: "BANJARMASIN SELATAN", 9: "BANJARMASIN TIMUR", 10: "BANJARMASIN BARAT",
                   11: "BANJARMASIN UTARA", 12: "BANJARMASIN TENGAH"}

# Judul tabel buku: nomor, titik, spasi, lalu huruf (mis. '8. Jumlah Penduduk ...', '13. Umur Median ...').
# Angka seperti '1.273' tidak cocok karena setelah titik bukan spasi+huruf.
RE_JUDUL = re.compile(r"^\s*(\d{1,2})\.\s+[A-Za-z]")
RE_UMUR = re.compile(r"^(\d{1,2}\s*-\s*\d{1,2}|>=\s*75)$")
RE_KODE = re.compile(r"^\d{2}\.\d{2}\.\d{2}$")
RE_ANGKA = re.compile(r"^[\d.,]+$")

KOLOM_STG = [
    "batch_id", "manifest_id", "halaman", "tabel_halaman", "baris_tabel", "nomor_tabel_buku", "tingkat",
    "kode_wilayah_asli", "kecamatan_asli", "wilayah_asli", "jenis_baris", "kelompok_umur_asli",
    "laki_jiwa_asli", "laki_persen_asli", "perempuan_jiwa_asli", "perempuan_persen_asli",
    "jumlah_asli", "jumlah_persen_asli", "urutan_dalam_blok",
]
URUTAN_UMUR = ["0-4", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49",
               "50-54", "55-59", "60-64", "65-69", "70-74", ">=75"]


def _sel(row) -> list[str]:
    return [" ".join(str(c).split()) for c in row if c not in (None, "") and str(c).strip()]


def _angka(sel: list[str], n_harap: int, lokasi: str) -> list[str | None]:
    if len(sel) != n_harap or not all(RE_ANGKA.match(s) for s in sel):
        raise StrukturSumberError(f"{lokasi}: diharapkan {n_harap} angka, didapat {sel}")
    # Tabel 7–12 tidak punya kolom % jumlah -> diisi None
    return sel + [None] * (6 - n_harap)


def parse_pdf(isi: bytes) -> tuple[list[dict], list[dict]]:
    """Mengembalikan (baris staging sebagai dict, catatan dq)."""
    baris: list[dict] = []
    dq: list[dict] = []
    tabel_aktif: int | None = None
    blok: list[dict] = []      # baris data milik wilayah yang sedang dibaca
    nama_blok: list[str] = []  # potongan nama wilayah
    kode_blok: str | None = None
    selesai = False

    def tutup_blok():
        nama = " ".join(dict.fromkeys(nama_blok))  # gabung potongan, buang duplikat berurutan
        for b in blok:
            b["wilayah_asli"] = nama or None
            b["kode_wilayah_asli"] = kode_blok
        return nama

    with pdfplumber.open(io.BytesIO(isi)) as pdf:
        if len(pdf.pages) < max(HALAMAN_CARI):
            raise StrukturSumberError(f"PDF hanya {len(pdf.pages)} halaman; tabel 6–12 tidak mungkin ada")
        for no_hal in HALAMAN_CARI:
            if selesai:
                break
            for t_idx, tabel in enumerate(pdf.pages[no_hal - 1].extract_tables()):
                for r_idx, row in enumerate(tabel):
                    sel = _sel(row)
                    if not sel:
                        continue
                    lokasi = f"hal. {no_hal} tabel {t_idx} baris {r_idx}"
                    m = RE_JUDUL.match(sel[0])
                    if m:
                        nomor = int(m.group(1))
                        if blok:
                            raise StrukturSumberError(f"{lokasi}: judul tabel baru sebelum baris Total wilayah sebelumnya")
                        if nomor not in TABEL_DIPAKAI:
                            if tabel_aktif is not None and nomor > max(TABEL_DIPAKAI):
                                selesai = True
                            tabel_aktif = None
                        else:
                            tabel_aktif = nomor
                        continue
                    if tabel_aktif is None:
                        continue
                    tingkat = TABEL_DIPAKAI[tabel_aktif]
                    n_angka = 6 if tabel_aktif == 6 else 5
                    dasar = {"halaman": no_hal, "tabel_halaman": t_idx, "baris_tabel": r_idx,
                             "nomor_tabel_buku": tabel_aktif, "tingkat": tingkat,
                             "kecamatan_asli": KECAMATAN_TABEL.get(tabel_aktif)}

                    i_umur = next((i for i, s in enumerate(sel) if RE_UMUR.match(s)), None)
                    umur_standar = i_umur is not None
                    # Label kelompok umur TIDAK standar: kolom umur berisi angka (temuan: blok Kelayan
                    # Tengah hal. 22 di buku sumber). Dikenali dari n+1 angka di ujung baris.
                    # Nilai asli disimpan apa adanya; pemetaan ke kelompok umur diputuskan di transform
                    # memakai urutan_dalam_blok.
                    if (not umur_standar and tingkat != "kota" and len(sel) >= n_angka + 1
                            and all(RE_ANGKA.match(s) for s in sel[-(n_angka + 1):])
                            and not (len(sel) > n_angka + 1 and sel[0].casefold().endswith("total"))):
                        i_umur = len(sel) - (n_angka + 1)
                        dq.append({"kode_aturan": "STG07", "tingkat": "PERINGATAN", "objek": f"E1 tabel {tabel_aktif}",
                                   "baris_sumber": no_hal, "kolom": "kelompok umur", "nilai": sel[i_umur],
                                   "keterangan": f"{lokasi}: label kelompok umur bukan format umur (kesalahan buku "
                                                 "sumber); disimpan apa adanya, urutan dalam blok dicatat"})
                    if i_umur is not None:
                        awalan, angka = sel[:i_umur], sel[i_umur + 1:]
                        umur = sel[i_umur].replace(" ", "")
                        if umur == "0-4" and blok:
                            raise StrukturSumberError(f"{lokasi}: blok wilayah baru dimulai sebelum baris Total")
                        if not blok:  # baris pertama blok wilayah baru
                            nama_blok, kode_blok = [], None
                        for a in awalan:
                            if RE_KODE.match(a):
                                kode_blok = a
                            else:
                                nama_blok.append(a)
                        v = _angka(angka, n_angka, lokasi)
                        blok.append({**dasar, "jenis_baris": "data", "kelompok_umur_asli": umur,
                                     "urutan_dalam_blok": len(blok) + 1,
                                     "laki_jiwa_asli": v[0], "laki_persen_asli": v[1],
                                     "perempuan_jiwa_asli": v[2], "perempuan_persen_asli": v[3],
                                     "jumlah_asli": v[4], "jumlah_persen_asli": v[5]})
                        continue

                    # Baris total TANPA label (mis. dasar hal. 21: total Pekauman): hanya diterima bila
                    # isinya tepat n angka dan muncul langsung setelah baris '>=75' dalam satu blok.
                    if (len(sel) == n_angka and all(RE_ANGKA.match(s) for s in sel) and blok
                            and blok[-1]["kelompok_umur_asli"] == ">=75" and tingkat != "kota"):
                        sel = ["Total"] + sel
                        dq.append({"kode_aturan": "STG06", "tingkat": "INFO", "objek": f"E1 tabel {tabel_aktif}",
                                   "baris_sumber": no_hal, "kolom": "label",
                                   "keterangan": f"{lokasi}: baris total tanpa label 'Total' setelah baris >=75; "
                                                 "diperlakukan sebagai total wilayah"})

                    label = sel[0]
                    if label.casefold().endswith("total") or label.casefold() == "jumlah":
                        v = _angka(sel[1:], n_angka, lokasi)
                        nilai = {"laki_jiwa_asli": v[0], "laki_persen_asli": v[1], "perempuan_jiwa_asli": v[2],
                                 "perempuan_persen_asli": v[3], "jumlah_asli": v[4], "jumlah_persen_asli": v[5]}
                        lab = label.casefold()
                        if lab.startswith("kota") or (tabel_aktif == 6 and lab == "jumlah"):
                            jenis, wilayah = "total_kota", "KOTA BANJARMASIN"
                        elif lab == "total":
                            jenis = "total_wilayah"
                            wilayah = None
                        else:  # 'Banjarmasin Selatan Total' / 'BANJARMASIN SELATAN Total'
                            nama_kec = " ".join(label.split()[:-1]).upper()
                            if tabel_aktif == 7:
                                jenis, wilayah = "total_wilayah", nama_kec
                            else:
                                jenis, wilayah = "total_kecamatan", nama_kec
                                if nama_kec != KECAMATAN_TABEL[tabel_aktif]:
                                    raise StrukturSumberError(
                                        f"{lokasi}: total kecamatan '{nama_kec}' tidak sesuai tabel {tabel_aktif}")
                        if jenis == "total_wilayah":
                            nama = tutup_blok()
                            if tabel_aktif == 7 and wilayah != nama:
                                raise StrukturSumberError(f"{lokasi}: label total '{wilayah}' ≠ nama blok '{nama}'")
                            total = {**dasar, "jenis_baris": jenis, "kelompok_umur_asli": None,
                                     "wilayah_asli": nama, "kode_wilayah_asli": kode_blok, **nilai}
                            baris.extend(blok + [total])
                            blok, nama_blok, kode_blok = [], [], None
                        else:
                            if blok and tabel_aktif == 6:  # tabel 6: blok kota ditutup oleh JUMLAH
                                for b in blok:
                                    b["wilayah_asli"] = wilayah
                                baris.extend(blok)
                                blok = []
                            elif blok:
                                raise StrukturSumberError(f"{lokasi}: baris {label} muncul sebelum Total wilayah")
                            baris.append({**dasar, "jenis_baris": jenis, "kelompok_umur_asli": None,
                                          "wilayah_asli": wilayah, "kode_wilayah_asli": None, **nilai})
                        continue
                    # baris lain (header kolom, potongan judul) diabaikan
    if blok:
        raise StrukturSumberError("dokumen berakhir sebelum baris Total wilayah terakhir")
    terlihat = {b["nomor_tabel_buku"] for b in baris}
    if terlihat != set(TABEL_DIPAKAI):
        raise StrukturSumberError(f"tabel yang terbaca {sorted(terlihat)} ≠ yang diharapkan {sorted(TABEL_DIPAKAI)}")
    return baris, dq


def ringkasan_kelengkapan(baris: list[dict]) -> list[dict]:
    """Pemeriksaan bentuk (bukan quality gate): tiap wilayah 16 kelompok umur + 1 total,
    dan label umur standar berada pada urutan yang benar (0-4, 5-9, ..., >=75)."""
    dq = []
    hitung = defaultdict(lambda: [0, 0])
    for b in baris:
        if b["jenis_baris"] == "data" and RE_UMUR.match(b["kelompok_umur_asli"]):
            u = b.get("urutan_dalam_blok")
            if u is not None and (u > 16 or URUTAN_UMUR[u - 1] != b["kelompok_umur_asli"]):
                dq.append({"kode_aturan": "STG04", "tingkat": "PERINGATAN", "objek": f"E1 tabel {b['nomor_tabel_buku']}",
                           "kolom": "kelompok umur", "nilai": b["kelompok_umur_asli"],
                           "keterangan": f"{b['wilayah_asli']}: label umur pada urutan ke-{u} tidak sesuai urutan baku"})
        if b["jenis_baris"] in ("data", "total_wilayah") and b["tingkat"] != "kota":
            k = (b["nomor_tabel_buku"], b["wilayah_asli"])
            hitung[k][0 if b["jenis_baris"] == "data" else 1] += 1
    for (tabel, wilayah), (n_data, n_total) in sorted(hitung.items(), key=lambda x: (x[0][0], x[0][1] or "")):
        if n_data != 16 or n_total != 1:
            dq.append({"kode_aturan": "STG04", "tingkat": "PERINGATAN", "objek": f"E1 tabel {tabel}",
                       "kolom": "wilayah", "nilai": wilayah,
                       "keterangan": f"{n_data} baris kelompok umur dan {n_total} baris total (harap 16 dan 1)"})
    per_tabel = defaultdict(set)
    for tabel, wilayah in hitung:
        per_tabel[tabel].add(wilayah)
    dq.append({"kode_aturan": "STG00", "tingkat": "INFO", "objek": "E1 tabel 6-12",
               "nilai": str(len(baris)),
               "keterangan": "jumlah wilayah per tabel: " + ", ".join(f"T{t}={len(w)}" for t, w in sorted(per_tabel.items()))})
    return dq


def staging_penduduk(batch_id: str, percobaan_ke: int | None = None) -> int:
    with catat_task(batch_id, "staging_pdf", "staging", percobaan_ke) as hasil:
        obj = ambil_sumber(batch_id, SUMBER)
        baris, dq = parse_pdf(obj.isi)
        dq += ringkasan_kelengkapan(baris)
        rows = [tuple([batch_id, obj.manifest_id] + [b.get(k) for k in KOLOM_STG[2:]]) for b in baris]
        n = muat_ulang("stg.penduduk_kelompok_umur", KOLOM_STG, rows, batch_id, dq)
        n_kel = len({b["wilayah_asli"] for b in baris if b["tingkat"] == "kelurahan" and b["jenis_baris"] == "data"})
        hasil.jumlah_baris = n
        hasil.pesan = f"{n} baris; {n_kel} kelurahan; peringatan bentuk: {sum(d['tingkat'] == 'PERINGATAN' for d in dq)}"
        return n
