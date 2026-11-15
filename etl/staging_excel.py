"""Staging sumber internal `Laporan Kunjungan Posyandu.xlsx` (Raw -> stg).

Tiga task:
  staging_laporan  sheet 2023–2026 -> stg.kunjungan_bulanan (posyandu × bulan × kelompok)
  staging_antrop   sheet Antrop    -> stg.antrop
  staging_kader    sheet Kader     -> stg.kader (nama dipseudonimkan, alamat tidak dimuat)

Berkas dibaca dari data lake (objek yang tercatat di manifest batch), BUKAN dari folder lokal.

Validasi struktur sebelum memuat: posisi header 3 tingkat (bulan, kelompok, metrik), nama
kolom identitas, dan tidak ada kolom tambahan berisi data. Bila struktur berbeda
(mis. dinas menambah kolom) -> StrukturSumberError, task GAGAL, staging lama tidak berubah.
"""

from __future__ import annotations

import io
import logging
from collections import Counter

import openpyxl
from openpyxl.utils import get_column_letter

from common.batch import catat_task
from common.errors import StrukturSumberError
from common.privasi import ambil_kunci, pseudonim_kader
from etl.staging_util import ambil_sumber, ke_teks, kode_tipe, muat_ulang, norm

log = logging.getLogger(__name__)

SUMBER = "internal_laporan_posyandu"

# ---------------------------------------------------------------- laporan bulanan
SHEET_LAPORAN = ("2023", "2024", "2025", "2026")
IDENTITAS_LAPORAN = ["No.", "Kecamatan", "Kelurahan", "Puskesmas", "Posyandu"]
BULAN = ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus",
         "September", "Oktober", "Nopember", "Desember"]  # ejaan sesuai header sumber
KELOMPOK = ["Ibu Hamil", "Bayi Balita", "Usia Sekolah dan Remaja", "Usia Produktif", "Lansia"]
METRIK = ["Sasaran", "Cakupan", "% Dilayani", "Dirujuk", "% Dirujuk"]
N_ID = len(IDENTITAS_LAPORAN)
N_KOLOM_LAPORAN = N_ID + len(BULAN) * len(KELOMPOK) * len(METRIK)  # 305


def kolom_metrik(bulan_idx: int, kelompok_idx: int, metrik_idx: int = 0) -> int:
    """Indeks kolom (0-based) suatu metrik: j = 5 + 25*bulan + 5*kelompok + metrik."""
    return N_ID + bulan_idx * 25 + kelompok_idx * 5 + metrik_idx


def _baca_sheet(wb, nama: str) -> list[tuple]:
    if nama not in wb.sheetnames:
        raise StrukturSumberError(f"sheet '{nama}' tidak ada (sheet tersedia: {wb.sheetnames})")
    return [tuple(r) for r in wb[nama].iter_rows(values_only=True)]


def _cari_header(rows: list[tuple], label: str, maks: int = 10) -> int:
    for i, r in enumerate(rows[:maks]):
        if r and norm(r[0]) == norm(label):
            return i
    raise StrukturSumberError(f"baris header berlabel '{label}' tidak ditemukan di {maks} baris pertama")


def _ambil(r: tuple, j: int):
    return r[j] if j < len(r) else None


def _kolom_ekstra_terisi(rows: list[tuple], mulai: int, n_kolom: int) -> list[str]:
    """Kolom di luar struktur yang diharapkan tetapi berisi nilai (indikasi format berubah)."""
    terisi = set()
    for r in rows[mulai:]:
        for j in range(n_kolom, len(r)):
            if r[j] is not None:
                terisi.add(get_column_letter(j + 1))
    return sorted(terisi)


def validasi_header_laporan(sheet: str, rows: list[tuple], h: int) -> None:
    r1, r2, r3 = rows[h], rows[h + 1], rows[h + 2]
    galat = []
    if [norm(_ambil(r1, j)) for j in range(N_ID)] != [norm(x) for x in IDENTITAS_LAPORAN]:
        galat.append(f"identitas {[_ambil(r1, j) for j in range(N_ID)]}")
    for b in range(12):
        if norm(_ambil(r1, kolom_metrik(b, 0))) != norm(BULAN[b]):
            galat.append(f"{get_column_letter(kolom_metrik(b, 0) + 1)}: bulan '{_ambil(r1, kolom_metrik(b, 0))}'")
        for k in range(5):
            if norm(_ambil(r2, kolom_metrik(b, k))) != norm(KELOMPOK[k]):
                galat.append(f"{get_column_letter(kolom_metrik(b, k) + 1)}: kelompok '{_ambil(r2, kolom_metrik(b, k))}'")
            for m in range(5):
                j = kolom_metrik(b, k, m)
                if norm(_ambil(r3, j)) != norm(METRIK[m]):
                    galat.append(f"{get_column_letter(j + 1)}: metrik '{_ambil(r3, j)}'")
    ekstra = _kolom_ekstra_terisi(rows, h, N_KOLOM_LAPORAN)
    if ekstra:
        galat.append(f"kolom tambahan berisi data: {ekstra[:5]}")
    if galat:
        raise StrukturSumberError(f"sheet {sheet}: struktur header tidak sesuai ({len(galat)} beda), contoh: {galat[:3]}")


KOLOM_STG_LAPORAN = [
    "batch_id", "manifest_id", "tahun_sheet", "baris_sumber", "kolom_sumber_awal",
    "no_asli", "kecamatan_asli", "kelurahan_asli", "puskesmas_asli", "posyandu_asli",
    "bulan_ke", "kelompok_ke", "sasaran_asli", "cakupan_asli", "persen_dilayani_asli",
    "dirujuk_asli", "persen_dirujuk_asli", "tipe_sel_asli",
]


def staging_laporan(batch_id: str, percobaan_ke: int | None = None) -> int:
    with catat_task(batch_id, "staging_laporan", "staging", percobaan_ke) as hasil:
        obj = ambil_sumber(batch_id, SUMBER)
        wb = openpyxl.load_workbook(io.BytesIO(obj.isi), read_only=True, data_only=True)
        baris_stg: list[tuple] = []
        dq: list[dict] = []
        ringkas = {}
        for sheet in SHEET_LAPORAN:
            rows = _baca_sheet(wb, sheet)
            h = _cari_header(rows, IDENTITAS_LAPORAN[0])
            validasi_header_laporan(sheet, rows, h)
            n_posyandu = 0
            for i in range(h + 3, len(rows)):
                r = rows[i]
                if all(c is None for c in r):
                    continue
                baris_excel = i + 1
                if _ambil(r, 4) is None:
                    dq.append({"kode_aturan": "STG01", "tingkat": "PERINGATAN", "objek": f"sheet {sheet}",
                               "baris_sumber": baris_excel, "kolom": "Posyandu",
                               "keterangan": "baris berisi data tanpa nama posyandu"})
                n_posyandu += 1
                ident = [ke_teks(_ambil(r, j)) for j in range(N_ID)]
                for b in range(12):
                    for k in range(5):
                        j0 = kolom_metrik(b, k)
                        nilai = [_ambil(r, j0 + m) for m in range(5)]
                        baris_stg.append((
                            batch_id, obj.manifest_id, int(sheet), baris_excel, j0 + 1, *ident,
                            b + 1, k + 1, *[ke_teks(v) for v in nilai], "".join(kode_tipe(v) for v in nilai),
                        ))
            ringkas[sheet] = n_posyandu
            dq.append({"kode_aturan": "STG00", "tingkat": "INFO", "objek": f"sheet {sheet}",
                       "nilai": str(n_posyandu),
                       "keterangan": f"header di baris {h + 1}-{h + 3}, data mulai baris {h + 4}; jumlah baris posyandu"})
        wb.close()
        n = muat_ulang("stg.kunjungan_bulanan", KOLOM_STG_LAPORAN, baris_stg, batch_id, dq)
        hasil.jumlah_baris = n
        hasil.pesan = "posyandu per sheet: " + ", ".join(f"{s}={v}" for s, v in ringkas.items())
        return n


# ---------------------------------------------------------------- Antrop
IDENTITAS_ANTROP = ["NO", "KECAMATAN", "PUSKESMAS", "KELURAHAN", "NO", "NAMA POSYANDU"]
ALAT = [("timbangan_bayi", "TIMBANGAN BAYI DIGITAL"), ("panjang_bayi", "PENGUKUR PANJANG BAYI"),
        ("timbangan_badan", "TIMBANGAN BADAN DIGITAL"), ("tinggi_badan", "PENGUKUR TINGGI BADAN"),
        ("lila", "PENGUKUR LILA/LP/LK")]
KONDISI = [("std_baik", "STANDAR", "Baik"), ("std_rusak", None, "Rusak"),
           ("tstd_baik", "TDK STANDAR", "Baik"), ("tstd_rusak", None, "Rusak")]
J_TENSI = 6 + 4 * len(ALAT)  # kolom 26 (0-based)
N_KOLOM_ANTROP = J_TENSI + 2  # 28

KOLOM_STG_ANTROP = (
    ["batch_id", "manifest_id", "baris_sumber", "cut_off_asli", "no_asli", "kecamatan_asli", "puskesmas_asli",
     "kelurahan_asli", "no_kelurahan_asli", "nama_posyandu_asli"]
    + [f"{a}_{k}" for a, _ in ALAT for k, _, _ in KONDISI]
    + ["tensimeter_ada_asli", "tensimeter_tidak_ada_asli", "is_baris_total"]
)


def validasi_header_antrop(rows: list[tuple], h: int) -> None:
    r1, r2, r3 = rows[h], rows[h + 1], rows[h + 2]
    galat = []
    if [norm(_ambil(r1, j)) for j in range(6)] != [norm(x) for x in IDENTITAS_ANTROP]:
        galat.append(f"identitas {[_ambil(r1, j) for j in range(6)]}")
    for a, (_, label) in enumerate(ALAT):
        j = 6 + 4 * a
        if norm(_ambil(r1, j)) != norm(label):
            galat.append(f"{get_column_letter(j + 1)}: alat '{_ambil(r1, j)}'")
        for m, (_, std, kondisi) in enumerate(KONDISI):
            if std is not None and norm(_ambil(r2, j + m)) != norm(std):
                galat.append(f"{get_column_letter(j + m + 1)}: '{_ambil(r2, j + m)}'")
            if norm(_ambil(r3, j + m)) != norm(kondisi):
                galat.append(f"{get_column_letter(j + m + 1)}: '{_ambil(r3, j + m)}'")
    if norm(_ambil(r1, J_TENSI)) != norm("TENSIMETER DIGITAL") or [norm(_ambil(r3, J_TENSI)), norm(_ambil(r3, J_TENSI + 1))] != ["ada", "tdk ada"]:
        galat.append("kolom tensimeter")
    ekstra = _kolom_ekstra_terisi(rows, h, N_KOLOM_ANTROP)
    if ekstra:
        galat.append(f"kolom tambahan berisi data: {ekstra[:5]}")
    if galat:
        raise StrukturSumberError(f"sheet Antrop: struktur header tidak sesuai ({len(galat)} beda), contoh: {galat[:3]}")


def staging_antrop(batch_id: str, percobaan_ke: int | None = None) -> int:
    with catat_task(batch_id, "staging_antrop", "staging", percobaan_ke) as hasil:
        obj = ambil_sumber(batch_id, SUMBER)
        wb = openpyxl.load_workbook(io.BytesIO(obj.isi), read_only=True, data_only=True)
        rows = _baca_sheet(wb, "Antrop")
        wb.close()
        h = _cari_header(rows, "NO")
        validasi_header_antrop(rows, h)
        # Sel "CUT OFF" di kepala sheet (mis. ': JUNI 2026') -> menentukan bulan snapshot di transform.
        cut_off = next((ke_teks(_ambil(r, 1)) for r in rows[:h] if norm(_ambil(r, 0)).startswith("cut off")), None)

        baris_stg, dq = [], []
        n_posyandu = n_total = 0
        for i in range(h + 3, len(rows)):
            r = rows[i]
            if all(c is None for c in r):
                continue
            ada_identitas = any(_ambil(r, j) is not None for j in range(6))
            is_total = not ada_identitas  # baris berangka tanpa label = baris total (T14)
            n_total += is_total
            n_posyandu += not is_total
            baris_stg.append((
                batch_id, obj.manifest_id, i + 1, cut_off,
                *[ke_teks(_ambil(r, j)) for j in range(6)],
                *[ke_teks(_ambil(r, j)) for j in range(6, J_TENSI + 2)],
                is_total,
            ))
            if is_total:
                dq.append({"kode_aturan": "T14", "tingkat": "INFO", "objek": "sheet Antrop", "baris_sumber": i + 1,
                           "keterangan": "baris total tanpa label disimpan dengan is_baris_total=true; "
                                         "tidak diteruskan ke curated/DWH, dipakai untuk rekonsiliasi"})
        if n_total != 1:
            dq.append({"kode_aturan": "T14", "tingkat": "PERINGATAN", "objek": "sheet Antrop", "nilai": str(n_total),
                       "keterangan": "jumlah baris total tanpa label bukan 1; periksa berkas sumber"})
        dq.append({"kode_aturan": "STG00", "tingkat": "INFO", "objek": "sheet Antrop", "nilai": str(n_posyandu),
                   "keterangan": f"header di baris {h + 1}-{h + 3}; jumlah baris posyandu; cut off '{cut_off}'"})
        n = muat_ulang("stg.antrop", KOLOM_STG_ANTROP, baris_stg, batch_id, dq)
        hasil.jumlah_baris = n
        hasil.pesan = f"{n_posyandu} baris posyandu + {n_total} baris total; cut off {cut_off}"
        return n


# ---------------------------------------------------------------- Kader
# Header -> kolom staging. None = TIDAK dimuat (data pribadi / tidak dipakai).
HEADER_KADER = {
    "No": "no_asli",
    "Kecamatan": "kecamatan_asli",
    "Puskesmas": "puskesmas_asli",
    "Nama Posyandu": "nama_posyandu_asli",
    "Kelurahan": "kelurahan_asli",
    "Alamat Posyandu": None,   # bisa berupa alamat rumah kader; tidak dibutuhkan analisis
    "Nama Kader": "__nama__",  # khusus: diubah menjadi kader_kode, nama tidak disimpan
    "Usia": "usia_asli",
    "Jenis Kelamin": "jenis_kelamin_asli",
    "Pendidikan": "pendidikan_asli",
    "Kepemilikan JKN": "kepemilikan_jkn_asli",
    "Alamat Kader": None,      # data pribadi: tidak dimuat (aturan privasi)
    "Tingkatan Kader": "tingkatan_kader_asli",
    "Pelatihan 25 Keterampilan": "pelatihan_25_asli",
    "Penilaian 25 Keterampilan": "penilaian_25_asli",
}
KOLOM_STG_KADER = [
    "batch_id", "manifest_id", "baris_sumber", "no_asli", "kecamatan_asli", "puskesmas_asli",
    "nama_posyandu_asli", "kelurahan_asli", "kader_kode", "usia_asli", "jenis_kelamin_asli",
    "pendidikan_asli", "kepemilikan_jkn_asli", "tingkatan_kader_asli", "pelatihan_25_asli", "penilaian_25_asli",
]


def staging_kader(batch_id: str, percobaan_ke: int | None = None) -> int:
    """PRIVASI: nama kader hanya ada di memori selama pseudonim dibuat; tidak pernah ditulis
    ke database, log, maupun pesan error. Alamat tidak dibaca ke struktur staging."""
    with catat_task(batch_id, "staging_kader", "staging", percobaan_ke) as hasil:
        kunci = ambil_kunci()
        obj = ambil_sumber(batch_id, SUMBER)
        wb = openpyxl.load_workbook(io.BytesIO(obj.isi), read_only=True, data_only=True)
        rows = _baca_sheet(wb, "Kader")
        wb.close()
        h = _cari_header(rows, "No")
        header = rows[h]

        posisi: dict[str, int] = {}
        tanpa_header: list[int] = []
        tak_dikenal: list[str] = []
        for j, label in enumerate(header):
            if label is None:
                tanpa_header.append(j)
                continue
            cocok = next((k for k in HEADER_KADER if norm(k) == norm(label)), None)
            if cocok is None:
                tak_dikenal.append(str(label))
            else:
                posisi[cocok] = j
        hilang = [k for k in HEADER_KADER if k not in posisi]
        if hilang:
            raise StrukturSumberError(f"sheet Kader: kolom wajib tidak ditemukan: {hilang}")

        data_rows = [(i + 1, rows[i]) for i in range(h + 1, len(rows)) if any(c is not None for c in rows[i])]
        dq: list[dict] = []
        # Kolom tanpa header / tak dikenal: tidak dimuat (bisa memuat data pribadi). Dicatat lokasinya saja.
        for j in tanpa_header:
            terisi = [b for b, r in data_rows if _ambil(r, j) is not None]
            if terisi:
                dq.append({"kode_aturan": "K09", "tingkat": "INFO", "objek": "sheet Kader",
                           "kolom": get_column_letter(j + 1), "nilai": str(len(terisi)),
                           "baris_sumber": terisi[0] if len(terisi) == 1 else None,
                           "keterangan": f"kolom tanpa header berisi {len(terisi)} sel; diabaikan, tidak dimuat "
                                         f"(baris: {terisi[:10]})"})
        for label in tak_dikenal:
            dq.append({"kode_aturan": "STG02", "tingkat": "PERINGATAN", "objek": "sheet Kader", "kolom": label,
                       "keterangan": "kolom baru tak dikenal; tidak dimuat sampai diperiksa (mungkin data pribadi)"})

        baris_stg = []
        nama_kosong = 0
        for baris_excel, r in data_rows:
            nilai = {}
            for label, kolom in HEADER_KADER.items():
                if kolom is None or kolom == "__nama__":
                    continue
                nilai[kolom] = ke_teks(_ambil(r, posisi[label]))
            kode = pseudonim_kader(_ambil(r, posisi["Nama Kader"]), kunci)
            if kode is None:
                nama_kosong += 1
                dq.append({"kode_aturan": "STG03", "tingkat": "PERINGATAN", "objek": "sheet Kader",
                           "baris_sumber": baris_excel, "kolom": "Nama Kader",
                           "keterangan": "nama kader kosong; kader_kode NULL"})
            nilai["kader_kode"] = kode
            baris_stg.append(tuple([batch_id, obj.manifest_id, baris_excel] + [nilai[k] for k in KOLOM_STG_KADER[3:]]))

        # Ringkasan tanpa data pribadi: distribusi jenis kelamin (untuk T13/Q09).
        jk = Counter(b[KOLOM_STG_KADER.index("jenis_kelamin_asli")] for b in baris_stg)
        dq.append({"kode_aturan": "STG00", "tingkat": "INFO", "objek": "sheet Kader", "nilai": str(len(baris_stg)),
                   "keterangan": f"header di baris {h + 1}; jumlah baris kader; jenis kelamin: {dict(jk)}; "
                                 "alamat kader & alamat posyandu tidak dimuat; nama dipseudonimkan"})
        n = muat_ulang("stg.kader", KOLOM_STG_KADER, baris_stg, batch_id, dq)
        hasil.jumlah_baris = n
        hasil.pesan = f"{n} kader; nama kosong {nama_kosong}; kolom tanpa header berisi: {len([d for d in dq if d['kode_aturan'] == 'K09'])}"
        return n
