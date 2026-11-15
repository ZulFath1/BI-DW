"""Normalisasi nama wilayah & posyandu (T04) dan pembentukan kunci posyandu (T05),
serta pencocokan nama posyandu dari sheet lain (Antrop, Kader) ke kunci tersebut (T14).

Langkah normalisasi nama posyandu (contoh 'PYD NUSA INDAH 02 - SUNGAI BILU', kel. SUNGAI BILU):
  1. rapikan teks: entitas HTML ('&#039;' -> "'"), spasi ganda/tepi, huruf besar      (T04)
  2. tanda baca . , - _ / ( ) -> spasi
  3. buang awalan 'PYD', 'POSYANDU', 'PYD POSYANDU'                                    (T05)
  4. buang akhiran nama kelurahan (termasuk alias & ejaan Sei/Sungai; toleran salah
     ketik ringan, mis. 'TIMUUR')                                                       (T05)
     -> nama_normal = 'NUSA INDAH 02'
  5. kunci_cocok: angka Romawi -> Arab (II -> 2, IIA -> 2A), nol di depan dibuang
     (02 -> 2), tanda petik dibuang, akhiran inisial kelurahan dibuang (mis. 'CEMPAKA KC'
     di Kuin Cerucuk)                                                                   -> 'NUSA INDAH 2'
posyandu_id = '<KELURAHAN>--<KUNCI_COCOK>' dengan spasi -> '_' (mis. 'SUNGAI_BILU--NUSA_INDAH_2').

Teruji pada data 2023–2026: 395/395/487/490 posyandu per tahun, gabungan 490 kunci unik
(sama dengan dokumen UTS), tanpa kunci ganda dalam satu tahun.
"""

from __future__ import annotations

import csv
import difflib
import html
import re
from dataclasses import dataclass
from pathlib import Path

# Alias ejaan kelurahan -> nama resmi (E1/E2). Sumber: dokumen UTS T16 dan temuan data.
ALIAS_KELURAHAN = {
    "BENUA ANYAR": "BANUA ANYAR",
    "SEB. MESJID": "SEBERANG MESJID",
    "SEB MESJID": "SEBERANG MESJID",
    "SEI LULUT": "SUNGAI LULUT",
    "ANTASAN KECIL": "ANTASAN KECIL TIMUR",
}
# Ejaan lain yang mungkin dipakai sebagai akhiran nama posyandu untuk kelurahan resmi tertentu.
AKHIRAN_TAMBAHAN = {
    "BANUA ANYAR": ["BENUA ANYAR"],
    "ANTASAN KECIL TIMUR": ["ANTASAN KECIL"],
    "SEBERANG MESJID": ["SEB MESJID"],
}
# Ejaan puskesmas (T04): disamakan dengan bentuk yang dipakai di laporan.
EJAAN_PUSKESMAS = {"S. PARMAN": "S.PARMAN", "SUNGAI BILU": "SEI BILU", "SUNGAI JINGAH": "SEI JINGAH",
                   "SUNGAI MESA": "SEI MESA", "GEDANG HANYAR": "GADANG HANYAR"}

ROMAWI = {r: i for i, r in enumerate(
    "I II III IV V VI VII VIII IX X XI XII XIII XIV XV XVI XVII XVIII XIX XX".split(), start=1)}
AMBANG_FUZZY = 0.85        # kemiripan minimum pencocokan otomatis tahap 3
SELISIH_FUZZY_MIN = 0.10   # kandidat terbaik harus unggul jelas dari kandidat kedua
FILE_MANUAL = Path(__file__).with_name("pemetaan_posyandu_manual.csv")


def rapikan(teks) -> str | None:
    """T04: entitas HTML diurai, spasi dirapikan, huruf besar. None/kosong -> None."""
    if teks is None:
        return None
    s = " ".join(html.unescape(str(teks)).split()).upper()
    return s or None


def kelurahan_resmi(nama) -> str | None:
    s = rapikan(nama)
    return ALIAS_KELURAHAN.get(s, s) if s else None


def puskesmas_resmi(nama) -> str | None:
    s = rapikan(nama)
    return EJAAN_PUSKESMAS.get(s, s) if s else None


def _varian_kelurahan(kel: str) -> list[str]:
    v = [kel] + AKHIRAN_TAMBAHAN.get(kel, [])
    v += [x.replace("SUNGAI ", "SEI ", 1) for x in v if x.startswith("SUNGAI ")]
    return sorted(set(v), key=len, reverse=True)


def nama_normal(nama_posyandu, kelurahan: str) -> str:
    """Langkah 1–4: nama posyandu tanpa awalan POSYANDU/PYD dan tanpa akhiran nama kelurahan."""
    s = rapikan(nama_posyandu) or ""
    s = " ".join(re.sub(r"[.,\-_/()]", " ", s).split())
    s = re.sub(r"^PYD\s+", "", s)
    s = re.sub(r"^POSYANDU\s+", "", s)
    t = s.split()
    for v in _varian_kelurahan(kelurahan):
        n = len(v.split())
        if len(t) > n and difflib.SequenceMatcher(None, " ".join(t[-n:]), v).ratio() >= 0.9:
            t = t[:-n]
            break
    return " ".join(t)


def _inisial(kel: str) -> str:
    return "".join(w[0] for w in kel.split())


def kunci_cocok(nama_norm: str, kelurahan: str) -> str:
    """Langkah 5: bentuk kanonik untuk membandingkan nama antar-sheet."""
    out = []
    for t in nama_norm.replace("'", "").split():
        m = re.fullmatch(r"([IVX]+)([A-Z]?)", t)
        if m and m.group(1) in ROMAWI:
            t = f"{ROMAWI[m.group(1)]}{m.group(2)}"
        m = re.fullmatch(r"0*(\d+)([A-Z]?)", t)
        if m:
            t = f"{m.group(1)}{m.group(2)}"
        out.append(t)
    kw = kelurahan.split()
    if len(out) > len(kw) and out[-len(kw):] == kw:
        out = out[:-len(kw)]
    if len(out) > 1 and out[-1] == _inisial(kelurahan):
        out = out[:-1]
    return " ".join(out)


def buat_posyandu_id(kelurahan: str, kunci: str) -> str:
    slug = lambda x: re.sub(r"[^A-Z0-9]+", "_", x).strip("_")  # noqa: E731
    return f"{slug(kelurahan)}--{slug(kunci)}"


# ---------------------------------------------------------------- pencocokan antar-sheet
@dataclass
class HasilCocok:
    posyandu_id: str | None
    cara: str                 # kunci | manual | fuzzy | tidak_terpetakan
    skor: float | None
    kunci: str
    keterangan: str | None = None


def baca_pemetaan_manual(path: Path = FILE_MANUAL) -> dict[tuple[str, str, str], tuple[str, str]]:
    """(sumber, kelurahan, kunci_sumber) -> (posyandu_id, keterangan). Berkas ditinjau manual.

    Berkas TIDAK di-commit (memuat nama posyandu dari dataset privat; *.csv ada di .gitignore).
    Bila tidak ada, pencocokan tetap berjalan (kunci + fuzzy) dan baris yang tersisa ditahan."""
    hasil = {}
    if not path.is_file():
        return hasil
    with path.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            hasil[(r["sumber"], r["kelurahan"], r["kunci_sumber"])] = (
                buat_posyandu_id(r["kelurahan_tujuan"], r["kunci_tujuan"]), r["keterangan"])
    return hasil


class PencocokPosyandu:
    """Mencocokkan (kelurahan, nama posyandu) dari Antrop/Kader ke posyandu_id hasil laporan.

    Urutan: (1) kunci_cocok sama persis dalam kelurahan yang sama; (2) tabel pemetaan manual
    yang sudah ditinjau; (3) fuzzy dalam kelurahan yang sama, hanya bila kemiripan >= 0,85,
    kandidat terbaik unggul jelas, dan kandidat belum dipakai baris lain -> diberi PERINGATAN
    untuk ditinjau; (4) selain itu tidak terpetakan (ditahan, tidak masuk fakta).
    """

    def __init__(self, posyandu: dict[str, tuple[str, str]], manual: dict | None = None):
        # posyandu: posyandu_id -> (kelurahan, kunci)
        self.indeks = {(kel, k): pid for pid, (kel, k) in posyandu.items()}
        self.semua_id = set(posyandu)
        self.per_kel: dict[str, list[tuple[str, str]]] = {}
        for pid, (kel, k) in posyandu.items():
            self.per_kel.setdefault(kel, []).append((k, pid))
        self.manual = manual if manual is not None else baca_pemetaan_manual()

    def cocokkan_semua(self, sumber: str, baris: list[tuple[int, str | None, str | None]]) -> dict[int, HasilCocok]:
        """baris: (baris_sumber, kelurahan_asli, nama_posyandu_asli)."""
        hasil: dict[int, HasilCocok] = {}
        sisa = []
        for b, kel_asli, nama_asli in baris:
            kel = kelurahan_resmi(kel_asli)
            if not kel or not rapikan(nama_asli) or kel == "N/A" or rapikan(nama_asli) == "N/A":
                hasil[b] = HasilCocok(None, "tidak_terpetakan", None, "",
                                      "kelurahan/nama posyandu kosong atau 'N/A'")
                continue
            k = kunci_cocok(nama_normal(nama_asli, kel), kel)
            pid = self.indeks.get((kel, k))
            if pid:
                hasil[b] = HasilCocok(pid, "kunci", 1.0, k)
            elif (sumber, kel, k) in self.manual:
                pid, ket = self.manual[(sumber, kel, k)]
                if pid not in self.semua_id:
                    hasil[b] = HasilCocok(None, "tidak_terpetakan", None, k,
                                          f"pemetaan manual menunjuk posyandu_id tak dikenal {pid}")
                else:
                    hasil[b] = HasilCocok(pid, "manual", None, k, ket)
            else:
                sisa.append((b, kel, k))
        dipakai = {(h.posyandu_id) for h in hasil.values() if h.posyandu_id}
        for b, kel, k in sisa:
            kandidat = [(difflib.SequenceMatcher(None, k, kk).ratio(), pid)
                        for kk, pid in self.per_kel.get(kel, []) if pid not in dipakai]
            kandidat.sort(reverse=True)
            if (kandidat and kandidat[0][0] >= AMBANG_FUZZY
                    and (len(kandidat) == 1 or kandidat[1][0] <= kandidat[0][0] - SELISIH_FUZZY_MIN)):
                skor, pid = kandidat[0]
                hasil[b] = HasilCocok(pid, "fuzzy", round(skor, 3), k, "cocok otomatis (fuzzy); perlu ditinjau")
                if sumber == "antrop":  # Antrop: satu baris per posyandu
                    dipakai.add(pid)
            else:
                terbaik = f"; kandidat terdekat {kandidat[0][1]} ({kandidat[0][0]:.2f})" if kandidat else ""
                hasil[b] = HasilCocok(None, "tidak_terpetakan", None, k, "tidak ada pasangan" + terbaik)
        return hasil
