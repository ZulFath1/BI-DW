"""Task train_ml: clustering posyandu (baseline aturan -> K-Means/Agglomerative -> perbandingan -> model terpilih).

Data: mart.posyandu_profil (dibangun dari DWH oleh build_mart), hanya posyandu yang punya data 2025
(is_layak_ml, 487 posyandu). Tujuan bisnis: mengelompokkan posyandu menurut kinerja dan sumber dayanya
agar Dinkes dapat menentukan posyandu prioritas pembinaan. Tidak ada label "benar" -> unsupervised.

Fitur dasar:
    persen_ibu_hamil_2025     cakupan ibu hamil (Σ cakupan ÷ Σ sasaran, filter KPI K38)
    persen_bayi_balita_2025   cakupan bayi balita
    kelengkapan_2025          bulan terlapor ÷ 12
    persen_anomali_2025       % baris Terlapor yang ditandai anomali (kualitas pencatatan)
    rasio_alat_baik           jenis alat antropometri standar & baik ÷ 5 (snapshot Juni 2026)
    jumlah_kader              baris kader (snapshot 2026)
    persen_kader_terlatih     % kader dengan pelatihan 25 keterampilan
    rasio_stunting_kelurahan_2025   (eksternal) stunting ÷ penduduk 0–4 tahun kelurahan

Tiga set fitur (tahap Baseline -> Improvement):
    v1_internal            7 fitur internal apa adanya (setelah clip + imputasi + standardisasi)
    v2_internal            rekayasa fitur: kelengkapan_2025 DIBUANG (97,5% posyandu = 100%, hampir
                           konstan sehingga 12 posyandu sisanya menjadi pencilan ekstrem setelah
                           standardisasi); persen_anomali di-log1p (median 0, p95 25%, sangat menceng)
    v2_internal+eksternal  v2 + rasio stunting kelurahan (menguji apakah data eksternal menambah struktur)

Preprocessing umum: persen dipotong ke [0, 100] (cakupan > sasaran adalah anomali, bukan kinerja
> 100%); kosong diisi median (jumlahnya dicatat); jumlah_kader dipotong di persentil 99; z-score
(StandardScaler) karena K-Means memakai jarak Euclid.

Model: baseline aturan (3 kelompok) | K-Means K=2..8 (n_init=20) | Agglomerative ward K=2..8.
Metrik: Silhouette (−1..1, makin besar cluster makin terpisah), Davies-Bouldin (>= 0, makin kecil
makin baik), Calinski-Harabasz (makin besar makin baik), inertia (elbow, K-Means), stabilitas =
rata-rata Adjusted Rand Index antar 10 seed K-Means (1 = selalu menghasilkan pembagian yang sama).

Aturan pemilihan (K39): K = 3..6 (masih bisa ditindaklanjuti), ARI >= 0,80, cluster terkecil
>= 5% posyandu (kelompok berisi segelintir pencilan tidak bisa dijadikan program); dari kandidat
dipilih Silhouette tertinggi, bila selisih < 0,02 dipilih Davies-Bouldin terkecil.
Syarat ukuran cluster ditambahkan setelah percobaan pertama memilih Agglomerative K=3 yang
menaruh 436 dari 487 posyandu di satu cluster (dicatat jujur di docs/KEPUTUSAN.md K39).
"""

from __future__ import annotations

import json
import logging

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.metrics import (adjusted_rand_score, calinski_harabasz_score, davies_bouldin_score,
                             silhouette_score)
from sklearn.preprocessing import StandardScaler

from common.batch import catat_task, tulis_dq_log
from common.db import buka_koneksi

log = logging.getLogger(__name__)

FITUR_INTERNAL = ["persen_ibu_hamil_2025", "persen_bayi_balita_2025", "kelengkapan_2025", "persen_anomali_2025",
                  "rasio_alat_baik", "jumlah_kader", "persen_kader_terlatih"]
FITUR_EKSTERNAL = ["rasio_stunting_kelurahan_2025"]
FITUR_V2 = [f for f in FITUR_INTERNAL if f != "kelengkapan_2025"]
SET_FITUR = {
    "v1_internal": {"fitur": FITUR_INTERNAL, "log1p": []},
    "v2_internal": {"fitur": FITUR_V2, "log1p": ["persen_anomali_2025"]},
    "v2_internal+eksternal": {"fitur": FITUR_V2 + FITUR_EKSTERNAL, "log1p": ["persen_anomali_2025"]},
}
# Arah "baik" tiap fitur untuk menyusun peringkat prioritas cluster (+1 makin besar makin baik).
ARAH = {"persen_ibu_hamil_2025": 1, "persen_bayi_balita_2025": 1, "kelengkapan_2025": 1, "persen_anomali_2025": -1,
        "rasio_alat_baik": 1, "jumlah_kader": 1, "persen_kader_terlatih": 1, "rasio_stunting_kelurahan_2025": -1}
LABEL_FITUR = {"persen_ibu_hamil_2025": "cakupan ibu hamil", "persen_bayi_balita_2025": "cakupan bayi balita",
               "kelengkapan_2025": "kelengkapan laporan", "persen_anomali_2025": "anomali data",
               "rasio_alat_baik": "kelengkapan alat standar", "jumlah_kader": "jumlah kader",
               "persen_kader_terlatih": "kader terlatih", "rasio_stunting_kelurahan_2025": "rasio stunting kelurahan"}
K_RENTANG = range(2, 9)
K_KANDIDAT = range(3, 7)
ARI_MIN, SIL_SELISIH, PORSI_MIN = 0.80, 0.02, 0.05
SEED = 42


# ---------------------------------------------------------------- data & preprocessing
def muat_profil(cur) -> pd.DataFrame:
    kolom = ["posyandu_key", "posyandu_id", "nama_kecamatan"] + FITUR_INTERNAL + FITUR_EKSTERNAL
    cur.execute(f"SELECT {', '.join(kolom)} FROM mart.posyandu_profil WHERE is_layak_ml ORDER BY posyandu_id")
    df = pd.DataFrame(cur.fetchall(), columns=kolom)
    for c in FITUR_INTERNAL + FITUR_EKSTERNAL:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    # jumlah_kader = 0 berarti posyandu TIDAK punya baris di data kader (tidak terpetakan), bukan "tanpa kader";
    # angka acuan "< 5 kader" juga hanya menghitung posyandu berkader. Diperlakukan kosong -> imputasi median
    # (K58). Tanpa ini posyandu tanpa data kader naik ke puncak daftar prioritas dengan alasan "0 kader".
    df.loc[df.jumlah_kader == 0, "jumlah_kader"] = np.nan
    return df


def siapkan_fitur(df: pd.DataFrame, fitur: list[str], log1p: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Mengembalikan (x_asli, x_model, catatan). x_asli = skala asli setelah clip & imputasi (untuk
    interpretasi); x_model = x_asli setelah transformasi log1p (masuk ke scaler)."""
    x = df[fitur].copy()
    catatan = {"kosong_diisi_median": {c: int(x[c].isna().sum()) for c in fitur if x[c].isna().any()}}
    for c in fitur:
        if c.startswith("persen_"):
            x[c] = x[c].clip(0, 100)
    if "jumlah_kader" in fitur:
        batas = float(x["jumlah_kader"].quantile(0.99))
        catatan["jumlah_kader_dipotong_p99"] = {"batas": batas, "n": int((x["jumlah_kader"] > batas).sum())}
        x["jumlah_kader"] = x["jumlah_kader"].clip(upper=batas)
    x = x.fillna(x.median())
    xm = x.copy()
    for c in log1p:
        xm[c] = np.log1p(xm[c])
    if log1p:
        catatan["log1p"] = log1p
    return x, xm, catatan


# ---------------------------------------------------------------- baseline berbasis aturan
def kelompok_aturan(df: pd.DataFrame) -> pd.Series:
    """Baseline tanpa ML (meniru cara manual dinas):
    Prioritas tinggi : cakupan bayi balita < 50% ATAU kelengkapan < 75% (9 dari 12 bulan)
    Perlu perhatian  : alat standar tidak lengkap (< 5 jenis) ATAU kader < 5 ATAU kader terlatih < 50%
    Baik             : selain itu."""
    bb = df.persen_bayi_balita_2025.fillna(0)
    tinggi = (bb < 50) | (df.kelengkapan_2025 < 0.75)
    perhatian = (df.rasio_alat_baik.fillna(0) < 1) | (df.jumlah_kader < 5) | (df.persen_kader_terlatih.fillna(0) < 50)
    return pd.Series(np.select([tinggi, perhatian], ["Prioritas tinggi", "Perlu perhatian"], "Baik"), index=df.index)


# ---------------------------------------------------------------- evaluasi
def metrik(z: np.ndarray, label) -> dict:
    label = np.asarray(label)
    if len(set(label.tolist())) < 2:
        return {"silhouette": None, "davies_bouldin": None, "calinski_harabasz": None}
    return {"silhouette": float(silhouette_score(z, label)), "davies_bouldin": float(davies_bouldin_score(z, label)),
            "calinski_harabasz": float(calinski_harabasz_score(z, label))}


def stabilitas_kmeans(z: np.ndarray, k: int, n_seed: int = 10) -> float:
    acuan = KMeans(k, n_init=20, random_state=SEED).fit_predict(z)
    ari = [adjusted_rand_score(acuan, KMeans(k, n_init=20, random_state=SEED + s).fit_predict(z))
           for s in range(1, n_seed + 1)]
    return float(np.mean(ari))


def ukuran(label) -> dict:
    u, n = np.unique(np.asarray(label), return_counts=True)
    return {str(a): int(b) for a, b in zip(u.tolist(), n.tolist())}


def _baris(model_id, algoritma, set_fitur, k, z, label, inertia, ari, catatan) -> dict:
    uk = ukuran(label)
    return {"model_id": model_id, "algoritma": algoritma, "set_fitur": set_fitur, "k": k, **metrik(z, label),
            "inertia": inertia, "stabilitas_ari": ari, "ukuran_cluster": json.dumps(uk),
            "porsi_min": min(uk.values()) / len(label), "catatan": catatan}


def bandingkan(df: pd.DataFrame) -> tuple[list[dict], dict]:
    """Melatih semua kandidat; mengembalikan daftar hasil evaluasi + data per set fitur."""
    hasil, dukung = [], {}
    aturan = kelompok_aturan(df).to_numpy()
    for nama_set, cfg in SET_FITUR.items():
        x, xm, cat = siapkan_fitur(df, cfg["fitur"], cfg["log1p"])
        z = StandardScaler().fit_transform(xm)
        dukung[nama_set] = {"x": x, "z": z, "catatan": cat, "fitur": cfg["fitur"]}
        hasil.append(_baris(f"aturan_{nama_set}", "aturan", nama_set, 3, z, aturan, None, 1.0,
                            "baseline berbasis aturan; metrik dihitung di ruang fitur yang sama"))
        for k in K_RENTANG:
            km = KMeans(k, n_init=20, random_state=SEED).fit(z)
            hasil.append(_baris(f"kmeans_{nama_set}_k{k}", "kmeans", nama_set, k, z, km.labels_, float(km.inertia_),
                                stabilitas_kmeans(z, k), "n_init=20, random_state=42"))
            ag = AgglomerativeClustering(k, linkage="ward").fit(z)
            hasil.append(_baris(f"agglo_{nama_set}_k{k}", "agglomerative", nama_set, k, z, ag.labels_, None, 1.0,
                                "linkage=ward (deterministik)"))
    return hasil, dukung


def pilih_model(hasil: list[dict]) -> dict:
    """Aturan K39: K 3..6, ARI >= 0,80, cluster terkecil >= 5%; Silhouette tertinggi; selisih < 0,02 -> DBI."""
    kand = [h for h in hasil if h["algoritma"] != "aturan" and h["k"] in K_KANDIDAT and h["silhouette"] is not None
            and h["stabilitas_ari"] >= ARI_MIN and h["porsi_min"] >= PORSI_MIN]
    if not kand:
        raise RuntimeError("tidak ada model yang memenuhi syarat pemilihan K39")
    terbaik = max(h["silhouette"] for h in kand)
    dekat = [h for h in kand if terbaik - h["silhouette"] < SIL_SELISIH]
    return min(dekat, key=lambda h: h["davies_bouldin"])


# ---------------------------------------------------------------- interpretasi cluster
# Nama cluster bagi dinas dibuat dengan ATURAN eksplisit atas z-score rata-rata cluster terhadap rata-rata
# kota (bukan ditulis tangan per nomor cluster), sehingga tetap benar bila run berikutnya menghasilkan
# nomor/urutan cluster yang berbeda. Aturan dicek berurutan; yang pertama cocok dipakai:
#   1. anomali data z >= 1                           -> Pencatatan perlu diverifikasi       (prioritas Sedang)
#   2. alat standar z <= -1                          -> Kekurangan alat antropometri         (Sedang)
#   3. jumlah kader z >= 1 dan cakupan z <= -0,5     -> Kader banyak, cakupan rendah         (Tinggi)
#   4. cakupan (min ibu hamil, bayi balita) <= -0,5  -> Cakupan rendah                       (Tinggi)
#   5. kader terlatih z <= -0,5                      -> Cakupan baik, kader belum terlatih   (Sedang)
#   6. semua indikator (arah baik) >= -0,3           -> Kinerja baik (percontohan)           (Rendah)
#   7. selain itu                                    -> Mendekati rata-rata kota              (Sedang)
# Tingkat prioritas: Tinggi = hasil layanan (cakupan) rendah; Sedang = masalah input/proses (alat,
# pencatatan, pelatihan) dengan cakupan tidak rendah; Rendah = tidak perlu intervensi khusus.
TINGKAT_URUT = {"Tinggi": 0, "Sedang": 1, "Rendah": 2}
REKOMENDASI = {
    "Pencatatan perlu diverifikasi":
        "Puskesmas memverifikasi angka Sasaran dan Cakupan sebelum dipakai untuk target; pendampingan pengisian "
        "format laporan bulanan; cocokkan Sasaran balita dengan data penduduk 0-4 tahun.",
    "Kekurangan alat antropometri":
        "Prioritaskan pengadaan/penggantian alat standar (timbangan digital, pengukur panjang/tinggi badan, pita "
        "LILA) di posyandu cluster ini lebih dulu; kalibrasi alat yang masih ada.",
    "Kader banyak, cakupan rendah":
        "Masalahnya bukan kekurangan tenaga: supervisi puskesmas atas pembagian tugas dan jadwal kader, kunjungan "
        "rumah sasaran yang tidak datang, evaluasi lokasi dan jam buka posyandu.",
    "Cakupan rendah":
        "Sweeping/kunjungan rumah ibu hamil dan balita, pengingat jadwal (pesan WA, pengumuman RT), koordinasi "
        "dengan bidan kelurahan; periksa juga apakah angka Sasaran terlalu besar.",
    "Cakupan baik, kader belum terlatih":
        "Ikutkan kader pelatihan 25 keterampilan secara bertahap; cakupan sudah baik, pelatihan menjaga mutu "
        "pengukuran dan konseling.",
    "Kinerja baik (percontohan)":
        "Jadikan percontohan dan mentor bagi posyandu cluster lain (kunjungan belajar antarkader); cukup "
        "pemantauan rutin.",
    "Mendekati rata-rata kota": "Pemantauan rutin bulanan; tidak perlu intervensi khusus.",
}


def _d(v: float, n: int = 1) -> str:
    """Desimal gaya Indonesia (koma), mis. 8,1."""
    return f"{v:.{n}f}".replace(".", ",")


def _lvl(z: float) -> str:
    return "di atas" if z >= 0.3 else "di bawah" if z <= -0.3 else "mendekati"


def kategori_bisnis(zc: dict, m: dict, kota: dict) -> tuple[str, str, str]:
    """(nama, tingkat_prioritas, karakter satu kalimat) dari z-score dan rata-rata skala asli sebuah cluster."""
    def g(f):
        return float(zc.get(f, 0.0))
    cak = min(g("persen_ibu_hamil_2025"), g("persen_bayi_balita_2025"))
    ih, bb = m.get("persen_ibu_hamil_2025", 0), m.get("persen_bayi_balita_2025", 0)
    kih, kbb = kota.get("persen_ibu_hamil_2025", 0), kota.get("persen_bayi_balita_2025", 0)
    if g("persen_anomali_2025") >= 1.0:
        return ("Pencatatan perlu diverifikasi", "Sedang",
                f"Rata-rata {_d(m['persen_anomali_2025'])}% baris laporan 2025 bertanda anomali (kota "
                f"{_d(kota['persen_anomali_2025'])}%), sehingga cakupannya ({ih:.0f}% ibu hamil, {bb:.0f}% bayi "
                "balita) belum bisa dipercaya penuh.")
    if g("rasio_alat_baik") <= -1.0:
        return ("Kekurangan alat antropometri", "Sedang",
                f"Hanya {_d(5 * m['rasio_alat_baik'])} dari 5 jenis alat standar dalam kondisi baik (kota "
                f"{_d(5 * kota['rasio_alat_baik'])}), sementara cakupan {_lvl(cak)} rata-rata kota.")
    if g("jumlah_kader") >= 1.0 and cak <= -0.5:
        return ("Kader banyak, cakupan rendah", "Tinggi",
                f"Rata-rata {_d(m['jumlah_kader'])} kader per posyandu (kota {_d(kota['jumlah_kader'])}), tetapi "
                f"cakupan bayi balita hanya {bb:.0f}% (kota {kbb:.0f}%).")
    if cak <= -0.5:
        return ("Cakupan rendah", "Tinggi",
                f"Cakupan ibu hamil {ih:.0f}% dan bayi balita {bb:.0f}%, jauh di bawah rata-rata kota "
                f"({kih:.0f}% dan {kbb:.0f}%), padahal alat dan jumlah kader tidak jauh dari rata-rata.")
    if g("persen_kader_terlatih") <= -0.5:
        return ("Cakupan baik, kader belum terlatih", "Sedang",
                f"Cakupan {_lvl(cak)} rata-rata kota ({ih:.0f}% ibu hamil, {bb:.0f}% bayi balita), tetapi baru "
                f"{m['persen_kader_terlatih']:.0f}% kader ikut pelatihan 25 keterampilan (kota "
                f"{kota['persen_kader_terlatih']:.0f}%).")
    if min(ARAH[f] * g(f) for f in zc) >= -0.3:
        return ("Kinerja baik (percontohan)", "Rendah",
                f"Cakupan ibu hamil {ih:.0f}% dan bayi balita {bb:.0f}%, {m.get('persen_kader_terlatih', 0):.0f}% "
                f"kader terlatih, anomali data {_d(m.get('persen_anomali_2025', 0))}%: semua indikator di atas atau "
                "mendekati rata-rata kota.")
    return ("Mendekati rata-rata kota", "Sedang", "Tidak ada indikator yang menyimpang jauh dari rata-rata kota.")


def profil_cluster(x: pd.DataFrame, label: np.ndarray, fitur: list[str]) -> tuple[pd.DataFrame, dict, dict]:
    """Rata-rata fitur (skala asli) per cluster, z-score terhadap rata-rata kota, dan interpretasi.

    Mengembalikan (baris profil, nama {cluster: "K1 ..."}, info {cluster: deskripsi, nama, tingkat, karakter,
    rekomendasi, skor}). Urutan K1..Kn: tingkat prioritas (Tinggi -> Rendah), lalu skor = rata-rata z-score
    semua fitur dengan arah "baik" (terendah lebih dulu)."""
    rata_kota, sd_kota = x.mean(), x.std(ddof=0).replace(0, 1)
    baris, info = [], {}
    for c in sorted(set(label.tolist())):
        m = x[label == c].mean()
        zc = (m - rata_kota) / sd_kota
        menonjol = sorted(fitur, key=lambda f: -abs(zc[f]))
        teks = [f"{LABEL_FITUR[f]} {'tinggi' if zc[f] > 0 else 'rendah'}" for f in menonjol if abs(zc[f]) >= 0.5][:3]
        nama_b, tingkat, karakter = kategori_bisnis({f: zc[f] for f in fitur}, m.to_dict(), rata_kota.to_dict())
        info[c] = {"deskripsi": ", ".join(teks) if teks else "mendekati rata-rata kota", "nama": nama_b,
                   "tingkat": tingkat, "karakter": karakter, "rekomendasi": REKOMENDASI[nama_b],
                   "skor": float(np.mean([ARAH[f] * zc[f] for f in fitur]))}
        for f in fitur:
            baris.append({"cluster": int(c), "fitur": f, "rata_rata": float(m[f]), "rata_rata_kota": float(rata_kota[f]),
                          "z_score": float(zc[f]), "n_posyandu": int((label == c).sum())})
    urut = sorted(info, key=lambda c: (TINGKAT_URUT[info[c]["tingkat"]], info[c]["skor"]))
    nama, dipakai = {}, set()
    for i, c in enumerate(urut):
        n = info[c]["nama"]
        if n in dipakai:  # dua cluster jatuh ke aturan yang sama -> dibedakan dengan fitur paling menonjol
            n = f"{n} ({info[c]['deskripsi'].split(',')[0]})"
        dipakai.add(n)
        nama[c] = f"K{i + 1} {n}"
    return pd.DataFrame(baris), nama, info


TEKS_ALASAN = {
    "persen_ibu_hamil_2025": lambda v, k: f"cakupan ibu hamil {v:.0f}% (kota {k:.0f}%)",
    "persen_bayi_balita_2025": lambda v, k: f"cakupan bayi balita {v:.0f}% (kota {k:.0f}%)",
    "kelengkapan_2025": lambda v, k: f"kelengkapan laporan {100 * v:.0f}% (kota {100 * k:.0f}%)",
    "persen_anomali_2025": lambda v, k: f"{v:.0f}% baris laporan anomali (kota {_d(k)}%)",
    "rasio_alat_baik": lambda v, k: f"alat standar baik {5 * v:.0f}/5 jenis (kota {_d(5 * k)})",
    "jumlah_kader": lambda v, k: f"{v:.0f} kader (kota {_d(k)})",
    "persen_kader_terlatih": lambda v, k: f"kader terlatih {v:.0f}% (kota {k:.0f}%)",
    "rasio_stunting_kelurahan_2025": lambda v, k: f"rasio stunting kelurahan {_d(v)}% (kota {_d(k)}%)",
}


def prioritas_posyandu(x: pd.DataFrame, label: np.ndarray, fitur: list[str], info: dict) -> pd.DataFrame:
    """Daftar prioritas per posyandu.

    skor   = rata-rata z-score fitur dengan arah dibalik (makin besar makin perlu pembinaan), skala asli
    tingkat = tingkat prioritas cluster-nya
    peringkat = urut tingkat (Tinggi dulu), lalu skor terbesar
    alasan = maksimal 2 indikator terburuk posyandu itu (z arah baik <= -0,5) dibanding rata-rata kota."""
    rata, sd = x.mean(), x.std(ddof=0).replace(0, 1)
    zarah = pd.DataFrame({f: ARAH[f] * (x[f] - rata[f]) / sd[f] for f in fitur}, index=x.index)
    out = pd.DataFrame({"skor": -zarah.mean(axis=1), "tingkat": [info[c]["tingkat"] for c in label]}, index=x.index)
    alasan = []
    for i, r in zarah.iterrows():
        buruk = [f for f in r.sort_values().index if r[f] <= -0.5][:2]
        alasan.append("; ".join(TEKS_ALASAN[f](x.at[i, f], rata[f]) for f in buruk)
                      or "tidak ada indikator yang jauh di bawah rata-rata kota")
    out["alasan"] = alasan
    urutan = out.assign(u=out.tingkat.map(TINGKAT_URUT)).sort_values(["u", "skor"], ascending=[True, False]).index
    out.loc[urutan, "peringkat"] = np.arange(1, len(out) + 1)
    out["peringkat"] = out.peringkat.astype(int)
    return out


# ---------------------------------------------------------------- task
def train_ml(batch_id: str, percobaan_ke: int | None = None) -> dict:
    with catat_task(batch_id, "train_ml", "ml", percobaan_ke) as hasil_task:
        conn = buka_koneksi()
        try:
            with conn, conn.cursor() as cur:
                df = muat_profil(cur)
                if len(df) < 50:
                    raise RuntimeError(f"data posyandu untuk ML terlalu sedikit ({len(df)})")
                hasil, dukung = bandingkan(df)
                terpilih = pilih_model(hasil)
                for h in hasil:
                    h["is_terpilih"] = h["model_id"] == terpilih["model_id"]

                d = dukung[terpilih["set_fitur"]]
                if terpilih["algoritma"] == "kmeans":
                    model = KMeans(terpilih["k"], n_init=20, random_state=SEED).fit(d["z"])
                    label, pusat = model.labels_, model.cluster_centers_
                else:
                    label = AgglomerativeClustering(terpilih["k"], linkage="ward").fit_predict(d["z"])
                    pusat = np.vstack([d["z"][label == c].mean(axis=0) for c in range(terpilih["k"])])
                jarak = np.linalg.norm(d["z"] - pusat[label], axis=1)
                prof, nama, info = profil_cluster(d["x"], label, d["fitur"])
                prio = prioritas_posyandu(d["x"], label, d["fitur"], info)
                aturan = kelompok_aturan(df)
                # Kesesuaian dengan baseline (bukan "akurasi": clustering tidak punya label benar).
                ari_aturan = float(adjusted_rand_score(aturan, label))

                cur.execute("DELETE FROM mart.ml_evaluasi")
                execute_values(cur, """INSERT INTO mart.ml_evaluasi (model_id, algoritma, set_fitur, k, silhouette,
                    davies_bouldin, calinski_harabasz, inertia, stabilitas_ari, ukuran_cluster, is_terpilih, catatan,
                    batch_id) VALUES %s""", [(h["model_id"], h["algoritma"], h["set_fitur"], h["k"], h["silhouette"],
                                              h["davies_bouldin"], h["calinski_harabasz"], h["inertia"],
                                              h["stabilitas_ari"], h["ukuran_cluster"], h["is_terpilih"],
                                              h["catatan"], batch_id) for h in hasil])
                cur.execute("DELETE FROM mart.hasil_cluster")
                execute_values(cur, """INSERT INTO mart.hasil_cluster (posyandu_key, posyandu_id, model_id, cluster,
                    label_cluster, kelompok_aturan, jarak_centroid, tingkat_prioritas, skor_kebutuhan,
                    peringkat_prioritas, alasan_prioritas, batch_id) VALUES %s""",
                               [(int(pk), pid, terpilih["model_id"], int(c), nama[int(c)], a, float(j), p.tingkat,
                                 float(p.skor), int(p.peringkat), p.alasan, batch_id)
                                for pk, pid, c, a, j, p in zip(df.posyandu_key, df.posyandu_id, label, aturan, jarak,
                                                               prio.itertuples())])
                cur.execute("DELETE FROM mart.cluster_profil")
                execute_values(cur, """INSERT INTO mart.cluster_profil (model_id, cluster, label_cluster, deskripsi,
                    n_posyandu, fitur, rata_rata, rata_rata_kota, z_score, tingkat_prioritas, karakter, rekomendasi,
                    batch_id) VALUES %s""",
                               [(terpilih["model_id"], r.cluster, nama[r.cluster], info[r.cluster]["deskripsi"],
                                 r.n_posyandu, r.fitur, r.rata_rata, r.rata_rata_kota, r.z_score,
                                 info[r.cluster]["tingkat"], info[r.cluster]["karakter"],
                                 info[r.cluster]["rekomendasi"], batch_id)
                                for r in prof.itertuples()])
                tulis_dq_log(cur, batch_id, "ml", [{
                    "kode_aturan": "ML01", "tingkat": "INFO", "objek": "mart.hasil_cluster",
                    "nilai": terpilih["model_id"],
                    "keterangan": (f"{len(df)} posyandu; model terpilih {terpilih['model_id']} silhouette "
                                   f"{terpilih['silhouette']:.4f} DBI {terpilih['davies_bouldin']:.4f} ARI-seed "
                                   f"{terpilih['stabilitas_ari']:.3f}; ARI vs baseline aturan {ari_aturan:.3f}; "
                                   f"preprocessing {json.dumps(d['catatan'])}")[:2000],
                }])
        finally:
            conn.close()
        hasil_task.jumlah_baris = len(df)
        hasil_task.pesan = (f"terpilih {terpilih['model_id']} (silhouette {terpilih['silhouette']:.3f}, "
                            f"DBI {terpilih['davies_bouldin']:.3f}); {len(hasil)} model dibandingkan")
        return {"model": terpilih["model_id"], "silhouette": round(terpilih["silhouette"], 4), "n": len(df),
                "ari_vs_aturan": round(ari_aturan, 4),
                "cluster": {nama[c]: info[c]["tingkat"] for c in sorted(nama, key=lambda c: nama[c])}}
