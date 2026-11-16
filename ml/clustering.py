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
    cur.execute(f"SELECT {', '.join(kolom)} FROM mart.posyandu_profil WHERE is_layak_ml ORDER BY posyandu_key")
    df = pd.DataFrame(cur.fetchall(), columns=kolom)
    for c in FITUR_INTERNAL + FITUR_EKSTERNAL:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
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
def profil_cluster(x: pd.DataFrame, label: np.ndarray, fitur: list[str]) -> tuple[pd.DataFrame, dict, dict]:
    """Rata-rata fitur (skala asli) per cluster, z-score terhadap rata-rata kota, deskripsi & label prioritas."""
    rata_kota, sd_kota = x.mean(), x.std(ddof=0).replace(0, 1)
    baris, skor, desk = [], {}, {}
    for c in sorted(set(label.tolist())):
        m = x[label == c].mean()
        zc = (m - rata_kota) / sd_kota
        skor[c] = float(np.mean([ARAH[f] * zc[f] for f in fitur]))
        menonjol = sorted(fitur, key=lambda f: -abs(zc[f]))
        teks = [f"{LABEL_FITUR[f]} {'tinggi' if zc[f] > 0 else 'rendah'}" for f in menonjol if abs(zc[f]) >= 0.5][:3]
        desk[c] = ", ".join(teks) if teks else "mendekati rata-rata kota"
        for f in fitur:
            baris.append({"cluster": int(c), "fitur": f, "rata_rata": float(m[f]), "rata_rata_kota": float(rata_kota[f]),
                          "z_score": float(zc[f]), "n_posyandu": int((label == c).sum())})
    # Urutan: skor terendah = K1 (paling perlu pembinaan menurut rata-rata semua fitur), tertinggi = terakhir.
    # Nama cluster = karakter paling menonjol (1–2 fitur dengan |z| terbesar), bukan ditebak.
    urut = sorted(skor, key=lambda c: skor[c])
    nama = {}
    for i, c in enumerate(urut):
        prof_c = sorted((r for r in baris if r["cluster"] == c), key=lambda r: -abs(r["z_score"]))
        frasa = []
        for r in prof_c:
            if abs(r["z_score"]) < 0.5 and frasa:
                break
            p = FRASA[(r["fitur"], r["z_score"] > 0)]
            if p not in frasa:
                frasa.append(p)
            if len(frasa) == 2:
                break
        nama[c] = f"K{i + 1} " + " & ".join(frasa)
    return pd.DataFrame(baris), nama, desk


FRASA = {("persen_ibu_hamil_2025", False): "cakupan rendah", ("persen_ibu_hamil_2025", True): "cakupan tinggi",
         ("persen_bayi_balita_2025", False): "cakupan rendah", ("persen_bayi_balita_2025", True): "cakupan tinggi",
         ("kelengkapan_2025", False): "laporan tidak lengkap", ("kelengkapan_2025", True): "laporan lengkap",
         ("persen_anomali_2025", True): "data banyak anomali", ("persen_anomali_2025", False): "data rapi",
         ("rasio_alat_baik", False): "alat standar kurang", ("rasio_alat_baik", True): "alat lengkap",
         ("jumlah_kader", True): "kader banyak", ("jumlah_kader", False): "kader sedikit",
         ("persen_kader_terlatih", False): "kader belum terlatih", ("persen_kader_terlatih", True): "kader terlatih",
         ("rasio_stunting_kelurahan_2025", True): "stunting kelurahan tinggi",
         ("rasio_stunting_kelurahan_2025", False): "stunting kelurahan rendah"}


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
                prof, nama, desk = profil_cluster(d["x"], label, d["fitur"])
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
                    label_cluster, kelompok_aturan, jarak_centroid, batch_id) VALUES %s""",
                               [(int(pk), pid, terpilih["model_id"], int(c), nama[int(c)], a, float(j), batch_id)
                                for pk, pid, c, a, j in zip(df.posyandu_key, df.posyandu_id, label, aturan, jarak)])
                cur.execute("DELETE FROM mart.cluster_profil")
                execute_values(cur, """INSERT INTO mart.cluster_profil (model_id, cluster, label_cluster, deskripsi,
                    n_posyandu, fitur, rata_rata, rata_rata_kota, z_score, batch_id) VALUES %s""",
                               [(terpilih["model_id"], r.cluster, nama[r.cluster], desk[r.cluster], r.n_posyandu,
                                 r.fitur, r.rata_rata, r.rata_rata_kota, r.z_score, batch_id)
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
                "cluster": {nama[c]: desk[c] for c in sorted(nama, key=lambda c: nama[c])}}
