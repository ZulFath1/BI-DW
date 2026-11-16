"""Laporan evaluasi ML (Markdown) dari hasil run terakhir: dipakai untuk laporan PDF dan bukti.

    docker compose exec airflow-scheduler python -m ml.laporan > docs/bukti/26_ml_perbandingan.txt

Isi (semua angka dihitung dari database, bukan diketik tangan):
  A. tabel perbandingan lengkap 45 model (baseline aturan, K-Means & Agglomerative K=2–8, 3 set fitur);
  B. model terpilih berdampingan dengan model terbaik TANPA syarat ukuran cluster;
  C. profil model tanpa syarat (apa isi cluster kecilnya);
  D. profil cluster model terpilih: jumlah posyandu, rata-rata fitur skala asli, nama, karakter, rekomendasi;
  E. apakah K lebih kecil lebih tepat: K-Means v2 K=4/5/6 dengan aturan penamaan yang sama + tabulasi silang.
Hanya agregat; tidak ada nama posyandu maupun data kader.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering, KMeans

from common.db import buka_koneksi
from ml.clustering import (ARI_MIN, K_KANDIDAT, LABEL_FITUR, SEED, SET_FITUR, kategori_bisnis, muat_profil,
                           profil_cluster, siapkan_fitur)

FITUR_TAMPIL = ["persen_ibu_hamil_2025", "persen_bayi_balita_2025", "kelengkapan_2025", "persen_anomali_2025",
                "rasio_alat_baik", "jumlah_kader", "persen_kader_terlatih", "rasio_stunting_kelurahan_2025"]


def d(v, n=3) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "–"
    return f"{float(v):,.{n}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def md(df: pd.DataFrame) -> str:
    kolom = list(df.columns)
    baris = ["| " + " | ".join(kolom) + " |", "|" + "---|" * len(kolom)]
    baris += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(baris)


def ukuran_teks(js: str) -> tuple[str, float, float]:
    u = sorted(json.loads(js).values(), reverse=True)
    n = sum(u)
    return " / ".join(map(str, u)), min(u) / n, max(u) / n


def bagian_a(ev: pd.DataFrame) -> str:
    rows = []
    for r in ev.sort_values(["set_fitur", "algoritma", "k"]).itertuples():
        uk, pmin, pmax = ukuran_teks(r.ukuran_cluster)
        rows.append({"Set fitur": r.set_fitur, "Model": r.algoritma, "K": r.k, "Silhouette ↑": d(r.silhouette),
                     "Davies-Bouldin ↓": d(r.davies_bouldin), "Calinski-H ↑": d(r.calinski_harabasz, 0),
                     "Inertia": d(r.inertia, 0), "ARI seed": d(r.stabilitas_ari, 2), "Ukuran cluster": uk,
                     "Terkecil": f"{100 * pmin:.1f}%".replace(".", ","),
                     "Lolos K39": "ya" if lolos(r, True) else "tidak", "Terpilih": "**ya**" if r.is_terpilih else ""})
    return md(pd.DataFrame(rows))


def lolos(r, dengan_ukuran: bool) -> bool:
    if r.algoritma == "aturan" or r.k not in K_KANDIDAT or r.silhouette is None:
        return False
    _, pmin, _ = ukuran_teks(r.ukuran_cluster)
    return r.stabilitas_ari >= ARI_MIN and (pmin >= 0.05 or not dengan_ukuran)


def bagian_b(ev: pd.DataFrame) -> tuple[str, pd.Series]:
    ml = ev[ev.algoritma != "aturan"].copy()
    ml["lolos_k39"] = [lolos(r, True) for r in ml.itertuples()]
    ml["lolos_tanpa_ukuran"] = [lolos(r, False) for r in ml.itertuples()]
    pilih = ev[ev.is_terpilih].iloc[0]
    tanpa_ukuran = ml[ml.lolos_tanpa_ukuran].sort_values("silhouette").iloc[-1]
    mutlak = ml.sort_values("silhouette").iloc[-1]
    eks = ml[(ml.set_fitur == "v2_internal+eksternal") & ml.lolos_k39].sort_values("silhouette").iloc[-1]
    agg = ml[(ml.algoritma == "agglomerative") & ml.lolos_k39].sort_values("silhouette").iloc[-1]
    base = ev[(ev.algoritma == "aturan") & (ev.set_fitur == "v2_internal")].iloc[0]
    kol = {"Baseline aturan (ruang v2)": base, "**Terpilih (K39)**": pilih,
           "Terbaik tanpa syarat ukuran (K 3–6, ARI ≥ 0,80)": tanpa_ukuran,
           "Terbaik mutlak (K 2–8, tanpa syarat)": mutlak, "Terbaik + fitur stunting (K39)": eks,
           "Terbaik Agglomerative (K39)": agg}
    baris = {"model_id": [], "Silhouette ↑": [], "Davies-Bouldin ↓": [], "Calinski-Harabasz ↑": [], "ARI seed": [],
             "Ukuran cluster": [], "Cluster terbesar": [], "Cluster terkecil": []}
    for r in kol.values():
        uk, pmin, pmax = ukuran_teks(r.ukuran_cluster)
        baris["model_id"].append(f"`{r.model_id}`")
        baris["Silhouette ↑"].append(d(r.silhouette))
        baris["Davies-Bouldin ↓"].append(d(r.davies_bouldin))
        baris["Calinski-Harabasz ↑"].append(d(r.calinski_harabasz, 0))
        baris["ARI seed"].append(d(r.stabilitas_ari, 2) if r.algoritma == "kmeans" else "– (deterministik)")
        baris["Ukuran cluster"].append(uk)
        baris["Cluster terbesar"].append(f"{100 * pmax:.1f}%".replace(".", ","))
        baris["Cluster terkecil"].append(f"{100 * pmin:.1f}%".replace(".", ","))
    t = pd.DataFrame(baris, index=list(kol)).T.reset_index().rename(columns={"index": "Ukuran"})
    return md(t), tanpa_ukuran


def profil_tabel(x: pd.DataFrame, label: np.ndarray, fitur: list[str]) -> pd.DataFrame:
    rows = []
    for c in sorted(set(label.tolist()), key=lambda c: -(label == c).sum()):
        m = x[label == c].mean()
        r = {"Cluster": c, "Posyandu": int((label == c).sum())}
        for f in fitur:
            r[LABEL_FITUR[f]] = d(5 * m[f], 1) + "/5" if f == "rasio_alat_baik" else \
                d(100 * m[f], 1) + "%" if f == "kelengkapan_2025" else d(m[f], 1)
        rows.append(r)
    m = x.mean()
    r = {"Cluster": "**Kota**", "Posyandu": len(x)}
    for f in fitur:
        r[LABEL_FITUR[f]] = d(5 * m[f], 1) + "/5" if f == "rasio_alat_baik" else \
            d(100 * m[f], 1) + "%" if f == "kelengkapan_2025" else d(m[f], 1)
    rows.append(r)
    return pd.DataFrame(rows)


def label_model(df: pd.DataFrame, model_id: str) -> tuple[np.ndarray, pd.DataFrame, list[str]]:
    algoritma, sisa = model_id.split("_", 1)
    set_f, k = sisa.rsplit("_k", 1)
    cfg = SET_FITUR[set_f]
    x, xm, _ = siapkan_fitur(df, cfg["fitur"], cfg["log1p"])
    from sklearn.preprocessing import StandardScaler
    z = StandardScaler().fit_transform(xm)
    if algoritma == "kmeans":
        lab = KMeans(int(k), n_init=20, random_state=SEED).fit_predict(z)
    else:
        lab = AgglomerativeClustering(int(k), linkage="ward").fit_predict(z)
    return lab, x, cfg["fitur"]


def main() -> None:
    conn = buka_koneksi()
    with conn, conn.cursor() as cur:
        ev = pd.read_sql("SELECT * FROM mart.ml_evaluasi", conn)
        for c in ["silhouette", "davies_bouldin", "calinski_harabasz", "inertia", "stabilitas_ari"]:
            ev[c] = pd.to_numeric(ev[c], errors="coerce").astype(float)
        df = muat_profil(cur)
        cp = pd.read_sql("SELECT * FROM mart.cluster_profil", conn)
        batch = ev.batch_id.iloc[0]
    conn.close()

    print(f"# Evaluasi clustering posyandu — batch `{batch}`\n")
    print(f"{len(df)} posyandu (punya data 2025). Aturan pemilihan K39: K 3–6, ARI antar seed ≥ 0,80, "
          "cluster terkecil ≥ 5%, Silhouette tertinggi (selisih < 0,02 → Davies-Bouldin terkecil).\n")
    print("## A. Perbandingan lengkap (45 model)\n")
    print(bagian_a(ev))
    print("\nARI seed Agglomerative = 1 karena algoritmanya deterministik (tidak bergantung seed); baseline "
          "aturan juga deterministik. Inertia hanya untuk K-Means.\n")

    print("## B. Model terpilih vs model terbaik tanpa syarat ukuran cluster\n")
    teks_b, tanpa = bagian_b(ev)
    print(teks_b)

    print(f"\n## C. Isi model tanpa syarat ukuran (`{tanpa.model_id}`)\n")
    lab, x, fitur = label_model(df, tanpa.model_id)
    print(md(profil_tabel(x, lab, fitur)))

    print("\n## D. Profil cluster model terpilih (rata-rata skala asli)\n")
    piv = cp.pivot_table(index=["label_cluster", "n_posyandu", "tingkat_prioritas"], columns="fitur",
                         values="rata_rata", aggfunc="first").reset_index()
    kota = cp.groupby("fitur").rata_rata_kota.first()
    rows = []
    for r in piv.sort_values("label_cluster").itertuples():
        rr = r._asdict()
        rows.append({"Cluster": r.label_cluster, "Posyandu": r.n_posyandu, "Prioritas": r.tingkat_prioritas,
                     "% ibu hamil": d(rr["persen_ibu_hamil_2025"], 1), "% bayi balita": d(rr["persen_bayi_balita_2025"], 1),
                     "% baris anomali": d(rr["persen_anomali_2025"], 1),
                     "Alat std baik": d(5 * float(rr["rasio_alat_baik"]), 1) + "/5",
                     "Kader": d(rr["jumlah_kader"], 1), "% kader terlatih": d(rr["persen_kader_terlatih"], 1)})
    rows.append({"Cluster": "**Kota**", "Posyandu": int(piv.n_posyandu.sum()), "Prioritas": "",
                 "% ibu hamil": d(kota["persen_ibu_hamil_2025"], 1), "% bayi balita": d(kota["persen_bayi_balita_2025"], 1),
                 "% baris anomali": d(kota["persen_anomali_2025"], 1),
                 "Alat std baik": d(5 * float(kota["rasio_alat_baik"]), 1) + "/5",
                 "Kader": d(kota["jumlah_kader"], 1), "% kader terlatih": d(kota["persen_kader_terlatih"], 1)})
    print(md(pd.DataFrame(rows)))
    print()
    ket = cp[["label_cluster", "karakter", "rekomendasi"]].drop_duplicates().sort_values("label_cluster")
    print(md(ket.rename(columns={"label_cluster": "Cluster", "karakter": "Karakter",
                                 "rekomendasi": "Rekomendasi tindakan"})))

    print("\n## E. Apakah K lebih kecil lebih tepat? (K-Means v2 internal, aturan penamaan yang sama)\n")
    lab6, x6, f6 = label_model(df, "kmeans_v2_internal_k6")
    _, nama6, _ = profil_cluster(x6, lab6, f6)
    for k in (4, 5):
        labk, xk, fk = label_model(df, f"kmeans_v2_internal_k{k}")
        _, namak, infok = profil_cluster(xk, labk, fk)
        r = ev[ev.model_id == f"kmeans_v2_internal_k{k}"].iloc[0]
        print(f"### K = {k} (Silhouette {d(r.silhouette)}, DBI {d(r.davies_bouldin)}, ARI {d(r.stabilitas_ari, 2)})\n")
        print(md(pd.DataFrame([{"Cluster": namak[c], "Posyandu": int((labk == c).sum()), "Karakter": infok[c]["karakter"]}
                               for c in sorted(namak, key=lambda c: namak[c])])))
        tab = pd.crosstab(pd.Series([nama6[c] for c in lab6], name="K=6"),
                          pd.Series([namak[c] for c in labk], name=f"K={k}"))
        print(f"\nTabulasi silang K=6 (baris) × K={k} (kolom), jumlah posyandu:\n")
        print(md(tab.reset_index()))
        print()


if __name__ == "__main__":
    main()
