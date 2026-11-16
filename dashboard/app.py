"""Dashboard Posyandu Kota Banjarmasin (Streamlit).

Sumber data: HANYA schema `mart` (dan view `dw.v_rasio_stunting_balita` lewat mart.kpi_kelurahan),
dibaca dengan user read-only DASHBOARD_USER. Tidak membaca berkas mentah, staging, atau dq.
Tidak ada data pribadi kader: kader hanya tampil sebagai jumlah per posyandu.

Cache: hasil query di-cache per batch_id terakhir di mart.dashboard_refresh. Task Airflow
`refresh_dashboard` menulis baris baru setiap run sukses, sehingga data baru langsung terbaca.

Halaman (tab): Ringkasan & insight | Tren cakupan | Posyandu prioritas (ML) | Stunting kelurahan |
Kualitas data. Filter global di sidebar: tahun, kecamatan, kelompok sasaran.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

st.set_page_config(page_title="Dashboard Posyandu Banjarmasin", layout="wide")

# Palet (skill dataviz, palet referensi tervalidasi; mode terang). Urutan kategori tetap, tidak diputar.
KATEGORI = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SATU_SERI = KATEGORI[0]
DIVERGING = [[0.0, "#e34948"], [0.5, "#f0efec"], [1.0, "#2a78d6"]]  # merah = lebih buruk, biru = lebih baik
KECAMATAN = ["BANJARMASIN BARAT", "BANJARMASIN SELATAN", "BANJARMASIN TENGAH", "BANJARMASIN TIMUR",
             "BANJARMASIN UTARA"]
WARNA_KEC = dict(zip(KECAMATAN, KATEGORI))  # warna mengikuti entitas, bukan urutan tampil
LAYOUT = dict(plot_bgcolor="#fcfcfb", paper_bgcolor="#fcfcfb", font=dict(color="#0b0b0b"),
              margin=dict(l=10, r=10, t=40, b=10), hoverlabel=dict(bgcolor="white"))


def rapikan(fig: go.Figure, judul: str, y: str | None = None, x: str | None = None) -> go.Figure:
    fig.update_layout(title=judul, **LAYOUT)
    fig.update_xaxes(showgrid=False, title=x, linecolor="#c3c2b7")
    fig.update_yaxes(gridcolor="#e8e7e2", title=y, zeroline=False)
    fig.update_traces(cliponaxis=False, selector=dict(type="bar"))  # label angka tidak terpotong
    fig.update_layout(margin=dict(l=10, r=60, t=40, b=10))
    return fig


@st.cache_resource
def engine():
    url = URL.create("postgresql+psycopg2", username=os.environ["DASHBOARD_USER"],
                     password=os.environ["DASHBOARD_PASSWORD"], host=os.environ.get("DW_HOST", "postgres"),
                     port=int(os.environ.get("DW_PORT", "5432")), database=os.environ["DW_DB"])
    return create_engine(url, pool_pre_ping=True)


def batch_terakhir() -> tuple[str | None, object]:
    with engine().connect() as c:
        r = c.execute(text("SELECT batch_id, dibuat_pada FROM mart.dashboard_refresh ORDER BY refresh_id DESC LIMIT 1")
                      ).first()
    return (r[0], r[1]) if r else (None, None)


@st.cache_data(show_spinner=False)
def q(sql: str, batch: str | None) -> pd.DataFrame:  # batch hanya kunci cache
    with engine().connect() as c:
        return pd.read_sql(text(sql), c)


# ---------------------------------------------------------------- data
try:
    BATCH, WAKTU = batch_terakhir()
    kpi = q("SELECT * FROM mart.kpi_cakupan_bulanan", BATCH)
    lengkap = q("SELECT * FROM mart.kelengkapan_bulanan", BATCH)
    profil = q("SELECT * FROM mart.posyandu_profil", BATCH)
    kel = q("SELECT * FROM mart.kpi_kelurahan", BATCH)
    kual = q("SELECT * FROM mart.ringkasan_kualitas", BATCH)
    gate = q("SELECT * FROM mart.quality_gate_terakhir ORDER BY kode_cek", BATCH)
    klaster = q("SELECT * FROM mart.hasil_cluster", BATCH)
    kprof = q("SELECT * FROM mart.cluster_profil", BATCH)
    meval = q("SELECT * FROM mart.ml_evaluasi", BATCH)
except Exception as err:
    st.error(f"Gagal membaca data mart ({type(err).__name__}). Jalankan pipeline Airflow dulu.")
    st.stop()

if kpi.empty or profil.empty:
    st.warning("Data mart masih kosong. Jalankan DAG `posyandu_pipeline` di Airflow.")
    st.stop()

# ---------------------------------------------------------------- sidebar (filter global)
st.sidebar.header("Filter")
tahun_ada = sorted(kpi.tahun.unique())
tahun = st.sidebar.selectbox("Tahun", tahun_ada, index=tahun_ada.index(2025) if 2025 in tahun_ada else len(tahun_ada) - 1)
kec_pilih = st.sidebar.multiselect("Kecamatan", KECAMATAN, default=KECAMATAN)
kelompok_ada = kpi[["kelompok_key", "nama_kelompok"]].drop_duplicates().sort_values("kelompok_key")
kelompok = st.sidebar.selectbox("Kelompok sasaran", list(kelompok_ada.nama_kelompok), index=1)
st.sidebar.caption("KPI cakupan = Σ cakupan ÷ Σ sasaran; hanya baris Terlapor, bukan anomali A1, sasaran > 0 "
                   "(aturan K38).")
st.sidebar.caption(f"Data batch: `{BATCH}`  \nDiperbarui: {WAKTU:%d %b %Y %H:%M}" if WAKTU else "")
if not kec_pilih:
    st.info("Pilih minimal satu kecamatan.")
    st.stop()

kpi_f = kpi[kpi.nama_kecamatan.isin(kec_pilih)]
prof_f = profil[profil.nama_kecamatan.isin(kec_pilih)]


def persen(c, s):
    return round(100 * c / s, 1) if s else None


st.title("Dashboard Posyandu — Kota Banjarmasin")
st.caption("Sumber: data warehouse posyandu (laporan bulanan 2023–2026, alat antropometri Juni 2026, kader 2026), "
           "Disdukcapil Sem. II 2025, Dinkes Satu Data (stunting). Studi kasus tugas UAS — bukan dokumen resmi dinas.")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Ringkasan & insight", "Tren cakupan", "Posyandu prioritas (ML)",
                                        "Stunting kelurahan", "Kualitas data"])

# ---------------------------------------------------------------- tab 1: ringkasan
with tab1:
    th = kpi_f[kpi_f.tahun == tahun]
    kel_th = th[th.nama_kelompok == kelompok]
    lk = lengkap[(lengkap.tahun == tahun) & lengkap.nama_kecamatan.isin(kec_pilih)]
    bln_akhir = int(lk[lk.n_posyandu_terlapor > 0].bulan.max()) if (lk.n_posyandu_terlapor > 0).any() else None
    akhir = lk[lk.bulan == bln_akhir] if bln_akhir else lk.iloc[0:0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"Cakupan {kelompok} {tahun}", f"{persen(kel_th.cakupan.sum(), kel_th.sasaran.sum()) or '–'}%",
              help="Σ cakupan ÷ Σ sasaran setahun (filter KPI K38)")
    c2.metric(f"Posyandu terlapor (bulan {bln_akhir or '–'})",
              f"{int(akhir.n_posyandu_terlapor.sum())} / {int(akhir.n_posyandu.sum())}",
              help="Posyandu dengan minimal satu kelompok berstatus Terlapor ÷ posyandu yang terdaftar di sheet tahun "
                   "tsb., pada bulan terakhir berdata")
    c3.metric("Posyandu < 5 kader", f"{int(((prof_f.jumlah_kader > 0) & (prof_f.jumlah_kader < 5)).sum())} / "
              f"{int((prof_f.jumlah_kader > 0).sum())}", help="Dihitung dari baris kader (snapshot 2026)")
    c4.metric("Alat standar lengkap (5 jenis)", f"{int((prof_f.jenis_alat_std_baik == 5).sum())} / {len(prof_f)}",
              help="Snapshot Juni 2026")
    kk = kel[(kel.tahun == 2025) & kel.nama_kecamatan.isin(kec_pilih)]
    rs = kk.penduduk_0_4.sum()
    c5.metric("Rasio stunting 2025", f"{100 * kk.jumlah_stunting.sum() / rs:.2f}%" if rs else "–",
              help="Jumlah balita stunting ÷ penduduk 0–4 tahun (rasio, bukan prevalensi)")

    kiri, kanan = st.columns([3, 2])
    with kiri:
        per_kec = (kel_th.groupby("nama_kecamatan")[["cakupan", "sasaran"]].sum().reset_index())
        per_kec["persen"] = (100 * per_kec.cakupan / per_kec.sasaran).round(1)
        per_kec = per_kec.sort_values("persen")
        fig = px.bar(per_kec, x="persen", y="nama_kecamatan", orientation="h", text="persen",
                     color_discrete_sequence=[SATU_SERI],
                     hover_data={"cakupan": ":,", "sasaran": ":,", "persen": ":.1f", "nama_kecamatan": False})
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", marker_line_width=0, width=0.6)
        st.plotly_chart(rapikan(fig, f"Cakupan {kelompok} per kecamatan, {tahun}", x="% cakupan"), width="stretch")
    with kanan:
        st.markdown("#### Insight utama")
        # Insight dihitung dari data (bukan teks tetap); pola Data → Temuan → Rekomendasi.
        th25 = kpi[(kpi.tahun == 2025)]
        def cak(df, k):
            d = df[df.nama_kelompok == k]
            return persen(d.cakupan.sum(), d.sasaran.sum())
        rendah = (th25[th25.nama_kelompok == "Bayi Balita"].groupby("nama_kecamatan")[["cakupan", "sasaran"]].sum())
        rendah = (100 * rendah.cakupan / rendah.sasaran).sort_values()
        k2 = klaster.label_cluster.value_counts()
        st.markdown(
            f"1. **Cakupan bayi balita 2025 {cak(th25, 'Bayi Balita')}%** (ibu hamil {cak(th25, 'Ibu Hamil')}%). "
            f"Terendah di **{rendah.index[0].title()} ({rendah.iloc[0]:.1f}%)** → prioritaskan sweeping balita "
            "dan pengingat jadwal di kecamatan ini.\n"
            f"2. **{int(profil.ada_alat_rusak_tdk_standar.sum())} dari {len(profil)} posyandu** punya alat rusak/tidak "
            f"standar; hanya **{int((profil.jenis_alat_std_baik == 5).sum())}** lengkap 5 jenis alat standar baik → "
            "pengadaan alat diarahkan ke cluster *alat standar kurang*.\n"
            f"3. **{int(kual[(kual.tahun == 2025) & (kual.item == 'Total baris anomali')].n_baris.sum())} baris "
            "laporan 2025 ditandai anomali** (Sasaran janggal, Cakupan > Sasaran) → validasi Sasaran di puskesmas "
            "sebelum angka dipakai untuk target.\n"
            f"4. Clustering menemukan **{len(k2)} profil posyandu**; terbesar: "
            f"*{k2.index[0]}* ({k2.iloc[0]} posyandu) → pembinaan bisa per profil, bukan seragam.\n"
            f"5. **{int(((profil.jumlah_kader > 0) & (profil.jumlah_kader < 5)).sum())} posyandu punya < 5 kader** "
            "(batas KPI rancangan UTS) → prioritaskan rekrutmen kader di posyandu tersebut.")

# ---------------------------------------------------------------- tab 2: tren
with tab2:
    tren = (kpi_f[kpi_f.nama_kelompok == kelompok].groupby(["waktu_key", "tahun", "bulan", "nama_kecamatan"])
            [["cakupan", "sasaran"]].sum().reset_index())
    tren = tren[tren.sasaran > 0]
    tren["persen"] = (100 * tren.cakupan / tren.sasaran).round(1)
    tren["periode"] = pd.to_datetime(dict(year=tren.tahun, month=tren.bulan, day=1))
    fig = px.line(tren, x="periode", y="persen", color="nama_kecamatan", color_discrete_map=WARNA_KEC,
                  markers=True, hover_data={"cakupan": ":,", "sasaran": ":,", "periode": "|%b %Y"},
                  labels={"nama_kecamatan": "Kecamatan"})
    fig.update_traces(line_width=2, marker_size=6)
    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(rapikan(fig, f"Cakupan {kelompok} per bulan (%)", y="% cakupan"), width="stretch")
    st.caption("Bulan tanpa sasaran tidak digambar (bukan 0). Kategori layanan berbeda antar tahun, sehingga "
               "perbandingan antar tahun untuk kelompok selain bayi balita & ibu hamil perlu hati-hati. "
               "Data 2026 baru sampai Agustus/September.")
    with st.expander("Tabel data tren"):
        st.dataframe(tren.drop(columns=["waktu_key", "periode"]), hide_index=True, width="stretch")

# ---------------------------------------------------------------- tab 3: ML
with tab3:
    terpilih = meval[meval.is_terpilih].iloc[0]
    st.markdown(f"**Model terpilih:** `{terpilih.model_id}` — K-Means, K={terpilih.k}, fitur internal v2. "
                f"Silhouette **{terpilih.silhouette:.3f}**, Davies-Bouldin **{terpilih.davies_bouldin:.3f}**, "
                f"stabilitas antar seed (ARI) **{terpilih.stabilitas_ari:.2f}**.")
    st.caption("Silhouette ≈ 0,2 berarti cluster saling tumpang tindih (struktur lemah): label cluster dipakai "
               "sebagai alat bantu menyusun prioritas, bukan klasifikasi pasti. Posyandu baru 2026 (tanpa data 2025) "
               "tidak ikut clustering.")
    gab = klaster.merge(profil, on=["posyandu_key", "posyandu_id"])
    gab_f = gab[gab.nama_kecamatan.isin(kec_pilih)]
    kiri, kanan = st.columns([2, 3])
    with kiri:
        uk = gab_f.groupby("label_cluster").size().reset_index(name="posyandu").sort_values("label_cluster")
        fig = px.bar(uk, x="posyandu", y="label_cluster", orientation="h", text="posyandu",
                     color_discrete_sequence=[SATU_SERI])
        fig.update_traces(textposition="outside", width=0.6)
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(rapikan(fig, "Jumlah posyandu per cluster (kecamatan terpilih)"), width="stretch")
    with kanan:
        arah = {"persen_ibu_hamil_2025": 1, "persen_bayi_balita_2025": 1, "persen_anomali_2025": -1,
                "rasio_alat_baik": 1, "jumlah_kader": 1, "persen_kader_terlatih": 1}
        nama_f = {"persen_ibu_hamil_2025": "Cakupan ibu hamil", "persen_bayi_balita_2025": "Cakupan bayi balita",
                  "persen_anomali_2025": "Anomali data (dibalik)", "rasio_alat_baik": "Alat standar lengkap",
                  "jumlah_kader": "Jumlah kader", "persen_kader_terlatih": "Kader terlatih"}
        kp = kprof[kprof.fitur.isin(arah)].copy()
        kp["z_baik"] = kp.apply(lambda r: arah[r.fitur] * r.z_score, axis=1)
        # Alat ditampilkan sebagai jumlah jenis (0–5), bukan rasio, agar mudah dibaca.
        kp["teks"] = kp.apply(lambda r: f"{r.rata_rata * 5:.1f}/5" if r.fitur == "rasio_alat_baik"
                              else f"{r.rata_rata:.1f}", axis=1)
        mat = kp.pivot(index="label_cluster", columns="fitur", values="z_baik")[list(arah)]
        teks = kp.pivot(index="label_cluster", columns="fitur", values="teks")[list(arah)]
        fig = go.Figure(go.Heatmap(z=mat.values, x=[nama_f[c] for c in mat.columns], y=mat.index, text=teks.values,
                                   texttemplate="%{text}", colorscale=DIVERGING, zmid=0, zmin=-2.5, zmax=2.5,
                                   xgap=2, ygap=2, colorbar=dict(title="z (arah baik)"),
                                   hovertemplate="%{y}<br>%{x}: rata-rata %{text}<br>z arah baik %{z:.2f}<extra></extra>"))
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(rapikan(fig, "Profil cluster: biru = lebih baik dari rata-rata kota, merah = lebih buruk"),
                        width="stretch")
    desk = kprof[["label_cluster", "deskripsi", "n_posyandu"]].drop_duplicates().sort_values("label_cluster")
    st.dataframe(desk.rename(columns={"label_cluster": "Cluster", "deskripsi": "Karakter (|z| ≥ 0,5)",
                                      "n_posyandu": "Posyandu (kota)"}), hide_index=True, width="stretch")

    pilih = st.selectbox("Lihat daftar posyandu pada cluster", sorted(gab_f.label_cluster.unique()))
    daftar = gab_f[gab_f.label_cluster == pilih][[
        "nama_posyandu", "nama_kelurahan", "nama_kecamatan", "persen_ibu_hamil_2025", "persen_bayi_balita_2025",
        "jenis_alat_std_baik", "jumlah_kader", "persen_kader_terlatih", "persen_anomali_2025", "kelompok_aturan"]]
    st.dataframe(daftar.sort_values("persen_bayi_balita_2025"), hide_index=True, width="stretch",
                 column_config={"nama_posyandu": "Posyandu", "nama_kelurahan": "Kelurahan", "nama_kecamatan": "Kecamatan",
                                "persen_ibu_hamil_2025": "% ibu hamil", "persen_bayi_balita_2025": "% bayi balita",
                                "jenis_alat_std_baik": "Jenis alat std baik (0–5)", "jumlah_kader": "Kader",
                                "persen_kader_terlatih": "% kader terlatih", "persen_anomali_2025": "% baris anomali",
                                "kelompok_aturan": "Baseline aturan"})

    with st.expander("Evaluasi model: baseline → improvement → perbandingan"):
        km = meval[meval.algoritma == "kmeans"].sort_values("k")
        fig = px.line(km, x="k", y="silhouette", color="set_fitur", markers=True,
                      color_discrete_sequence=KATEGORI[:3], labels={"set_fitur": "Set fitur"})
        base = meval[(meval.algoritma == "aturan") & (meval.set_fitur == "v2_internal")].silhouette.iloc[0]
        fig.add_hline(y=base, line_dash="dot", line_color="#52514e",
                      annotation_text=f"baseline aturan (v2) {base:.3f}", annotation_position="bottom right")
        fig.update_traces(line_width=2, marker_size=8)
        st.plotly_chart(rapikan(fig, "Silhouette K-Means per K (makin tinggi makin terpisah)", y="silhouette"),
                        width="stretch")
        st.dataframe(meval.drop(columns=["batch_id"]).sort_values(["set_fitur", "algoritma", "k"]), hide_index=True,
                     width="stretch")

# ---------------------------------------------------------------- tab 4: stunting
with tab4:
    th_st = st.radio("Tahun data stunting", sorted(kel.tahun.unique()), horizontal=True,
                     index=len(kel.tahun.unique()) - 1)
    ks = kel[(kel.tahun == th_st) & kel.nama_kecamatan.isin(kec_pilih)].sort_values("rasio_stunting_persen")
    fig = px.bar(ks, x="rasio_stunting_persen", y="nama_kelurahan", orientation="h", color="nama_kecamatan",
                 color_discrete_map=WARNA_KEC, labels={"nama_kecamatan": "Kecamatan"},
                 hover_data={"jumlah_stunting": True, "penduduk_0_4": ":,", "nama_kelurahan": False})
    fig.update_traces(marker_line_color="#fcfcfb", marker_line_width=1)
    fig.update_layout(height=max(400, 18 * len(ks)), legend=dict(orientation="h", y=-0.05))
    fig.update_yaxes(categoryorder="total ascending")  # urut rasio se-kota, warna tetap = kecamatan
    st.plotly_chart(rapikan(fig, f"Rasio stunting per kelurahan {th_st} (% balita stunting ÷ penduduk 0–4)",
                            x="rasio (%)"), width="stretch")
    st.caption("Rasio, bukan prevalensi: pembilang = balita stunting yang terukur (Dinkes), penyebut = penduduk "
               "terdaftar 0–4 tahun (Disdukcapil Sem. II 2025). Untuk 2024 penyebutnya pendekatan (buku 2024 tidak "
               "tersedia).")
    with st.expander("Tabel kelurahan"):
        st.dataframe(ks.drop(columns=["batch_id", "kelurahan_key"]), hide_index=True, width="stretch")

# ---------------------------------------------------------------- tab 5: kualitas
with tab5:
    kiri, kanan = st.columns(2)
    with kiri:
        st.markdown("#### Quality gate batch terakhir")
        g = gate.copy()
        g["hasil"] = g.lulus.map({True: "✔ lulus", False: "✖ gagal"})
        st.dataframe(g[["kode_cek", "hasil", "is_kritis", "nilai", "harapan"]], hide_index=True, width="stretch")
    with kanan:
        an = kual[(kual.kategori == "anomali") & (kual.tahun == tahun)].sort_values("n_baris")
        total = an[an.item == "Total baris anomali"].n_baris.sum()
        an = an[an.item != "Total baris anomali"]
        fig = px.bar(an, x="n_baris", y="item", orientation="h", text="n_baris", color_discrete_sequence=[SATU_SERI])
        fig.update_traces(textposition="outside", width=0.6)
        st.plotly_chart(rapikan(fig, f"Baris bertanda anomali {tahun}: total {total} baris (satu baris bisa >1 jenis)",
                                x="baris"), width="stretch")
        tot = kual[(kual.kategori == "anomali") & (kual.item == "Total baris anomali")].set_index("tahun").n_baris
        st.caption("Total baris anomali per tahun: " + ", ".join(f"{t}: {v}" for t, v in tot.items())
                   + f" (jumlah {tot.sum()})")
    stt = kual[kual.kategori == "status"].pivot(index="item", columns="tahun", values="n_baris").fillna(0).astype(int)
    st.markdown("#### Status data per tahun (baris posyandu × bulan × kelompok)")
    st.dataframe(stt, width="stretch")
    st.caption("Anomali ditandai, tidak dihapus. 'Cakupan tanpa sasaran' = Sasaran kosong tetapi ada pelayanan "
               "(K37); tidak masuk KPI cakupan.")
