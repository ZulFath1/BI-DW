"""Dashboard Posyandu Kota Banjarmasin (Streamlit).

Sumber data: HANYA schema `mart` (dibangun dari DWH oleh task build_mart dan train_ml), dibaca dengan user
read-only DASHBOARD_USER. Tidak membaca berkas mentah, staging, atau dq. Tidak ada data pribadi kader:
kader hanya tampil sebagai jumlah per posyandu.

Cache: hasil query di-cache per batch_id terakhir di mart.dashboard_refresh. Task Airflow `refresh_dashboard`
menulis baris baru setiap run sukses, sehingga data baru langsung terbaca tanpa restart.

Filter global (sidebar): tahun, kecamatan, puskesmas, cluster, kelompok sasaran. Visual kunjungan dihitung
dari mart.kunjungan_posyandu_bulanan (grain posyandu × bulan × kelompok), sehingga semua filter berlaku pada
KPI, tren, insight, dan grafik kualitas. Filter yang tidak berlaku pada sebuah visual disebutkan di
keterangannya (mis. model ML memakai data 2025, sehingga filter tahun tidak mengubah hasil cluster).
"""

from __future__ import annotations

import os

import numpy as np
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
BELUM = "Belum dicluster (baru 2026)"
LAYOUT = dict(plot_bgcolor="#fcfcfb", paper_bgcolor="#fcfcfb", font=dict(color="#0b0b0b"),
              hoverlabel=dict(bgcolor="white"), margin=dict(l=10, r=60, t=40, b=10))
BIT_ANOMALI = {1: "A1 Sasaran >= 10.000", 2: "A2 Sasaran di atas ambang kelompok", 4: "A3 lonjakan > 5x median",
               8: "Cakupan > Sasaran", 16: "Nilai negatif", 32: "Sasaran 0, Cakupan > 0", 64: "Selisih persen > 1 poin"}


def rapikan(fig: go.Figure, judul: str, y: str | None = None, x: str | None = None) -> go.Figure:
    fig.update_layout(title=judul, **LAYOUT)
    fig.update_xaxes(showgrid=False, title=x, linecolor="#c3c2b7")
    fig.update_yaxes(gridcolor="#e8e7e2", title=y, zeroline=False)
    fig.update_traces(cliponaxis=False, selector=dict(type="bar"))  # label angka tidak terpotong
    return fig


def fmt(v, n=1) -> str:
    """Angka gaya Indonesia: 12.345,6."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "–"
    return f"{v:,.{n}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def persen(c, s):
    return 100 * c / s if s else None


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
    kunj = q("""SELECT posyandu_id, nama_kecamatan, puskesmas, tahun, bulan, kelompok_key, nama_kelompok, status_key,
                       nama_status, cakupan_kpi, sasaran_kpi, flag_anomali FROM mart.kunjungan_posyandu_bulanan""", BATCH)
    profil = q("SELECT * FROM mart.posyandu_profil", BATCH)
    kel = q("SELECT * FROM mart.kpi_kelurahan", BATCH)
    gate = q("SELECT * FROM mart.quality_gate_terakhir ORDER BY kode_cek", BATCH)
    klaster = q("SELECT * FROM mart.hasil_cluster", BATCH)
    kprof = q("SELECT * FROM mart.cluster_profil", BATCH)
    meval = q("SELECT * FROM mart.ml_evaluasi", BATCH)
except Exception as err:
    st.error(f"Gagal membaca data mart ({type(err).__name__}). Jalankan pipeline Airflow dulu.")
    st.stop()

if kunj.empty or profil.empty:
    st.warning("Data mart masih kosong. Jalankan DAG `posyandu_pipeline` di Airflow.")
    st.stop()

peta_cluster = dict(zip(klaster.posyandu_id, klaster.label_cluster))
kunj["cluster"] = kunj.posyandu_id.map(peta_cluster).fillna(BELUM)
profil["cluster"] = profil.posyandu_id.map(peta_cluster).fillna(BELUM)
LABEL_CLUSTER = sorted(klaster.label_cluster.unique())
WARNA_CLUSTER = {c: KATEGORI[i] for i, c in enumerate(LABEL_CLUSTER)} | {BELUM: "#8a8984"}

# ---------------------------------------------------------------- sidebar (filter global)
st.sidebar.header("Filter")
tahun_ada = sorted(kunj.tahun.unique())
tahun = st.sidebar.selectbox("Tahun", tahun_ada, index=tahun_ada.index(2025) if 2025 in tahun_ada else len(tahun_ada) - 1)
kec_pilih = st.sidebar.multiselect("Kecamatan", KECAMATAN, default=KECAMATAN)
pusk_ada = sorted(set(kunj[kunj.nama_kecamatan.isin(kec_pilih)].puskesmas)
                  | set(profil[profil.nama_kecamatan.isin(kec_pilih)].puskesmas))
pusk_pilih = st.sidebar.multiselect("Puskesmas", pusk_ada, placeholder="Semua puskesmas")
cl_pilih = st.sidebar.multiselect("Cluster (hasil ML)", LABEL_CLUSTER + [BELUM], placeholder="Semua cluster")
kelompok_ada = kunj[["kelompok_key", "nama_kelompok"]].drop_duplicates().sort_values("kelompok_key")
kelompok = st.sidebar.selectbox("Kelompok sasaran", list(kelompok_ada.nama_kelompok), index=1)
st.sidebar.caption("Puskesmas & cluster kosong = semua. KPI cakupan = Σ cakupan ÷ Σ sasaran; hanya baris Terlapor, "
                   "bukan anomali A1, sasaran > 0 (aturan K38).")
st.sidebar.caption(f"Data batch: `{BATCH}`  \nDiperbarui: {WAKTU:%d %b %Y %H:%M} UTC" if WAKTU else "")
if not kec_pilih:
    st.info("Pilih minimal satu kecamatan.")
    st.stop()


def saring(df: pd.DataFrame) -> pd.DataFrame:
    """Filter kecamatan, puskesmas, cluster (tahun disaring terpisah karena tidak semua tabel bertahun)."""
    m = df.nama_kecamatan.isin(kec_pilih)
    if pusk_pilih:
        m &= df.puskesmas.isin(pusk_pilih)
    if cl_pilih:
        m &= df.cluster.isin(cl_pilih)
    return df[m]


kunj_f = saring(kunj)
kunj_fy = kunj_f[kunj_f.tahun == tahun]
prof_f = saring(profil)
# Stunting & penduduk berlevel kelurahan: filter kecamatan & puskesmas (kelurahan wilayah kerjanya) berlaku,
# filter cluster tidak (satu kelurahan berisi posyandu dari beberapa cluster).
prof_wil = profil[profil.nama_kecamatan.isin(kec_pilih) & (profil.puskesmas.isin(pusk_pilih) if pusk_pilih else True)]
kel_f = kel[kel.set_index(["nama_kecamatan", "nama_kelurahan"]).index.isin(
    prof_wil.set_index(["nama_kecamatan", "nama_kelurahan"]).index)]
gab = klaster.merge(profil.drop(columns=["batch_id"]), on=["posyandu_key", "posyandu_id"])
gab_f = saring(gab.rename(columns={"cluster_y": "cluster"}).drop(columns=["cluster_x"]))
unit = "puskesmas" if (len(kec_pilih) == 1 or pusk_pilih) else "nama_kecamatan"
nama_unit = "puskesmas" if unit == "puskesmas" else "kecamatan"
if kunj_fy.empty:
    st.warning("Tidak ada data untuk kombinasi filter ini.")
    st.stop()


def cari_cluster(kata: str) -> str | None:
    return next((c for c in LABEL_CLUSTER if kata.lower() in c.lower()), None)


st.title("Dashboard Posyandu — Kota Banjarmasin")
st.caption("Sumber: data warehouse posyandu (laporan bulanan 2023–2026, alat antropometri Juni 2026, kader 2026), "
           "Disdukcapil Sem. II 2025, Dinkes Satu Data (stunting). Studi kasus tugas UAS — bukan dokumen resmi dinas.")
ringkas_filter = (f"Filter aktif: tahun {tahun} · {len(kec_pilih)} kecamatan · "
                  f"{'semua puskesmas' if not pusk_pilih else f'{len(pusk_pilih)} puskesmas'} · "
                  f"{'semua cluster' if not cl_pilih else f'{len(cl_pilih)} cluster'} · "
                  f"{kunj_fy.posyandu_id.nunique()} posyandu")
st.markdown(f"<span style='color:#52514e'>{ringkas_filter}</span>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Ringkasan & insight", "Tren cakupan", "Posyandu prioritas (ML)",
                                        "Stunting kelurahan", "Kualitas data"])


def kpi_cakupan(df: pd.DataFrame, nama_kelompok: str):
    d = df[df.nama_kelompok == nama_kelompok]
    return persen(d.cakupan_kpi.sum(), d.sasaran_kpi.sum())


# ---------------------------------------------------------------- tab 1: ringkasan
with tab1:
    kel_th = kunj_fy[kunj_fy.nama_kelompok == kelompok]
    bln = kunj_fy[kunj_fy.status_key == 1].bulan.max()
    akhir = kunj_fy[kunj_fy.bulan == bln] if pd.notna(bln) else kunj_fy.iloc[0:0]
    th_st = tahun if tahun in set(kel.tahun) else int(kel.tahun.max())
    kk = kel_f[kel_f.tahun == th_st]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"Cakupan {kelompok} {tahun}", f"{fmt(kpi_cakupan(kunj_fy, kelompok))}%",
              help="Σ cakupan ÷ Σ sasaran setahun (filter KPI K38)")
    c2.metric(f"Posyandu terlapor (bulan {int(bln) if pd.notna(bln) else '–'})",
              f"{akhir[akhir.status_key == 1].posyandu_id.nunique()} / {akhir.posyandu_id.nunique()}",
              help="Posyandu dengan minimal satu kelompok berstatus Terlapor ÷ posyandu yang terdaftar di sheet tahun "
                   "tsb., pada bulan terakhir berdata")
    c3.metric("Posyandu < 5 kader", f"{int(((prof_f.jumlah_kader > 0) & (prof_f.jumlah_kader < 5)).sum())} / "
              f"{int((prof_f.jumlah_kader > 0).sum())}", help="Snapshot kader 2026 (filter tahun tidak berlaku)")
    c4.metric("Alat standar lengkap (5 jenis)", f"{int((prof_f.jenis_alat_std_baik == 5).sum())} / {len(prof_f)}",
              help="Snapshot Juni 2026 (filter tahun tidak berlaku)")
    rs = kk.penduduk_0_4.sum()
    c5.metric(f"Rasio stunting {th_st}", f"{fmt(100 * kk.jumlah_stunting.sum() / rs, 2)}%" if rs else "–",
              help="Balita stunting ÷ penduduk 0–4 tahun kelurahan wilayah filter (rasio, bukan prevalensi; "
                   "filter cluster tidak berlaku)")

    per = kel_th.groupby(unit)[["cakupan_kpi", "sasaran_kpi"]].sum().reset_index()
    per = per[per.sasaran_kpi > 0]
    per["persen"] = (100 * per.cakupan_kpi / per.sasaran_kpi).round(1)
    per = per.sort_values("persen")
    fig = px.bar(per, x="persen", y=unit, orientation="h", text="persen", color_discrete_sequence=[SATU_SERI],
                 hover_data={"cakupan_kpi": ":,", "sasaran_kpi": ":,", "persen": ":.1f", unit: False},
                 labels={"cakupan_kpi": "Σ cakupan", "sasaran_kpi": "Σ sasaran"})
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", marker_line_width=0, width=0.6)
    fig.update_layout(height=max(320, 26 * len(per) + 80))
    st.plotly_chart(rapikan(fig, f"Cakupan {kelompok} per {nama_unit}, {tahun}", x="% cakupan"), width="stretch")

    # ---------------- insight: Data → Temuan → Interpretasi → Impact → Rekomendasi (dihitung dari data terfilter)
    st.markdown("### Insight")
    st.caption("Setiap insight dihitung ulang dari data sesuai filter. Pola: Data → Temuan → Interpretasi → "
               "Impact → Rekomendasi.")

    def kartu(judul: str, data: str, temuan: str, interpretasi: str, impact: str, rekomendasi: str):
        with st.container(border=True):
            st.markdown(f"**{judul}**")
            st.markdown(f"- **Data:** {data}\n- **Temuan:** {temuan}\n- **Interpretasi:** {interpretasi}\n"
                        f"- **Impact:** {impact}\n- **Rekomendasi:** {rekomendasi}")

    insight = []
    # 1. Cakupan balita
    bb = kpi_cakupan(kunj_fy, "Bayi Balita")
    ih = kpi_cakupan(kunj_fy, "Ibu Hamil")
    pb = kunj_fy[kunj_fy.nama_kelompok == "Bayi Balita"].groupby(unit)[["cakupan_kpi", "sasaran_kpi"]].sum()
    pb = (100 * pb.cakupan_kpi / pb.sasaran_kpi.replace(0, np.nan)).dropna().sort_values()
    cl_rendah = [c for c in LABEL_CLUSTER if "cakupan rendah" in c.lower()]
    n_rendah = int(prof_f.cluster.isin(cl_rendah).sum())
    if bb is not None and len(pb):
        insight.append(("1. Cakupan penimbangan balita belum merata",
                        f"laporan bulanan {tahun}, kelompok bayi balita & ibu hamil, filter KPI K38.",
                        f"cakupan bayi balita **{fmt(bb)}%** (ibu hamil {fmt(ih)}%); terendah di "
                        f"**{pb.index[0].title()} ({fmt(pb.iloc[0])}%)**, tertinggi {pb.index[-1].title()} "
                        f"({fmt(pb.iloc[-1])}%).",
                        f"sekitar {round((100 - bb) / 10)} dari 10 balita sasaran tidak datang ditimbang setiap bulan.",
                        "gangguan pertumbuhan dan stunting terlambat terdeteksi; target cakupan program gizi tidak "
                        "tercapai.",
                        f"sweeping & kunjungan rumah dimulai di {pb.index[0].title()}, khususnya **{n_rendah} posyandu** "
                        f"di cluster cakupan rendah (daftar di tab *Posyandu prioritas*)."))
    # 2. Sasaran vs penduduk
    k25 = kel_f[kel_f.tahun == 2025]
    sas, pend = k25.sasaran_bayi_balita_des.sum(), k25.penduduk_0_4.sum()
    if pend:
        insight.append(("2. Sasaran balita posyandu jauh di bawah jumlah penduduk balita",
                        "Σ Sasaran bayi balita Desember 2025 (laporan posyandu) vs penduduk 0–4 tahun Disdukcapil "
                        "Sem. II 2025, kelurahan wilayah filter.",
                        f"Sasaran {fmt(sas, 0)} balita = **{fmt(100 * sas / pend, 0)}%** dari {fmt(pend, 0)} penduduk "
                        "0–4 tahun.",
                        "banyak balita terdaftar tidak tercatat sebagai sasaran posyandu (dilayani di tempat lain, "
                        "pindah, atau tidak terdata).",
                        "cakupan terlihat lebih tinggi dari kenyataan karena penyebutnya kecil; balita di luar daftar "
                        "tidak terpantau sama sekali.",
                        "puskesmas menyelaraskan daftar sasaran dengan data Disdukcapil per kelurahan setiap semester."))
    # 3. Alat
    if len(prof_f):
        kurang = int((5 - prof_f.jenis_alat_std_baik.fillna(0)).clip(lower=0).sum())
        cl_alat = cari_cluster("alat")
        n_alat = int((prof_f.cluster == cl_alat).sum()) if cl_alat else 0
        insight.append(("3. Alat antropometri standar belum lengkap",
                        "snapshot alat antropometri Juni 2026 (5 jenis alat standar).",
                        f"**{int(prof_f.ada_alat_rusak_tdk_standar.fillna(False).sum())} dari {len(prof_f)} posyandu** "
                        f"punya alat rusak/tidak standar; hanya {int((prof_f.jenis_alat_std_baik == 5).sum())} lengkap "
                        f"5 jenis; kekurangan minimal **{fmt(kurang, 0)} unit** (1 unit per jenis yang belum ada).",
                        "pengukuran berat/panjang/tinggi badan di banyak posyandu memakai alat tidak standar atau rusak.",
                        "status gizi dan stunting bisa salah klasifikasi, sehingga intervensi salah sasaran.",
                        f"pengadaan alat diprioritaskan untuk **{n_alat} posyandu** cluster *{cl_alat or 'alat kurang'}*"
                        " lebih dulu, lalu posyandu lain yang belum lengkap."))
    # 4. Kualitas data
    ter = kunj_fy[kunj_fy.status_key == 1]
    n_an = int((kunj_fy.flag_anomali > 0).sum())
    pp = ter.groupby("puskesmas").flag_anomali.apply(lambda s: 100 * (s > 0).mean()).sort_values(ascending=False)
    if len(pp):
        insight.append(("4. Angka Sasaran di sebagian laporan tidak wajar",
                        f"penanda anomali T09/T10 pada laporan {tahun} (ditandai, tidak dihapus).",
                        f"**{fmt(n_an, 0)} baris** bertanda anomali; tertinggi di Puskesmas "
                        + ", ".join(f"{p.title()} ({fmt(v)}%)" for p, v in pp.head(3).items()) + " dari baris Terlapor.",
                        "angka Sasaran/Cakupan di laporan tersebut tidak bisa langsung dipakai sebagai penyebut target.",
                        "KPI cakupan bias dan alokasi logistik (PMT, vaksin) bisa salah hitung.",
                        "puskesmas dengan anomali tertinggi memverifikasi Sasaran sebelum laporan direkap; dashboard "
                        "tab *Kualitas data* dipakai sebagai daftar periksa bulanan."))
    # 5. Kader
    if len(prof_f) and (prof_f.jumlah_kader > 0).any():
        cl_lat = cari_cluster("kader belum terlatih")
        n_lat = int((prof_f.cluster == cl_lat).sum()) if cl_lat else 0
        pk = 100 * prof_f.kader_terlatih.sum() / max(prof_f.jumlah_kader.sum(), 1)
        insight.append(("5. Pelatihan kader tertinggal dari jumlahnya",
                        "snapshot kader 2026 (jumlah per posyandu, tanpa data pribadi).",
                        f"baru **{fmt(pk, 0)}%** kader ikut pelatihan 25 keterampilan; "
                        f"{int(((prof_f.jumlah_kader > 0) & (prof_f.jumlah_kader < 5)).sum())} posyandu punya < 5 kader; "
                        f"**{n_lat} posyandu** cakupannya baik tetapi kadernya hampir semua belum terlatih.",
                        "kinerja posyandu tersebut bergantung pada kader berpengalaman, bukan pada keterampilan terstandar.",
                        "mutu pengukuran dan konseling gizi rentan turun saat kader berganti.",
                        f"kuota pelatihan diarahkan ke cluster *{cl_lat or 'kader belum terlatih'}*; rekrut kader baru "
                        "di posyandu yang kadernya < 5."))
    kol = st.columns(2)
    for i, ins in enumerate(insight):
        with kol[i % 2]:
            kartu(*ins)

# ---------------------------------------------------------------- tab 2: tren
with tab2:
    tren = (kunj_fy[kunj_fy.nama_kelompok == kelompok].groupby(["bulan", "nama_kecamatan"])
            [["cakupan_kpi", "sasaran_kpi"]].sum().reset_index())
    tren = tren[tren.sasaran_kpi > 0]
    tren["persen"] = (100 * tren.cakupan_kpi / tren.sasaran_kpi).round(1)
    tren["periode"] = pd.to_datetime(dict(year=tahun, month=tren.bulan, day=1))
    fig = px.line(tren, x="periode", y="persen", color="nama_kecamatan", color_discrete_map=WARNA_KEC, markers=True,
                  hover_data={"cakupan_kpi": ":,", "sasaran_kpi": ":,", "periode": "|%b %Y"},
                  labels={"nama_kecamatan": "Kecamatan", "cakupan_kpi": "Σ cakupan", "sasaran_kpi": "Σ sasaran"})
    fig.update_traces(line_width=2, marker_size=8)
    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(rapikan(fig, f"Cakupan {kelompok} per bulan, {tahun} (%)", y="% cakupan"), width="stretch")

    lk = kunj_fy.groupby(["bulan", "nama_kecamatan"]).agg(
        terdaftar=("posyandu_id", "nunique"),
        terlapor=("posyandu_id", lambda s: s[kunj_fy.loc[s.index, "status_key"] == 1].nunique())).reset_index()
    lk["persen"] = (100 * lk.terlapor / lk.terdaftar).round(1)
    lk["periode"] = pd.to_datetime(dict(year=tahun, month=lk.bulan, day=1))
    fig = px.line(lk, x="periode", y="persen", color="nama_kecamatan", color_discrete_map=WARNA_KEC, markers=True,
                  hover_data={"terlapor": True, "terdaftar": True, "periode": "|%b %Y"},
                  labels={"nama_kecamatan": "Kecamatan"})
    fig.update_traces(line_width=2, marker_size=8)
    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.15))
    fig.update_yaxes(range=[0, 105])
    st.plotly_chart(rapikan(fig, f"Kelengkapan laporan per bulan, {tahun} (% posyandu terlapor)", y="% terlapor"),
                    width="stretch")
    st.caption("Bulan tanpa sasaran tidak digambar (bukan 0). Kategori layanan berbeda antartahun, sehingga "
               "perbandingan antartahun untuk kelompok selain bayi balita & ibu hamil perlu hati-hati. Data 2026 baru "
               "sampai Agustus/September (bulan setelahnya = Belum dilaporkan).")
    with st.expander("Tabel data tren"):
        st.dataframe(tren.drop(columns=["periode"]).merge(lk.drop(columns=["periode"]), on=["bulan", "nama_kecamatan"],
                                                          suffixes=("_cakupan", "_terlapor")),
                     hide_index=True, width="stretch")

# ---------------------------------------------------------------- tab 3: ML
with tab3:
    terpilih = meval[meval.is_terpilih].iloc[0]
    st.markdown(f"**Model terpilih:** `{terpilih.model_id}` — K-Means, K={terpilih.k}, 6 fitur internal (cakupan "
                f"ibu hamil & bayi balita 2025, % baris anomali, alat standar, jumlah kader, % kader terlatih). "
                f"Silhouette **{fmt(terpilih.silhouette, 3)}**, Davies-Bouldin **{fmt(terpilih.davies_bouldin, 3)}**, "
                f"stabilitas antar seed (ARI) **{fmt(terpilih.stabilitas_ari, 2)}**.")
    st.caption("Filter kecamatan, puskesmas, dan cluster berlaku di tab ini; filter tahun tidak (model memakai data "
               "2025). Silhouette ≈ 0,2 = cluster saling tumpang tindih: label dipakai sebagai alat bantu prioritas, "
               "bukan klasifikasi pasti. 3 posyandu baru 2026 belum ikut clustering.")

    # Daftar posyandu prioritas
    st.markdown("#### Daftar posyandu prioritas")
    a, b, c = st.columns([2, 2, 1])
    tingkat_pilih = a.multiselect("Tingkat prioritas", ["Tinggi", "Sedang", "Rendah"], default=["Tinggi"])
    n_top = b.slider("Tampilkan", 10, 200, 25, step=5)
    pr = gab_f[gab_f.tingkat_prioritas.isin(tingkat_pilih)].sort_values("peringkat_prioritas")
    rek = kprof[["label_cluster", "rekomendasi"]].drop_duplicates()
    pr = pr.merge(rek, on="label_cluster", how="left")
    tampil = pr.head(n_top)[["peringkat_prioritas", "tingkat_prioritas", "nama_posyandu", "nama_kelurahan",
                             "nama_kecamatan", "puskesmas", "label_cluster", "alasan_prioritas", "skor_kebutuhan",
                             "rekomendasi"]]
    c.metric("Posyandu di tingkat terpilih", f"{len(pr)}")
    st.dataframe(tampil, hide_index=True, width="stretch", column_config={
        "peringkat_prioritas": st.column_config.NumberColumn("Peringkat kota", help="1 = paling prioritas se-kota"),
        "tingkat_prioritas": "Tingkat", "nama_posyandu": "Posyandu", "nama_kelurahan": "Kelurahan",
        "nama_kecamatan": "Kecamatan", "puskesmas": "Puskesmas", "label_cluster": "Cluster",
        "alasan_prioritas": st.column_config.TextColumn("Alasan (indikator terburuk)", width="large"),
        "skor_kebutuhan": st.column_config.ProgressColumn("Skor kebutuhan", min_value=-1.5, max_value=1.5,
                                                          format="%.2f", help="Rata-rata z-score arah dibalik; >0 = "
                                                          "di bawah rata-rata kota"),
        "rekomendasi": st.column_config.TextColumn("Rekomendasi (cluster)", width="large")})
    st.download_button("Unduh daftar prioritas (CSV)", pr.drop(columns=["batch_id"], errors="ignore")[tampil.columns]
                       .to_csv(index=False).encode("utf-8"), "posyandu_prioritas.csv", "text/csv")
    st.caption("Urutan: tingkat cluster (Tinggi = cakupan rendah; Sedang = alat, pencatatan, pelatihan; Rendah = "
               "percontohan), lalu skor kebutuhan posyandu. Daftar ini alat bantu; keputusan akhir tetap perlu "
               "verifikasi lapangan oleh puskesmas.")

    kiri, kanan = st.columns([2, 3])
    with kiri:
        uk = gab_f.groupby("label_cluster").size().reindex(LABEL_CLUSTER, fill_value=0).reset_index(name="posyandu")
        fig = px.bar(uk, x="posyandu", y="label_cluster", orientation="h", text="posyandu",
                     color_discrete_sequence=[SATU_SERI], labels={"label_cluster": ""})
        fig.update_traces(textposition="outside", width=0.6)
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(rapikan(fig, "Jumlah posyandu per cluster (sesuai filter)"), width="stretch")
    with kanan:
        # Profil dihitung ulang dari posyandu yang lolos filter; z terhadap rata-rata seluruh posyandu ber-cluster.
        fitur = {"persen_ibu_hamil_2025": ("Cakupan ibu hamil", 1), "persen_bayi_balita_2025": ("Cakupan bayi balita", 1),
                 "persen_anomali_2025": ("Anomali data (dibalik)", -1), "jenis_alat_std_baik": ("Alat standar (0–5)", 1),
                 "jumlah_kader": ("Jumlah kader", 1), "persen_kader_terlatih": ("Kader terlatih", 1)}
        dasar = gab.copy()
        for f in fitur:
            dasar[f] = pd.to_numeric(dasar[f], errors="coerce").astype(float)
            if f.startswith("persen_"):
                dasar[f] = dasar[f].clip(0, 100)
        dasar.loc[dasar.jumlah_kader == 0, "jumlah_kader"] = np.nan  # 0 = tidak ada data kader (K58)
        mu, sd = dasar[list(fitur)].mean(), dasar[list(fitur)].std(ddof=0).replace(0, 1)
        sub = dasar[dasar.posyandu_id.isin(gab_f.posyandu_id)]
        rata = sub.groupby("label_cluster")[list(fitur)].mean().reindex([c for c in LABEL_CLUSTER
                                                                         if c in set(sub.label_cluster)])
        if len(rata):
            z = (rata - mu) / sd * pd.Series({f: a for f, (_, a) in fitur.items()})
            fig = go.Figure(go.Heatmap(z=z.values, x=[fitur[f][0] for f in z.columns], y=z.index,
                                       text=rata.map(lambda v: fmt(v)).values, texttemplate="%{text}",
                                       colorscale=DIVERGING, zmid=0, zmin=-2.5, zmax=2.5, xgap=2, ygap=2,
                                       colorbar=dict(title="z (arah baik)"),
                                       hovertemplate="%{y}<br>%{x}: rata-rata %{text}<br>z arah baik %{z:.2f}"
                                                     "<extra></extra>"))
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(rapikan(fig, "Profil cluster (sesuai filter): biru = lebih baik dari rata-rata kota, "
                                         "merah = lebih buruk"), width="stretch")

    st.markdown("#### Karakter dan rekomendasi tiap cluster")
    desk = (kprof[["label_cluster", "tingkat_prioritas", "n_posyandu", "karakter", "rekomendasi"]].drop_duplicates()
            .sort_values("label_cluster"))
    if cl_pilih:
        desk = desk[desk.label_cluster.isin(cl_pilih)]
    st.dataframe(desk, hide_index=True, width="stretch", column_config={
        "label_cluster": "Cluster", "tingkat_prioritas": "Prioritas", "n_posyandu": "Posyandu (kota)",
        "karakter": st.column_config.TextColumn("Karakter", width="large"),
        "rekomendasi": st.column_config.TextColumn("Rekomendasi tindakan", width="large")})

    with st.expander("Evaluasi model: baseline → improvement → perbandingan (tidak dipengaruhi filter)"):
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
    th_st = tahun if tahun in set(kel.tahun) else int(kel.tahun.max())
    if th_st != tahun:
        st.info(f"Data stunting hanya tersedia {', '.join(map(str, sorted(kel.tahun.unique())))}; ditampilkan {th_st}.")
    ks = kel_f[kel_f.tahun == th_st].sort_values("rasio_stunting_persen")
    fig = px.bar(ks, x="rasio_stunting_persen", y="nama_kelurahan", orientation="h", color="nama_kecamatan",
                 color_discrete_map=WARNA_KEC, labels={"nama_kecamatan": "Kecamatan"},
                 hover_data={"jumlah_stunting": True, "penduduk_0_4": ":,", "nama_kelurahan": False})
    fig.update_traces(marker_line_color="#fcfcfb", marker_line_width=1)
    fig.update_layout(height=max(400, 18 * len(ks)), legend=dict(orientation="h", y=-0.05))
    fig.update_yaxes(categoryorder="total ascending")  # urut rasio se-kota, warna tetap = kecamatan
    st.plotly_chart(rapikan(fig, f"Rasio stunting per kelurahan {th_st} (% balita stunting ÷ penduduk 0–4)",
                            x="rasio (%)"), width="stretch")
    st.caption("Filter kecamatan & puskesmas berlaku (kelurahan wilayah kerja); filter cluster tidak berlaku karena "
               "satu kelurahan berisi posyandu dari beberapa cluster. Rasio, bukan prevalensi: pembilang = balita "
               "stunting yang terukur (Dinkes), penyebut = penduduk terdaftar 0–4 tahun (Disdukcapil Sem. II 2025). "
               "Untuk 2024 penyebutnya pendekatan (buku 2024 tidak tersedia).")
    with st.expander("Tabel kelurahan"):
        st.dataframe(ks.drop(columns=["batch_id", "kelurahan_key"]), hide_index=True, width="stretch")

# ---------------------------------------------------------------- tab 5: kualitas
with tab5:
    kiri, kanan = st.columns(2)
    with kiri:
        an = pd.DataFrame([{"item": nama, "n_baris": int((kunj_fy.flag_anomali & bit > 0).sum())}
                           for bit, nama in BIT_ANOMALI.items()]).sort_values("n_baris")
        total = int((kunj_fy.flag_anomali > 0).sum())
        fig = px.bar(an, x="n_baris", y="item", orientation="h", text="n_baris", color_discrete_sequence=[SATU_SERI],
                     labels={"item": "", "n_baris": "baris"})
        fig.update_traces(textposition="outside", width=0.6)
        st.plotly_chart(rapikan(fig, f"Baris bertanda anomali {tahun}: total {fmt(total, 0)} (satu baris bisa >1 jenis)",
                                x="baris"), width="stretch")
    with kanan:
        stt = kunj_fy.groupby("nama_status").size().sort_values().reset_index(name="n_baris")
        fig = px.bar(stt, x="n_baris", y="nama_status", orientation="h", text="n_baris",
                     color_discrete_sequence=[SATU_SERI], labels={"nama_status": "", "n_baris": "baris"})
        fig.update_traces(textposition="outside", width=0.6)
        st.plotly_chart(rapikan(fig, f"Status data {tahun} (baris posyandu × bulan × kelompok)", x="baris"),
                        width="stretch")
    pa = (kunj_fy[kunj_fy.status_key == 1].groupby(unit)
          .agg(baris_terlapor=("flag_anomali", "size"), baris_anomali=("flag_anomali", lambda s: int((s > 0).sum())))
          .reset_index())
    pa["persen_anomali"] = (100 * pa.baris_anomali / pa.baris_terlapor).round(1)
    st.markdown(f"#### Anomali per {nama_unit}, {tahun} (baris Terlapor)")
    st.dataframe(pa.sort_values("persen_anomali", ascending=False), hide_index=True, width="stretch",
                 column_config={unit: nama_unit.title(), "persen_anomali": st.column_config.ProgressColumn(
                     "% baris anomali", min_value=0, max_value=max(30.0, float(pa.persen_anomali.max() or 0)),
                     format="%.1f%%")})
    with st.expander("Status data per tahun (semua tahun, filter wilayah & cluster berlaku)"):
        st.dataframe(kunj_f.pivot_table(index="nama_status", columns="tahun", values="posyandu_id", aggfunc="size",
                                        fill_value=0), width="stretch")
    st.markdown("#### Quality gate batch terakhir (tingkat batch, tidak dipengaruhi filter)")
    g = gate.copy()
    g["hasil"] = g.lulus.map({True: "✔ lulus", False: "✖ gagal"})
    st.dataframe(g[["kode_cek", "hasil", "is_kritis", "nilai", "harapan"]], hide_index=True, width="stretch")
    st.caption("Anomali ditandai, tidak dihapus. 'Cakupan tanpa sasaran' = Sasaran kosong tetapi ada pelayanan "
               "(K37); tidak masuk KPI cakupan.")
