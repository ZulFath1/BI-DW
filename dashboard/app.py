"""Dashboard Posyandu (Streamlit) — tahap awal: cek koneksi.

Pada tahap ini DWH belum berisi data. Halaman ini hanya membuktikan bahwa:
  1. Streamlit berjalan di container;
  2. Streamlit terhubung ke PostgreSQL memakai user read-only (DASHBOARD_USER);
  3. User tersebut hanya bisa melihat schema dw dan mart.
KPI, grafik, filter, dan hasil ML ditambahkan setelah data mart terbentuk.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

st.set_page_config(page_title="Dashboard Posyandu", layout="wide")


@st.cache_resource
def get_engine():
    """Koneksi ke DWH dengan user read-only. Kredensial dari environment (.env)."""
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["DASHBOARD_USER"],
        password=os.environ["DASHBOARD_PASSWORD"],
        host=os.environ.get("DW_HOST", "postgres"),
        port=int(os.environ.get("DW_PORT", "5432")),
        database=os.environ["DW_DB"],
    )
    return create_engine(url, pool_pre_ping=True)


st.title("Dashboard Posyandu — Kota Banjarmasin")
st.caption("Tahap awal: cek koneksi ke data warehouse. Belum ada data yang ditampilkan.")

try:
    with get_engine().connect() as conn:
        info = conn.execute(
            text(
                "SELECT current_user AS pengguna, current_database() AS basis_data, "
                "current_setting('default_transaction_read_only') AS read_only"
            )
        ).mappings().one()
        schema = pd.read_sql(
            text(
                "SELECT nspname AS schema, has_schema_privilege(nspname, 'USAGE') AS bisa_dibaca "
                "FROM pg_namespace WHERE nspname IN ('stg','curated','dq','dw','mart') "
                "ORDER BY nspname"
            ),
            conn,
        )
        tabel = pd.read_sql(
            text(
                "SELECT table_schema AS schema, count(*) AS jumlah_tabel "
                "FROM information_schema.tables WHERE table_schema IN ('dw','mart') "
                "GROUP BY table_schema ORDER BY table_schema"
            ),
            conn,
        )
except Exception as err:  # tampilkan pesan yang jelas, bukan traceback
    st.error(f"Gagal terhubung ke data warehouse: {type(err).__name__}")
    st.stop()

st.success(
    f"Terhubung sebagai **{info['pengguna']}** ke **{info['basis_data']}** "
    f"(read-only: {info['read_only']})"
)
st.subheader("Akses schema")
st.dataframe(schema, hide_index=True)
st.subheader("Isi schema dw dan mart")
if tabel.empty:
    st.info("Schema dw dan mart masih kosong. Isi akan muncul setelah pipeline dijalankan.")
else:
    st.dataframe(tabel, hide_index=True)
