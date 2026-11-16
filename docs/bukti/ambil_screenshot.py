"""Mengambil screenshot bukti UI dengan Playwright (dijalankan di container, bukan di laptop).

    docker compose --profile bukti run --rm screenshot dashboard
    docker compose --profile bukti run --rm screenshot airflow
    docker compose --profile bukti run --rm screenshot lake
    docker compose --profile bukti run --rm screenshot url <nama_berkas> <url_airflow_relatif>

Login memakai kredensial dari environment (.env) dan TIDAK pernah memotret halaman yang
menampilkan kata sandi (screenshot diambil setelah login). Hasil ditulis ke /bukti (= docs/bukti).
Setiap screenshot WAJIB diperiksa manual sebelum di-commit (tidak boleh ada data pribadi kader).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request

from playwright.sync_api import sync_playwright

AIRFLOW = "http://airflow-apiserver:8080"
LAKE = "http://datalake:9001"
DASH = "http://dashboard:8501"
DAG = "posyandu_pipeline"
UKURAN = {"width": 1600, "height": 1000}


def token_airflow() -> str:
    data = json.dumps({"username": os.environ["AIRFLOW_ADMIN_USER"],
                       "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}).encode()
    req = urllib.request.Request(f"{AIRFLOW}/auth/token", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)["access_token"]


def api(path: str, token: str):
    req = urllib.request.Request(f"{AIRFLOW}/api/v2{path}", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def halaman_airflow(p, token):
    ctx = p.chromium.launch().new_context(viewport=UKURAN)
    ctx.add_cookies([{"name": "_token", "value": token, "url": AIRFLOW}])
    return ctx.new_page()


def foto(page, nama: str, url: str, tunggu: int = 6000, penuh: bool = False) -> None:
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(tunggu)
    page.screenshot(path=f"/bukti/{nama}", full_page=penuh)
    print("disimpan", nama, "<-", url.split("?")[0])


def airflow(p, args) -> None:
    token = token_airflow()
    page = halaman_airflow(p, token)
    runs = api(f"/dags/{DAG}/dagRuns?order_by=-logical_date&limit=50", token)["dag_runs"]
    sukses = next(r for r in runs if r["state"] == "success")
    gagal = [r for r in runs if r["state"] == "failed"]
    foto(page, "30_airflow_daftar_dag.png", f"{AIRFLOW}/dags")
    # Tampilan Graph: tombol ikon di panel kiri (UI Airflow 3 tidak punya parameter URL untuk ini).
    page.goto(f"{AIRFLOW}/dags/{DAG}/runs/{sukses['dag_run_id']}", wait_until="networkidle")
    page.wait_for_timeout(3000)
    for nama_tombol in ("Show Graph", "Graph"):
        b = page.get_by_role("button", name=nama_tombol)
        if b.count():
            b.first.click()
            break
    else:
        page.locator("button[aria-label*='raph']").first.click()
    page.wait_for_timeout(6000)
    page.screenshot(path="/bukti/31_airflow_graph.png")
    print("disimpan 31_airflow_graph.png")
    foto(page, "32_airflow_grid_run_sukses.png", f"{AIRFLOW}/dags/{DAG}/runs/{sukses['dag_run_id']}")
    foto(page, "33_airflow_daftar_run.png", f"{AIRFLOW}/dags/{DAG}/runs")
    for r in gagal[:2]:
        nama = "34_airflow_run_gagal_" + ("karantina" if "quarantine" in json.dumps(
            api(f"/dags/{DAG}/dagRuns/{r['dag_run_id']}/taskInstances?state=failed", token)) else "db_retry") + ".png"
        foto(page, nama, f"{AIRFLOW}/dags/{DAG}/runs/{r['dag_run_id']}")
    db = next((r for r in gagal if r["dag_run_id"].startswith("uji_a")), None)
    if db:  # riwayat percobaan (retry) task yang gagal karena database mati
        foto(page, "35_airflow_retry_init_schema.png",
             f"{AIRFLOW}/dags/{DAG}/runs/{db['dag_run_id']}/tasks/init_schema")
    foto(page, "36_airflow_detail_dag_jadwal.png", f"{AIRFLOW}/dags/{DAG}/details")


def url(p, args) -> None:
    nama, rel = args[0], args[1]
    page = halaman_airflow(p, token_airflow())
    foto(page, nama, f"{AIRFLOW}{rel}")


def lake(p, args) -> None:
    page = p.chromium.launch().new_context(viewport=UKURAN).new_page()
    page.goto(f"{LAKE}/login", wait_until="networkidle")
    page.fill("#accessKey", os.environ["LAKE_ROOT_USER"])
    page.fill("#secretKey", os.environ["LAKE_ROOT_PASSWORD"])
    page.keyboard.press("Enter")
    page.wait_for_timeout(4000)
    foto(page, "37_lake_daftar_bucket.png", f"{LAKE}/browser")
    foto(page, "38_lake_raw_zone.png", f"{LAKE}/browser/raw")
    foto(page, "39_lake_raw_laporan.png", f"{LAKE}/browser/raw/internal_laporan_posyandu%2F")


def dashboard(p, args) -> None:
    page = p.chromium.launch().new_context(viewport={"width": 1600, "height": 1100}).new_page()
    page.goto(DASH, wait_until="networkidle")
    page.wait_for_timeout(8000)
    tab = ["Ringkasan & insight", "Tren cakupan", "Posyandu prioritas (ML)", "Stunting kelurahan", "Kualitas data"]
    for i, t in enumerate(tab):
        page.get_by_role("tab", name=t).click()
        page.wait_for_timeout(4000)
        page.screenshot(path=f"/bukti/{40 + i}_dashboard_{i + 1}.png", full_page=True)
        print("disimpan", f"{40 + i}_dashboard_{i + 1}.png")


if __name__ == "__main__":
    perintah = sys.argv[1] if len(sys.argv) > 1 else "dashboard"
    with sync_playwright() as p:
        {"airflow": airflow, "lake": lake, "dashboard": dashboard, "url": url}[perintah](p, sys.argv[2:])
