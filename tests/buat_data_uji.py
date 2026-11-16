"""Membuat SALINAN UJI berisi data buruk untuk menguji quality gate & karantina di DAG run nyata.

    docker compose exec airflow-scheduler python -m tests.buat_data_uji

Berkas asli di data/source/ TIDAK diubah (di-mount read-only). Salinan ditulis ke
data/uji/Laporan_uji_data_buruk.xlsx (folder data/ diabaikan git; salinan tetap data privat).

Kerusakan yang disengaja (sheet 2025):
  1. baris posyandu pertama DIGANDAKAN di bawah baris terakhir  -> kunci posyandu ganda -> Q02 (kritis) gagal
  2. satu sel "% Dilayani" diisi teks 'salah ketik'              -> Q03 (peringatan) gagal
Tidak ada isi sheet Kader yang dicetak ke terminal.

Catatan: workbook dibuka dengan data_only=True sehingga rumus diganti nilai terakhirnya
(salinan berisi nilai saja). Struktur header tetap sama sehingga staging tetap berjalan.
"""

from __future__ import annotations

import openpyxl

from common.config import BERKAS_UJI, SUMBER_DIR, UJI_DIR

ASLI = SUMBER_DIR / "Laporan Kunjungan Posyandu.xlsx"
SHEET = "2025"


def main() -> None:
    UJI_DIR.mkdir(parents=True, exist_ok=True)
    tujuan = UJI_DIR / BERKAS_UJI["data_buruk"]
    wb = openpyxl.load_workbook(ASLI, data_only=True)
    ws = wb[SHEET]
    # Baris data pertama = baris setelah 3 baris header yang diawali sel "No."
    header = next(r for r in range(1, 11) if str(ws.cell(r, 1).value or "").strip() == "No.")
    pertama = header + 3
    terakhir = max(r for r in range(pertama, ws.max_row + 1) if ws.cell(r, 5).value is not None)
    baru = terakhir + 1
    for c in range(1, ws.max_column + 1):
        ws.cell(baru, c).value = ws.cell(pertama, c).value
    ws.cell(baru, 1).value = (ws.cell(terakhir, 1).value or 0) + 1 if isinstance(ws.cell(terakhir, 1).value, int) else None
    # Kolom % Dilayani Januari, Ibu Hamil = kolom ke-8 (indeks 1-based: 5 identitas + Sasaran, Cakupan, %)
    ws.cell(pertama + 1, 8).value = "salah ketik"
    wb.save(tujuan)
    print(f"salinan uji ditulis: {tujuan.name}; sheet {SHEET}: baris {pertama} digandakan ke baris {baru}; "
          f"sel H{pertama + 1} = teks")


if __name__ == "__main__":
    main()
