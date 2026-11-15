# quality/

**Quality gate** Q01–Q09 (`quality_gate.py`). Hasil setiap pemeriksaan disimpan di `dq.quality_result`
dan ringkasannya di `dq.dq_log` (tahap `quality`).

| Kode | Pemeriksaan | Kritis | Fase | Hasil 15 Nov 2026 |
|---|---|---|---|---|
| Q01 | Baris panjang per tahun = posyandu × 60 | tidak | sebelum load | 23.700 / 23.700 / 29.220 / 29.400 ✔ |
| Q02 | Kunci posyandu unik per tahun; satu versi aktif; versi SCD tidak tumpang tindih | **ya** | sebelum load | 0 ✔ |
| Q03 | Persen non-numerik tersisa | tidak | sebelum load | 0 ✔ |
| Q04 | Σ Sasaran & Σ Cakupan staging = fakta per tahun | **ya** | dalam transaksi load | selisih 0 ✔ |
| Q05 | FK tanpa pasangan, versi SCD tidak berlaku, baris hilang saat lookup | **ya** | dalam transaksi load | 0 ✔ |
| Q06 | Tidak ada kolom nama/alamat/usia kader di `dw` dan `mart` | **ya** | dalam transaksi load | tidak ada ✔ |
| Q07 | Kelurahan eksternal (E1, E2) dan kelurahan posyandu terpetakan | tidak | sebelum load | 0 ✔ |
| Q08 | Σ penduduk 0–4 kelurahan = total kecamatan (Tabel 7) dan kota (Tabel 6) | tidak | sebelum load | selisih 0 ✔ |
| Q09 | Tidak ada jenis kelamin kosong di fakta kader; fakta + T13 + ditahan = staging | tidak | sebelum load | 2.542 + 2 + 1 = 2.545 ✔ |

- Kritis gagal **sebelum load** → DAG bercabang ke `quarantine`; batch dicatat di `dq.quarantine`; DWH
  tidak disentuh.
- Kritis gagal **dalam load** → `ROLLBACK`; DWH persis seperti sebelum batch; `load_dw` GAGAL.
- Tidak kritis gagal → PERINGATAN di `dq_log`, batch tetap lanjut.

Uji: `python -m tests.uji_quality_gate` (E6 rollback Q06, E7 quarantine Q02, E8 idempotensi).
