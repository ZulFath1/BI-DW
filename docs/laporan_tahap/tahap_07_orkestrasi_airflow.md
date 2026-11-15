# Laporan Tahap 7 — Orkestrasi Airflow (DAG `posyandu_pipeline`) dan perubahan `ingest_web`

- **Tanggal:** 15 Nov 2026 · **Jadwal:** minggu 23–29 Nov · **Mode:** otonom
- **Status:** selesai dan dites; DAG aktif (tidak di-pause) dan sudah berjalan 3 kali dengan sukses

## 1. Tujuan

Menjalankan seluruh pipeline (ingestion → staging → transform → quality gate → load) sebagai DAG Airflow
terjadwal dengan retry, callback gagal, dan cabang karantina; serta menerapkan arahan pemilik untuk
`ingest_web` (berubah/tidak dari checksum isi tabel).

## 2. Yang dibuat / diubah

| Berkas | Isi |
|---|---|
| `dags/posyandu_pipeline.py` (baru) | DAG 13 task: `init_schema`, 3 ingest, task group `load_staging` (5), `transform`, `quality_gate` (branch), `load_dw`, `quarantine` |
| `dags/README.md` | Diagram, pengaturan jadwal/retry/callback |
| `ingestion/ingest_web.py` | Checksum isi tabel (JSON kanonik); `tidak_berubah` tanpa objek baru (K24) |
| `common/lake.py` | `catat_manifest(..., sha256_isi_tabel)`, `manifest_terakhir_isi_tabel`, `objek_ada` |
| `README.md`, `docs/bukti/README.md` | Cara menjalankan DAG; daftar bukti |
| `docs/KEPUTUSAN.md`, `docs/PERUBAHAN_DARI_UTS.md` | K15/K16/K20/K21 → Diputuskan; K22–K36; P04–P10 |

## 3. Cara kerja (untuk demo)

- **Jadwal** `0 2 10 * *` zona Asia/Makassar (tanggal 10, 02.00 WITA). `catchup=False`, `max_active_runs=1`.
- **Retry** 3× dengan jeda 2 menit yang berlipat (maks. 10 menit). Retry ini lapis kedua: koneksi DB dan
  portal web sudah punya retry sendiri di kode.
- **Callback gagal** menulis entri KRITIS `AIRFLOW` ke `dq.dq_log` (dag.task, percobaan ke-, jenis error).
- **batch_id = run_id** → baris staging, curated, dan fakta membawa id run Airflow (lineage).
- **Branch:** `quality_gate` mengembalikan `load_dw` atau `quarantine`.
- **ingest_web:** tabel diekstrak dengan parser staging, di-hash sebagai JSON kanonik. Sama dengan pengambilan
  terakhir → manifest `tidak_berubah` menunjuk objek lama, tidak ada objek baru di raw zone.

Menjawab "apa yang terjadi bila…":

| Kejadian | Respons sistem |
|---|---|
| Portal web gagal | retry 3× di kode → fallback berkas manual → bila tidak ada, task GAGAL → retry Airflow 3× → callback KRITIS |
| Database terputus | `buka_koneksi` retry (2, 4, 8, 16 dtk) → task GAGAL → retry Airflow |
| Data korup (checksum) | staging GAGAL, `dq.quarantine`, staging lama utuh |
| Struktur sumber berubah | `StrukturSumberError`, staging/curated lama utuh |
| Transform gagal | transaksi curated di-rollback; curated lama utuh; downstream tidak jalan |
| Quality gate kritis gagal | cabang `quarantine`; DWH tidak disentuh |
| Pemeriksaan kritis saat load gagal | ROLLBACK; DWH tetap versi sebelumnya |

## 4. Hasil uji

| Run | Pemicu | Hasil | Durasi |
|---|---|---|---|
| `scheduled__2026-11-09T18:00:00+00:00` | jadwal (10 Nov 02.00 WITA; dibuat otomatis saat DAG diaktifkan) | **success**, 12 task sukses (quarantine dilewati) | ±53 dtk |
| `manual__2026-11-15T15:32:55…` | trigger manual setelah uji E6–E8 | **success**, Q01–Q09 9/9 | ±1 mnt |
| `manual__2026-11-15T15:39:33…` | trigger manual dengan kode final | **success**, Q01–Q09 9/9, angka identik | ±1 mnt |

`ingest_web`: run pertama setelah fitur aktif menyimpan objek baru (belum ada checksum tabel sebelumnya);
semua run berikutnya `tidak_berubah` (`docs/bukti/21_ingest_web_tidak_berubah.txt`).

Bukti: `docs/bukti/16_run_dag_minggu2.txt`, `21_ingest_web_tidak_berubah.txt`.

**Belum diuji di Airflow:** cabang `quarantine` dan `on_failure_callback` belum dipicu dalam DAG run nyata;
logikanya diuji lewat fungsi yang sama (E7) — memicunya di DAG butuh data rusak di tengah run.
Rencana: uji di tahap end-to-end (7–13 Des), mis. dengan variabel uji yang memaksa Q02 gagal.

## 5. Keputusan & penyimpangan

K24, K30, K31, K35 (`docs/KEPUTUSAN.md`).

## 6. Pertanyaan demo yang mungkin muncul

1. **Kenapa tanggal 10?** — Puskesmas melapor tanggal 5 (Panduan); 5 hari untuk validasi/rekap (UTS 4.4).
2. **Kenapa catchup False?** — Setiap run memuat ulang seluruh berkas sumber; menjalankan bulan lampau tidak
   menghasilkan data berbeda.
3. **Bagaimana tahu sebuah run gagal di mana?** — Airflow UI (status task) + `dq.batch_log` (status per task,
   pesan error, percobaan ke-) + `dq.dq_log` entri `AIRFLOW` KRITIS.
4. **Kenapa ingest_web tidak membandingkan HTML?** — HTML berisi token formulir yang berubah tiap akses; yang
   penting bagi analisis adalah isi tabelnya.
5. **Apakah retry bisa menggandakan data?** — Tidak: manifest unik per (batch, sumber), staging/curated dimuat
   ulang atomik, dimensi di-upsert, fakta dihapus & disisip ulang dalam satu transaksi (E8).

## 7. Risiko / kendala / butuh tindakan pemilik

- **Screenshot UI Airflow belum ada** (K35): Playwright tidak terpasang di host; dijadwalkan lewat container.
- DAG kini **aktif**: scheduler akan menjalankan pipeline otomatis tanggal 10 Desember 2026 02.00 WITA bila
  container menyala. Bila tidak diinginkan, jalankan `airflow dags pause posyandu_pipeline`.
- Tidak ada yang membutuhkan tindakan pemilik selain git (bagian 8).

## 8. Untuk pemilik (git) — ringkasan tahap 5–7

Claude **tidak** menjalankan git. Usulan tiga commit (urutan: tahap 5, 6, lalu ini):

```
# commit tahap 5 dan 6: lihat bagian 8 di tahap_05_transform.md dan tahap_06_quality_gate_load_dwh.md

# commit tahap 7
git add dags/posyandu_pipeline.py dags/README.md ingestion/ingest_web.py common/lake.py README.md \
        docs/KEPUTUSAN.md docs/PERUBAHAN_DARI_UTS.md docs/bukti/README.md \
        docs/bukti/16_run_dag_minggu2.txt docs/bukti/21_ingest_web_tidak_berubah.txt \
        docs/laporan_tahap/tahap_07_orkestrasi_airflow.md
git status            # yang tersisa seharusnya hanya berkas yang diabaikan
git diff --cached
git commit -m "DAG Airflow posyandu_pipeline, ingest_web berbasis checksum tabel, dan catatan keputusan K15-K36"
git push              # tanpa --force
```

Pemeriksaan berkas sensitif (tanpa git, 15 Nov 2026):
- Berkas baru/berubah sejak tahap 4 sudah ditelusuri (34 berkas). Tidak ada nilai rahasia `.env`
  (kata sandi, kunci Fernet/JWT, `PSEUDONYM_KEY`) di berkas mana pun; yang cocok hanya nama default
  non-rahasia yang juga ada di `.env.example` (mis. `posyandu_dw`, `etl_user`).
- Tidak ada `KDR-…` (kode kader) atau nama/alamat kader di repo.
- `etl/pemetaan_posyandu_manual.csv` sengaja **tidak** di-commit (pola `*.csv`, K36).
- Yang tidak boleh ikut: `.env`, `data/`, `CLAUDE.md`, berkas `.csv`.
