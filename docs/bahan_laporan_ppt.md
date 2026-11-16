# Bahan laporan PDF dan PPT (maksimal 10 slide)

Kerangka untuk pemilik. Setiap angka merujuk ke bukti di `docs/bukti/` (semua dari run nyata, 16 Nov 2026).
Susunan slide mengikuti instruksi UAS. Bagian laporan PDF memakai urutan alur wajib.

## Alur laporan PDF

| Bab | Isi utama | Sumber |
|---|---|---|
| 1. Business problem | Laporan posyandu bulanan 305 kolom; dinas tidak bisa cepat menentukan posyandu prioritas, alat yang kurang, angka janggal | CLAUDE.md 4.1, laporan tahap 2 |
| 2. Big data sources & 3V/5V | Tabel sumber (internal Excel 6 sheet; E1 PDF Disdukcapil; E2 web Satu Data): record, ukuran, periode, format, frekuensi. 3V/5V jujur: Volume kecil (±106 ribu baris fakta, DWH 176 MB); Variety, Velocity batch, Veracity (2.073 baris anomali), Value | bukti 11, 15, 27 |
| 3. Ingestion & data lake | Raw zone `sumber/tanggal_ingest/`, SHA-256, tidak ditimpa, manifest lineage; reprocess dari raw zone | bukti 15, 21, 37–39, 30 |
| 4. ETL/ELT | raw → stg → curated (T01–T19) → dw → mart | bukti 10, 17 |
| 5. Data quality & quality gate | Q01–Q09, karantina, rollback | bukti 18, 19, 22 |
| 6. Error handling | Retry, callback, outbox, quarantine; jawaban 4 skenario (API gagal, DB putus, data corrupt, transform gagal) | bukti 13, 23, 24, persiapan_demo.md |
| 7. Orkestrasi | 2 DAG, jadwal, retry | bukti 16, 30–36 png |
| 8. DWH & data mart | Galaxy schema 5 fakta, 7 dimensi, SCD 2 (503 versi), grain; mart 11 tabel | bukti 14, 20, 25 |
| 9. ML | Baseline → improvement → comparison → terpilih; tabel bagian 2.1 dan 2.2 laporan tahap 10; arti metrik | bukti 26, ml/README.md |
| 10. Evaluasi & skalabilitas | Durasi task, ukuran layer, SPOF, rencana scale-up; uji E2E | bukti 27, 29 |
| 11. Disaster recovery | Kebijakan backup, uji restore RTO 18 detik | bukti 28, 30, backup/README.md |
| 12. Dashboard | 5 tab, filter, daftar prioritas | bukti 40–44 |
| 13. Insight & rekomendasi | 5 insight (di bawah) | dashboard tab 1 |
| 14. Kesimpulan & batasan | Volume kecil, struktur cluster lemah (Silhouette 0,21), backup satu laptop, Airflow 3.3 EOL, tanpa kode posyandu resmi | KEPUTUSAN, PERUBAHAN_DARI_UTS |

## Susunan 10 slide

| No | Judul | Isi (visual, bukan paragraf) | Gambar/tabel |
|---|---|---|---|
| 1 | Judul | judul project, nama, NIM, mata kuliah | — |
| 2 | Problem & 3V/5V | 3 pertanyaan bisnis; tabel 5V satu baris per V dengan angka | — |
| 3 | Dataset | tabel sumber: internal / E1 / E2, record, format, periode, frekuensi | `excel_daftar_sheet.png`, `09_pdf_tabel8_12.png` |
| 4 | Arsitektur | diagram: sumber → ingestion → raw zone (Silo) → PostgreSQL (stg/curated/dq/dw/mart) → ML → Streamlit; Airflow di atas; backup ke samping | diagram README |
| 5 | Pipeline & orkestrasi | Graph 16 task; jadwal; quality gate → karantina; durasi 63 detik | `31_airflow_graph.png`, `36_airflow_detail_dag_jadwal.png` |
| 6 | Data lake & DWH | struktur raw zone; diagram galaxy schema; grain; SCD 2 | `38_lake_raw_zone.png`, `14_schema_dw.txt` |
| 7 | ML | tabel perbandingan 6 kolom (laporan tahap 10 §2.1); tabel profil 6 cluster + rekomendasi | bukti 26 |
| 8 | Evaluasi & scalability | arti metrik; tabel ukuran layer; SPOF; uji E2E & DR (RTO 18 detik) | bukti 27, 29, 30 |
| 9 | Dashboard | 2 screenshot (ringkasan + prioritas ML) | `40_dashboard_1.png`, `42_dashboard_3.png` |
| 10 | Insight & kesimpulan | 3–5 insight singkat (Temuan → Rekomendasi) + batasan | — |

## 5 insight (pola Data → Temuan → Interpretasi → Impact → Recommendation)

| # | Data | Temuan | Interpretasi | Impact | Recommendation |
|---|---|---|---|---|---|
| 1 | Laporan bulanan 2025, KPI K38 | Cakupan bayi balita 58,7% (ibu hamil 56,6%); terendah Banjarmasin Timur 52,4%, tertinggi Selatan 67,1% | ±4 dari 10 balita sasaran tidak ditimbang tiap bulan | gangguan tumbuh & stunting terlambat terdeteksi | sweeping & kunjungan rumah, mulai dari 121 posyandu cluster K1 "Cakupan rendah" |
| 2 | Sasaran Des 2025 vs penduduk 0–4 Disdukcapil | Sasaran 23.299 = 59% dari 39.805 balita terdaftar | banyak balita tidak tercatat sebagai sasaran posyandu | cakupan terlihat lebih baik dari kenyataan | selaraskan daftar sasaran dengan data Disdukcapil per kelurahan tiap semester |
| 3 | Alat antropometri Juni 2026 | 335 dari 490 posyandu punya alat rusak/tidak standar; hanya 164 lengkap 5 jenis; cluster K3 rata-rata 0,8/5 | pengukuran BB/TB banyak memakai alat tidak standar | status gizi bisa salah klasifikasi | pengadaan alat diprioritaskan ke 59 posyandu K3 |
| 4 | Penanda anomali T09/T10 | 826 baris anomali 2025 (2.073 total 2023–2026); Puskesmas Kayu Tangi 24,4%, Mantuil 17,7% baris Terlapor | Sasaran di sebagian laporan tidak wajar | KPI bias, logistik salah hitung | verifikasi Sasaran oleh puskesmas tsb. (71 posyandu K4) |
| 5 | Data kader 2026 | 45,9% kader terlatih; 31 posyandu < 5 kader; 117 posyandu (K5) cakupan 75% tetapi kader terlatih 9% | kinerja bergantung pada pengalaman, bukan keterampilan terstandar | mutu layanan rentan turun saat kader berganti | kuota pelatihan 25 keterampilan ke K5; rekrut kader di posyandu < 5 |

## Batasan yang wajib disebut

- Volume data kecil; arsitektur dipilih untuk Variety/Veracity dan kesiapan skala, bukan karena volume.
- Silhouette 0,21: cluster tumpang tindih; daftar prioritas = alat bantu, perlu verifikasi lapangan.
- Fitur ML periode 2025, alat & kader snapshot 2026 (K42).
- Rasio stunting ≠ prevalensi; penyebut 2024 = pendekatan.
- Tidak ada kode posyandu resmi; perpindahan kecamatan tidak terdeteksi (P13).
- Backup masih satu laptop; PostgreSQL tunggal (SPOF); Airflow 3.3 sudah EOL.
- Disclaimer: studi kasus tugas, bukan dokumen resmi Dinkes dan bukan saran medis/kebijakan profesional.
