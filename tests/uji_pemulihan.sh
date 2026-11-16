#!/usr/bin/env bash
# =====================================================================
# Uji PEMULIHAN BENCANA (dijalankan dari folder project, Git Bash / Linux):
#     bash tests/uji_pemulihan.sh [berkas_bukti]
#
# Skenario: laptop/server baru — volume PostgreSQL & data lake kosong DAN berkas sumber di data/source
# tidak ada. Yang tersisa hanya backup (backup/output/: dump database + mirror raw zone).
# 1. Stack TERPISAH `posyandu_pulih` (volume baru, tanpa port host, data/source = folder kosong,
#    backup di-mount read-only). Hanya postgres + datalake yang dinyalakan.
# 2. python -m backup.restore semua: restore DWH + metadata Airflow + raw zone, lalu verifikasi
#    jumlah baris, md5 isi tabel dw/mart, dan sha256 objek terhadap manifest backup. Waktu dicatat (RTO).
# 3. Menyalakan Airflow & dashboard: riwayat run dan dashboard harus kembali.
# 4. Menjalankan pipeline: ingestion harus memakai raw zone (berkas lokal tidak ada), hasil harus identik
#    dengan stack utama.
# 5. Menghapus stack uji beserta volumenya.
# Prasyarat: minimal satu backup (DAG posyandu_backup) dan stack utama berjalan (pembanding).
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1 UJI_SUMBER_DIR=./data/kosong
mkdir -p data/kosong
OUT=${1:-docs/bukti/30_uji_pemulihan.txt}
C="docker compose -p posyandu_pulih -f docker-compose.yml -f docker/uji/compose.uji.yml"
U="docker compose"
TMP=$(mktemp -d)
RUN=pulih_dari_raw_zone__$(date +%Y%m%d%H%M)
: > "$OUT"
log() { printf '%s\n' "$*" | tee -a "$OUT"; }
cmd() { "$@" 2>/dev/null | tee -a "$OUT"; }

log "# Uji pemulihan bencana — $(date '+%Y-%m-%d %H:%M:%S %z')"
log "backup tersedia (backup/output/db):"
cmd ls backup/output/db
$C down -v >/dev/null 2>&1
T0=$(date +%s)
log "## 1. Stack kosong: hanya postgres + datalake; data/source kosong"
$C up -d postgres datalake datalake-init >/dev/null 2>&1
log "tabel dw/mart: $($C exec -T postgres psql -U postgres -d posyandu_dw -tAc \
  "select count(*) from information_schema.tables where table_schema in ('dw','mart')")"
log "## 2. Restore + verifikasi"
$C run --rm --no-deps airflow-scheduler python -m backup.restore semua 2>/dev/null | tee -a "$OUT"
log "RTO (stack kosong -> data terverifikasi): $(( $(date +%s) - T0 )) detik"

log "## 3. Airflow & dashboard dari data hasil restore"
$C up -d >/dev/null 2>&1
for _ in $(seq 1 30); do $C exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -q posyandu_backup && break; sleep 5; done
# Backup dari stack uji tidak boleh berjalan (folder backup read-only di sini).
$C exec -T airflow-scheduler airflow dags pause posyandu_backup >/dev/null 2>&1
cmd $C exec -T postgres psql -U postgres -d airflow -c \
  "select dag_id, count(*) as run, sum((state='success')::int) as sukses from dag_run group by 1 order by 1"
$C exec -T dashboard python - 2>/dev/null <<'EOF' | tee -a "$OUT"
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("app.py", default_timeout=120); at.run()
print("dashboard (data restore): exception", len(at.exception), "| KPI", [(m.label, m.value) for m in at.metric][:5])
EOF

log "## 4. Pipeline tanpa berkas sumber (proses ulang dari raw zone)"
log "isi data/source di container: $($C exec -T airflow-scheduler bash -c 'ls /opt/airflow/data/source | wc -l') berkas"
$C exec -T airflow-scheduler airflow dags trigger posyandu_pipeline -r "$RUN" >/dev/null 2>&1
for _ in $(seq 1 60); do
  s=$($C exec -T postgres psql -U postgres -d airflow -tAc "select state from dag_run where run_id='$RUN'")
  [ "$s" = success ] || [ "$s" = failed ] && break; sleep 10
done
cmd $C exec -T airflow-scheduler python -m tests.status_run "$RUN"
cmd $C exec -T postgres psql -U postgres -d posyandu_dw \
  -c "select sumber, cara_ambil, status_unggah, object_key from dq.ingest_manifest where batch_id='$RUN' order by 1" \
  -c "select kode_aturan, tingkat, keterangan from dq.dq_log where batch_id='$RUN' and kode_aturan='ING02'" \
  -c "select count(*) filter (where lulus) as lulus, count(*) as cek from dq.quality_result where batch_id='$RUN'"
for f in 03_dwh 04_mart; do
  $C exec -T postgres psql -U postgres -d posyandu_dw -f - < sql/validasi/$f.sql > "$TMP/uji_$f.txt" 2>&1
  $U exec -T postgres psql -U postgres -d posyandu_dw -f - < sql/validasi/$f.sql > "$TMP/utama_$f.txt" 2>&1
  log "sql/validasi/$f.sql: berbeda dari stack utama: $(diff "$TMP/utama_$f.txt" "$TMP/uji_$f.txt" | grep -c '^<') baris"
done
log "total waktu sejak stack kosong (restore + pipeline): $(( $(date +%s) - T0 )) detik"

log "## 5. Hapus stack uji"
$C down -v >/dev/null 2>&1 && log "stack posyandu_pulih dan volumenya dihapus"
rm -rf "$TMP"
