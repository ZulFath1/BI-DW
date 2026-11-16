#!/usr/bin/env bash
# =====================================================================
# Uji END-TO-END dari kondisi bersih (dijalankan dari folder project, Git Bash / Linux):
#     bash tests/uji_e2e_bersih.sh [berkas_bukti]
#
# 1. Membuat stack TERPISAH `posyandu_e2e` (volume PostgreSQL, data lake, log Airflow baru; tanpa port
#    host) memakai docker/uji/compose.uji.yml. Stack utama dan raw zone-nya tidak disentuh.
# 2. Membuktikan database kosong, lalu mengaktifkan DAG -> scheduler sendiri membuat run terjadwal.
# 3. Menunggu run selesai; mencetak status tiap task, durasi, quality gate.
# 4. Membandingkan keluaran sql/validasi/03_dwh.sql & 04_mart.sql dengan stack utama (harus identik),
#    lalu memeriksa dashboard stack uji (Streamlit AppTest).
# 5. Menghapus stack uji beserta volumenya (hanya volume project posyandu_e2e).
# Prasyarat: stack utama berjalan (pembanding), image sudah dibangun, data/source berisi berkas sumber.
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
OUT=${1:-docs/bukti/29_uji_e2e_bersih.txt}
C="docker compose -p posyandu_e2e -f docker-compose.yml -f docker/uji/compose.uji.yml"
U="docker compose"
TMP=$(mktemp -d)
: > "$OUT"
log() { printf '%s\n' "$*" | tee -a "$OUT"; }
cmd() { "$@" 2>/dev/null | tee -a "$OUT"; }

log "# Uji end-to-end dari kondisi bersih — $(date '+%Y-%m-%d %H:%M:%S %z')"
$C down -v >/dev/null 2>&1
T0=$(date +%s)
log "## 1. Stack baru (volume baru, tanpa port host)"
$C up -d >/dev/null 2>&1
cmd docker volume ls --filter name=posyandu_e2e --format '{{.Name}}'
log "tabel di schema dw/mart sebelum pipeline: $($C exec -T postgres psql -U postgres -d posyandu_dw -tAc \
  "select count(*) from information_schema.tables where table_schema in ('dw','mart')")"
log "stack siap dalam $(( $(date +%s) - T0 )) detik"

log "## 2. Aktifkan DAG (scheduler membuat run terjadwal untuk interval terakhir)"
for _ in $(seq 1 30); do $C exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -q posyandu_pipeline && break; sleep 5; done
$C exec -T airflow-scheduler airflow dags unpause posyandu_pipeline >/dev/null 2>&1
for _ in $(seq 1 60); do
  s=$($C exec -T postgres psql -U postgres -d airflow -tAc \
      "select run_id||' '||state from dag_run where dag_id='posyandu_pipeline' order by id desc limit 1")
  case "$s" in *success*|*failed*) break;; esac; sleep 10
done
RUN=${s%% *}
log "## 3. Hasil run $RUN"
cmd $C exec -T airflow-scheduler python -m tests.status_run "$RUN"
cmd $C exec -T postgres psql -U postgres -d posyandu_dw -c \
  "select kode_cek, lulus, is_kritis, left(nilai, 110) nilai from dq.quality_result where batch_id='$RUN' order by 1"
log "total waktu sejak stack kosong: $(( $(date +%s) - T0 )) detik"

log "## 4. Validasi vs stack utama (keluaran query validasi harus identik)"
for f in 03_dwh 04_mart; do
  $C exec -T postgres psql -U postgres -d posyandu_dw -f - < sql/validasi/$f.sql > "$TMP/uji_$f.txt" 2>&1
  $U exec -T postgres psql -U postgres -d posyandu_dw -f - < sql/validasi/$f.sql > "$TMP/utama_$f.txt" 2>&1
  log "sql/validasi/$f.sql: $(wc -l < "$TMP/uji_$f.txt") baris keluaran, berbeda dari stack utama: $(diff "$TMP/utama_$f.txt" "$TMP/uji_$f.txt" | grep -c '^<') baris"
done
log "cuplikan validasi mart (stack uji):"
grep -A4 "M01\|M02\|M10" "$TMP/uji_04_mart.txt" | tee -a "$OUT"
$C exec -T dashboard python - 2>/dev/null <<'EOF' | tee -a "$OUT"
from streamlit.testing.v1 import AppTest
at = AppTest.from_file("app.py", default_timeout=120); at.run()
print("dashboard stack uji: exception", len(at.exception), "| KPI", [(m.label, m.value) for m in at.metric][:5])
EOF

log "## 5. Hapus stack uji"
$C down -v >/dev/null 2>&1 && log "stack posyandu_e2e dan volumenya dihapus"
rm -rf "$TMP"
