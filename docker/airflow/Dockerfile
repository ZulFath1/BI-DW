# Image Airflow untuk pipeline posyandu.
#
# Basis: Apache Airflow 3.3.2 dengan Python 3.12, di-pin ke digest.
# Alasan Python 3.12: didukung image resmi Airflow 3.3.2 dan semua pustaka pipeline
# (diuji dengan pip --dry-run terhadap constraints resmi, 15 Nov 2026, tanpa konflik).
# Python 3.14 di laptop tidak dipakai untuk pipeline.
#
# Catatan: cabang Airflow 3.3 berakhir dukungannya 7 Nov 2026 (endoflife.date).
# Tetap dipakai karena patch matang dan sistem hanya berjalan lokal; upgrade ke 3.4.x
# dicatat sebagai pengembangan lanjutan.
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

COPY requirements.txt /tmp/requirements.txt

# "apache-airflow==${AIRFLOW_VERSION}" ikut disebut agar pip tidak meng-upgrade/downgrade
# Airflow; constraints resmi mengunci versi semua dependensi Airflow.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
