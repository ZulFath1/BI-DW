# Dashboard Streamlit. Basis Python 3.12 slim, di-pin ke digest.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY .streamlit/ .streamlit/
COPY app.py .

# Jalankan sebagai user biasa (bukan root)
RUN useradd --create-home --uid 10001 streamlit
USER streamlit

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
