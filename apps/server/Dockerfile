FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BILIPDJ_DATA_DIR=/data \
    BILIPDJ_DOCKER=1 \
    BILIPDJ_DOCKER_TRUSTED_CIDRS=auto

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY VERSION README.md /app/
COPY core /app/core
COPY apps /app/apps

RUN mkdir -p /data

EXPOSE 9816

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9816/health', timeout=3).read()" || exit 1

CMD ["python", "-m", "apps.server.main", "--host", "0.0.0.0", "--port", "9816"]
