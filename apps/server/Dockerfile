FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BILIPDJ_DATA_DIR=/data \
    BILIPDJ_DOCKER=1 \
    BILIPDJ_DOCKER_TRUSTED_CIDRS=auto

WORKDIR /app

# Install only Server/Web runtime dependencies; desktop tray libraries do not
# belong in the Docker image. Core API dependencies and QuickJS stay available.
COPY apps/server/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY VERSION README.md /app/
COPY core /app/core
COPY apps/__init__.py apps/update_download_source.py apps/update_workspace.py apps/versioning.py /app/apps/
COPY apps/server /app/apps/server
COPY apps/web/static /app/apps/web/static

RUN mkdir -p /data

STOPSIGNAL SIGTERM

EXPOSE 9816

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9816/health', timeout=3).read()" || exit 1

CMD ["python", "-m", "apps.server.main", "--host", "0.0.0.0", "--port", "9816"]
