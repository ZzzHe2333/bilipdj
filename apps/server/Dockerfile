FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY VERSION README.md /app/
COPY core /app/core
COPY apps /app/apps

EXPOSE 9816

CMD ["python", "-m", "apps.server.main", "--host", "0.0.0.0", "--port", "9816"]
