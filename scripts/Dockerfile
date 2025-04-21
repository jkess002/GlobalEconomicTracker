FROM python:3.10-slim

RUN apt-get update && apt-get install -y cron && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_indices.py .
COPY fetch_indices.cron /etc/cron.d/fetch_indices

RUN chmod 0644 /etc/cron.d/fetch_indices && \
    crontab /etc/cron.d/fetch_indices

RUN touch /var/log/cron.log

CMD ["cron", "-f"]
