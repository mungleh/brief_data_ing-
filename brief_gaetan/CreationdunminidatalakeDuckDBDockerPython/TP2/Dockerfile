FROM python:3.11-slim

RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*
RUN pip install duckdb pandas pyarrow requests

WORKDIR /app

COPY ./app /app
COPY app/cron.txt /etc/cron.d/ducky-cron

# donnai lai droat
RUN chmod 0644 /etc/cron.d/ducky-cron

CMD ["sh", "-c", "crontab /etc/cron.d/ducky-cron && cron -f"]
