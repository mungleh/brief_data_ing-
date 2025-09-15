FROM python:3.11-slim

RUN pip install duckdb pandas pyarrow

WORKDIR /app

COPY ./app .

CMD ["sh", "-c", "python duckydb.py && sleep infinity"]
