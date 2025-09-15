FROM python:3.11-slim

RUN pip install duckdb pandas streamlit plotly

WORKDIR /app

COPY ./app /app

EXPOSE 8501

CMD ["streamlit", "run", "duckydb.py","--server.port", "8501"]
