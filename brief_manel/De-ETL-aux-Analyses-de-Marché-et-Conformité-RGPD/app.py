from fastapi import FastAPI
import psycopg2
import uvicorn
import os

app = FastAPI(title="manelmart API")


def request(schema, table):
    connection = psycopg2.connect(os.getenv("database_url"))
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {schema}.{table}")
    mobile_records = cursor.fetchall()
    for row in mobile_records:
        print(row)


# -------------------------------------------------API BDD GET salesdatamart--------------------------------------------


@app.get("/date")
def customers():
    return request("salesdatamart", "date")


@app.get("/customers")
def products():
    return request("salesdatamart", "customers")


@app.get("/products")
def orders():
    return request("salesdatamart", "products")


@app.get("/shippers")
def orders():
    return request("salesdatamart", "shippers")


@app.get("/transactions")
def orders():
    return request("salesdatamart", "transactions")


@app.get("/salesfact")
def orders():
    return request("salesdatamart", "salesfact")


# -------------------------------------------------API BDD GET INVENTORY--------------------------------------------


@app.get("/suppliers")
def customers():
    return request("inventorydatamart", "suppliers")


@app.get("/stock")
def products():
    return request("inventorydatamart", "stock")


@app.get("/inventoryfact")
def orders():
    return request("inventorydatamart", "inventoryfact")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
