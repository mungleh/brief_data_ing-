from time import perf_counter
import duckdb as dd


con = dd.connect('../outputs/covid.db')

con.sql(f'''
    CREATE TABLE covide AS
        SELECT * FROM 'data/covid.csv';
''')

con.sql('''
    COPY covide TO 'data/covid.parquet' (FORMAT 'parquet');
''')


def compare_time(query):

    start = perf_counter()
    result = con.sql(query)
    stop = perf_counter()
    elapsed = stop - start

    result_str = result.df().to_string(index=False)

    with open('../outputs/TP1_time_results.txt', 'a') as f:
        f.write(f"{query}\n")
        f.write(result_str + "\n")
        f.write(f"Elapsed time: {elapsed}\n")
        f.write(f"\n")


loop_dict = [
    "covide",
    "read_csv_auto('data/covid.csv')",
    "read_parquet('data/covid.parquet')"
]

for i in loop_dict:
    compare_time(f"""
        SELECT *
        FROM {i}
        ORDER BY "Confirmed" DESC
        LIMIT 10;
    """)

    compare_time(f"""
        SELECT MEAN("1 week % increase")
        FROM {i};
    """)

    compare_time(f"""
        SELECT
            "WHO Region",
            SUM(Confirmed)
        FROM {i}
        GROUP BY "WHO Region";
    """)
