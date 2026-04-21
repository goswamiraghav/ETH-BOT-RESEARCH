import duckdb

sql = open("sql/signal_analysis.sql", "r").read()
con = duckdb.connect()

statements = [s.strip() for s in sql.split(";") if s.strip()]

for i, stmt in enumerate(statements, start=1):
    print(f"\n--- Query {i} ---")
    try:
        df = con.execute(stmt).df()
        print(df.to_string(index=False))
    except Exception as e:
        print("Error:", e)