import os
import psycopg2
from sqlalchemy import create_engine

conn = psycopg2.connect(
    host=os.getenv("HOST"),
    database=os.getenv("DATABASE"),
    user=os.getenv("USER_NAME"),
    password=os.getenv("PASSWORD"),
)

with conn.cursor() as cur:
    cur.execute(
        """
        SELECT * FROM "Cleaned-Food-Prices"
        LIMIT 10
    """
    )
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]

    print("=".join([str(col_name) for col_name in column_names]))
    for row in rows:
        print(f"| {' | '.join([str(val) for val in row])} |")


conn.close()
