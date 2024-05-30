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
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
    )
    print("Tables in public schema:", [row[0] for row in cur])
conn.close()


# """
#         SELECT *, date_trunc('month',max(date)over()::timestamp) as max from "Cleaned-Food-Prices"
#         SELECT date, item_type, category, price
#         FROM "Cleaned-Food-Prices"
#         WHERE food_item = 'oil'
#             AND source = 'NBS'
#     ORDER BY date DESC;
# """
# f""" WITH latest as (SELECT date_trunc('month',date) as month FROM "Cleaned-Food-Prices" ORDER BY date desc limit 1))
#         WHERE food_item = 'tomato' AND vendor_type = 'Supermarket'
#             AND EXTRACT(YEAR FROM CAST(date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)
#             AND EXTRACT(MONTH FROM CAST(date AS DATE)) = EXTRACT(MONTH FROM CURRENT_DATE)
#         GROUP BY EXTRACT(YEAR FROM CURRENT_DATE), EXTRACT(MONTH FROM CURRENT_DATE);
#         """
#                 with latest as (select date_trunc('month',date) as month from Cleaned-Food-Prices order by date desc limit 1)
# select item_type,category,avg(price) from Cleaned-Food-Prices group by item_type, category where food_item='rice' and date_trunc('month',date) = latest.month;
# """
# SELECT * FROM "Cleaned-Food-Prices"
# WHERE food_item = 'rice'
# AND source = 'NBS'
# ORDER BY date DESC;
# """
# """
# SELECT DISTINCT food_item, item_type, category
# FROM "Cleaned-Food-Prices"
# WHERE vendor_type = 'Supermarket';
# """
#     """
#     SELECT * FROM "Cleaned-Food-Prices"
#     WHERE food_item = 'tomato' AND item_type = 'tomato' AND category = '150 g' AND vendor_type = 'Supermarket'
#     LIMIT 40
# """
# )
#     rows = cur.fetchall()
#     column_names = [desc[0] for desc in cur.description]

#     print("=".join([str(col_name) for col_name in column_names]))
#     for row in rows:
#         print(f"| {' | '.join([str(val) for val in row])} |")


# conn.close()


# cur.execute(
#     """
#     WITH LatestRecords AS (
#         SELECT *, DATE_TRUNC('month', MAX(CAST(date AS TIMESTAMP)) OVER ()) AS latest_month
#         FROM "Cleaned-Food-Prices"
#         WHERE food_item = %s AND source = 'NBS'
#     )
#     SELECT date, item_type, category, price
#     FROM LatestRecords
#     WHERE DATE_TRUNC('month', CAST(date AS TIMESTAMP)) = latest_month
#     ORDER BY date DESC;
#     """,
#     (food_item,),
# )
