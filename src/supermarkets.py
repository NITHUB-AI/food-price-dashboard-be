import os
import psycopg2

from flask import jsonify, request
from flask_restx import Resource, Namespace


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn


api = Namespace("Supermarket", description="Supermarket food price data operations")


# http://127.0.0.1:5000/supermarkets/all-time/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
@api.route("/all-time/")
class AllTime(Resource):
    """Returns the price of a category's food item for all time."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                    SELECT date, AVG(price) AS avg_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s 
                      AND item_type = %s 
                      AND category = %s 
                      AND vendor_type = 'Supermarket'
                    GROUP BY date
                    ORDER BY date;
                    """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()
            data = [
                {"date": row[0], "value": "{:.2f}".format(float(row[1]))}
                for row in records
            ]
        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=150%20g&year=2024&current_month=true
# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=150%20g&year=2024&current_week=true
@api.route("/year/")
class FilterByCurrentYear(Resource):
    """Returns the price of a category's food item for the current year with filter for current month and/or current week."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")
        year = request.args.get("year")
        current_month = request.args.get("current_month", "false")
        current_week = request.args.get("current_week", "false")

        query = """
                SELECT date, AVG(price) AS avg_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s 
                    AND item_type = %s 
                    AND category = %s 
                    AND vendor_type = 'Supermarket'
                    AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
                """

        if current_month.lower() == "true":
            query += " AND EXTRACT(MONTH FROM CAST(date AS DATE)) = EXTRACT(MONTH FROM CURRENT_DATE)"

        if current_week.lower() == "true":
            query += " AND EXTRACT(WEEK FROM CAST(date AS DATE)) = EXTRACT(WEEK FROM CURRENT_DATE)"

        query += " GROUP BY date ORDER BY date;"

        with get_db_connection().cursor() as cur:
            cur.execute(query, (food_item, item_type, category, year))
            records = cur.fetchall()
            data = [
                {"date": row[0], "value": "{:.2f}".format(float(row[1]))}
                for row in records
            ]
        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/latest-price/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
@api.route("/latest-price/")
class LatestPrice(Resource):
    """Retuens the latest daily price of a category's food item."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                SELECT date,AVG(price) AS avg_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s
                    AND item_type = %s
                    AND category = %s
                    AND vendor_type = 'Supermarket'
                GROUP BY date
                ORDER BY date DESC
                LIMIT 1;
                """,
                (food_item, item_type, category),
            )
            records = cur.fetchall()
            data = [
                {"date": row[0], "value": "{:.2f}".format(float(row[1]))}
                for row in records
            ]
        return jsonify({"data": data})


@api.route("/average-price/")
class AveragePrice(Resource):
    """Returns the average price of all the item_types of the food_item chosen in a particular year."""

    def get(self):
        food_item = request.args.get("food_item")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                # TODO: Fix this query; something is wrong somewhere i think
                """
            SELECT item_type, EXTRACT(MONTH FROM CAST(date AS DATE)) AS month, AVG(daily_avg_price) AS yearly_avg_price
            FROM (
                SELECT item_type, date, AVG(price) AS daily_avg_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s 
                AND vendor_type = 'Supermarket'
                AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
                GROUP BY item_type, date
            ) AS daily_averages
            GROUP BY item_type, EXTRACT(MONTH FROM CAST(date AS DATE))
            ORDER BY item_type, month;
        """,
                (food_item, year),
            )
            records = cur.fetchall()
            print(records)
            data = [
                {
                    "item_type": row[0],
                    "yearly_avg_price": "{:.2f}".format(float(row[1])),
                }
                for row in records
            ]
        return jsonify({"data": data})
