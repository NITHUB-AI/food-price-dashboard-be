import os
import psycopg2

from flask import Flask, jsonify, request, Request
from flask_restx import Api, Resource

app = Flask(__name__)
api = Api(
    app,
    version="1.0",
    title="Food Price Prediction API",
    description="An API for getting food prices, daily, monthly, and yearly.",
    license="MIT",
    contact="NITDA AI Team.",
)


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn


# http://127.0.0.1:5000/nbs/year/?food_item=oil&item_type=vegetable&category=1%20ltr&year=2016
@api.route("/nbs/year/")
class FilterByYear(Resource):
    """Returns the price of a food item by year and previous year all together."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")
        year = request.args.get("year")

        # Calculate the previous year
        previous_year = str(int(year) - 1)

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                SELECT date, price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s AND item_type = %s AND category = %s 
                AND source = 'NBS' AND (EXTRACT(YEAR FROM CAST(date AS DATE)) = %s OR
                                        EXTRACT(YEAR FROM CAST(date AS DATE)) = %s);
            """,
                (food_item, item_type, category, year, previous_year),
            )

            # Fetch all results
            records = cur.fetchall()

            for record in records:
                data = [{"date": record[0], "value": record[1]} for record in records]
        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/average-price/?food_item=oil&year=2018
@api.route("/nbs/average-price/")
class AveragePrice(Resource):
    """Returns the average price of all the item_types of the food_item chosen."""

    def get(self):
        food_item = request.args.get("food_item")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                SELECT item_type, AVG(price) AS average_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s AND source = 'NBS' AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
                GROUP BY item_type;
            """,
                (food_item, year),
            )

            # Fetch all results
            records = cur.fetchall()
            data = [
                {
                    "item_type": record[0],
                    "average_price": "{:.2f}".format(float(record[1])),
                }
                for record in records
            ]

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/yearly-average-price/?food_item=oil&item_type=vegetable&category=1%20ltr
@api.route("/nbs/yearly-average-price/")
class YearlyAvergarePrice(Resource):
    """Returns tha average price over the years of the food item, item type and category chosen."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                    SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS year, AVG(price) AS average_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s AND item_type = %s AND category = %s
                    GROUP BY year
                    ORDER BY year;
                """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()

            data = [
                {
                    "year": int(record[0]),
                    "average_price": "{:.2f}".format(float(record[1])),
                }
                for record in records
            ]

        return jsonify({"data": data})


if __name__ == "__main__":
    app.run(debug=True)
