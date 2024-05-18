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


api = Namespace("NBS", description="NBS food price data operations")


# http://127.0.0.1:5000/nbs/year/?food_item=oil&item_type=vegetable&category=1%20ltr&year=2017
@api.route("/year/")
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
                data = [{"date": record[0], "value": record[1]}]
        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/average-price/?food_item=oil&year=2018
@api.route("/average-price/")
class AveragePrice(Resource):
    """Returns the average price of all the item_types of the food_item chosen in a particular year."""

    def get(self):
        food_item = request.args.get("food_item")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                # TODO: update this query
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
@api.route("/yearly-average-price/")
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
                    WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS'
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


# http://127.0.0.1:5000/nbs/average-price-over-years/?food_item=oil&item_type=vegetable&category=1%20ltr
@api.route("/average-price-over-years/")
class AveragePriceOverYears(Resource):
    """Returns the average price of the years of the category chosen"""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                    SELECT MIN(EXTRACT(YEAR FROM CAST(date AS DATE))) AS min_year,
                           MAX(EXTRACT(YEAR FROM CAST(date AS DATE))) AS max_year,
                           AVG(price) AS average_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS'
                """,
                (food_item, item_type, category),
            )

            result = cur.fetchone()
            if not result or result[0] is None or result[1] is None:
                return (
                    jsonify(
                        {"error": "No data available for the specified parameters"}
                    ),
                    404,
                )

            min_year, max_year, average_price = result
            if average_price is None:
                return (
                    jsonify({"error": "No price data found for the available years"}),
                    404,
                )

            data = {
                "years": f"{int(min_year)} to {int(max_year)}",
                "average_price": f"{average_price:.2f}",
            }

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/percentage/?food_item=oil&item_type=vegetable&category=1%20ltr
@api.route("/percentage/")
class AveragePercentage(Resource):
    """Returns the average percentage of the category chosen over the years."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            # Calculate the average price for each year and fetch min/max years and prices
            cur.execute(
                """
                WITH YearlyAverage AS (
                    SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS year, AVG(price) AS average_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS'
                    GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE))
                )
                SELECT 
                    MIN(year) AS min_year, 
                    MAX(year) AS max_year,
                    (SELECT average_price FROM YearlyAverage WHERE year = (SELECT MIN(year) FROM YearlyAverage)) AS min_price,
                    (SELECT average_price FROM YearlyAverage WHERE year = (SELECT MAX(year) FROM YearlyAverage)) AS max_price
                FROM YearlyAverage
                """,
                (food_item, item_type, category),
            )

            result = cur.fetchone()

            min_year, max_year, min_price, max_price = result

            # Calculating the percentage change
            percentage_change = ((max_price - min_price) / min_price) * 100

            data = {
                "years": f"{int(min_year)} to {int(max_year)}",
                "percentage_change": f"{percentage_change:.2f}%",
            }

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/latest-price/?food_item=oil&item_type=vegetable&category=1%20ltr
@api.route("/latest-price/")
class LatestPrice(Resource):
    """ "Returns the latest price of the category picked for the month."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                SELECT price, date
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s AND item_type = %s AND category = %s
                ORDER BY date DESC
                LIMIT 1
            """,
                (food_item, item_type, category),
            )

            result = cur.fetchone()
            price, date = result

            data = {"date": date, "price": f"{price:.2f}"}
        return jsonify({"data": data})
