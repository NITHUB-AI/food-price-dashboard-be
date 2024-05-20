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
            data = [
                {"date": record[0], "value": float("{:.2f}".format(record[1]))}
                for record in records
            ]
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
                """
                SELECT item_type, AVG(price) AS average_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s AND source = 'NBS' AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
                GROUP BY item_type;
            """,
                (food_item, year),
            )

            records = cur.fetchall()
            data = [
                {
                    "item_type": record[0],
                    "average_price": float("{:.2f}".format(record[1])),
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
                    "average_price": float(f"{record[1]:.2f}"),
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
                SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
                    AVG(price) AS average_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS'
                GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE))
                ORDER BY year;

                """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()

            data = [
                {"year": int(year), "average_price": float(f"{average_price:.2f}")}
                for year, average_price in records
            ]

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/percentage/?food_item=oil&item_type=vegetable&category=1%20ltr
@api.route("/percentage/")
class Percentage(Resource):
    """Returns the percentage of the category chosen between the most recent month and previous month."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                    SELECT
                        EXTRACT(MONTH FROM CURRENT_DATE) AS month,
                        price AS current_month_price,
                        LAG(price) OVER (ORDER BY EXTRACT(MONTH FROM CURRENT_DATE)) AS previous_month_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s
                        AND item_type = %s
                        AND category = %s
                        AND source = 'NBS'
                        AND EXTRACT(YEAR FROM CAST(date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)
                    ORDER BY date DESC
                    LIMIT 2;
                    """,
                (food_item, item_type, category),
            )

            records = cur.fetchone()
            if records:
                month, current_month_price, previous_month_price = records
                percentage_change = (
                    (
                        (current_month_price - previous_month_price)
                        / previous_month_price
                        * 100
                    )
                    if previous_month_price
                    else 0
                )
                data = {
                    "month": month,
                    "current_month_price": float(f"{current_month_price:.2f}"),
                    "previous_month_price": float(f"{previous_month_price:.2f}"),
                    "percentage_change": float(f"{percentage_change:.2f}"),
                }

        return jsonify(data)


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

            records = cur.fetchone()
            price, date = records

            data = {"date": date, "price": f"{price:.2f}"}
        return jsonify({"data": data})
