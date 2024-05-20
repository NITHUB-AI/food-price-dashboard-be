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


# http://127.0.0.1:5000/supermarkets/average-price/?food_item=tomato&year=2024
@api.route("/average-price/")
class AveragePrice(Resource):
    """Returns the average price of all the item_types of the food_item chosen in a particular year."""

    def get(self):
        food_item = request.args.get("food_item")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                # TODO: Update this query
                """
            SELECT item_type, AVG(price) AS average_price
            FROM "Cleaned-Food-Prices"
            WHERE food_item = %s AND vendor_type = 'Supermarket'
              AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
              AND EXTRACT(MONTH FROM CAST(date AS DATE)) = EXTRACT(MONTH FROM CURRENT_DATE)
            GROUP BY item_type
            ORDER BY item_type;
            """,
                (food_item, year),
            )
            records = cur.fetchall()
            data = [
                {
                    "item_type": row[0],
                    "average_price": "{:.2f}".format(float(row[1])),
                }
                for row in records
            ]
        return jsonify({"data": data})


@api.route("/yearly-average-price/")
class YearlyAveragePrice(Resource):
    """Returns the average price over the year for the food_item, item_type and category chosen."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
            SELECT EXTRACT(MONTH FROM CAST(date AS DATE)) AS month, AVG(price) AS monthly_avg_price
            FROM "Cleaned-Food-Prices"
            WHERE food_item = %s
              AND item_type = %s
              AND category = %s
            AND vendor_type = 'Supermarket'
              AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s
            GROUP BY EXTRACT(MONTH FROM CAST(date AS DATE))
            ORDER BY EXTRACT(MONTH FROM CAST(date AS DATE));
            """,
                (food_item, item_type, category, year),
            )
            records = cur.fetchall()
            data = [
                {
                    "month": int(row[0]),
                    "monthly_avg_price": "{:.2f}".format(float(row[1])),
                }
                for row in records
            ]
        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/monthly-average-price/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
@api.route("/monthly-average-price/")
class MonthlyAverage(Resource):
    """Returns the current monthly average price for the food_item, item_type and category chosen."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
            SELECT EXTRACT(MONTH FROM CAST(date AS DATE)) AS month, AVG(price) AS monthly_avg_price
            FROM "Cleaned-Food-Prices"
            WHERE food_item = %s
              AND item_type = %s
              AND category = %s
            AND vendor_type = 'Supermarket'
              AND EXTRACT(MONTH FROM CAST(date AS DATE)) = EXTRACT(MONTH FROM CURRENT_DATE)
            GROUP BY EXTRACT(MONTH FROM CAST(date AS DATE))
            ORDER BY EXTRACT(MONTH FROM CAST(date AS DATE));
            """,
                (food_item, item_type, category),
            )
            records = cur.fetchall()
            data = [
                {
                    "month": int(row[0]),
                    "monthly_avg_price": "{:.2f}".format(float(row[1])),
                }
                for row in records
            ]
        return jsonify({"data": data})


@api.route("/percentage/")
class Percentage(Resource):
    """Returns the percentage of the category chosen between the current month and previous month within a particular year."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                # TODO: fix query
                """
WITH DailyAverages AS (
    SELECT
        date,
        EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,
        AVG(price) AS avg_price
    FROM "Cleaned-Food-Prices"
    WHERE food_item = %s
        AND item_type = %s
        AND category = %s
        AND vendor_type = 'Supermarket'
        AND EXTRACT(YEAR FROM CAST(date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY date
),
MonthlyAverages AS (
    SELECT
        month,
        AVG(avg_price) AS monthly_avg_price
    FROM DailyAverages
    GROUP BY month
),
MonthToMonthChange AS (
    SELECT
        month,
        monthly_avg_price,
        LAG(monthly_avg_price, 1) OVER (ORDER BY month) AS previous_month_avg_price
    FROM MonthlyAverages
)
SELECT
    month,
    monthly_avg_price,
    previous_month_avg_price,
    COALESCE(((monthly_avg_price - previous_month_avg_price) / previous_month_avg_price) * 100, 0) AS percentage_change
FROM MonthToMonthChange
WHERE month = EXTRACT(MONTH FROM CURRENT_DATE)
    AND previous_month_avg_price IS NOT NULL;


            """,
                (food_item, item_type, category, year),
            )

            records = cur.fetchone()
            print(records)
            # data = [
            #     {
            #         "month": records[0],
            #         "monthly_avg_price": records[1],
            #         "previous_month_avg_price": records[2],
            #         "percentage_change": records[3],
            #     }
            # ]
            # return jsonify({"data": data})
