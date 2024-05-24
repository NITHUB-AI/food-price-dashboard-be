import os
import psycopg2

from datetime import datetime
from flask import jsonify, request
from flask_restx import Resource, Namespace

from src.utils import *


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn


api = Namespace("Supermarket", description="Supermarket food price data operations")

conversion_dictionary = {"g": [1000, "kg"], "ml": [1000, "L"], "pcs": [1, "pcs"]}


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


# http://127.0.0.1:5000/supermarkets/average-price/?food_item=tomato
@api.route("/average-price/")
class AveragePrice(Resource):
    """Returns the average price of all the item_types of the food_item chosen in a particular year."""

    def get(self):
        food_item = str(request.args.get("food_item"))

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                WITH LatestDate AS (
                    SELECT MAX(CAST(date AS TIMESTAMP)) AS max_date
                    FROM "Cleaned-Food-Prices"
                    WHERE category IS NOT NULL AND LENGTH(category) > 0
                    AND food_item = %s AND vendor_type = 'Supermarket'
                ),
                LatestRecords AS (
                    SELECT 
                        item_type, 
                        category, 
                        price, 
                        SPLIT_PART(category, ' ', 1) AS numeric_part,
                        SPLIT_PART(category, ' ', 2) AS unit,
                        price / NULLIF(CAST(COALESCE(NULLIF(SPLIT_PART(category, ' ', 1), ''), '0') AS numeric), 0) AS unit_price
                    FROM "Cleaned-Food-Prices"
                    WHERE CAST(date AS TIMESTAMP) = (SELECT max_date FROM LatestDate)
                )
                SELECT item_type, AVG(unit_price) AS average_price, unit
                FROM LatestRecords
                GROUP BY item_type, unit;
                """,
                (food_item,),
            )

            records = cur.fetchall()

            data = []
            for item_type, average_price, unit in records:
                if average_price is None:
                    average_price = 0

                if unit in conversion_dictionary:
                    conversion_factor, new_unit = conversion_dictionary[unit]
                    converted_price = (
                        average_price * conversion_factor if average_price != 0 else 0
                    )
                    data.append(
                        {
                            "item_type": item_type,
                            "average_price": round(converted_price, 2),
                            "unit": new_unit,
                        }
                    )
                else:
                    data.append(
                        {
                            "item_type": item_type,
                            "average_price": (
                                round(average_price, 2) if average_price != 0 else 0
                            ),
                            "unit": unit,
                        }
                    )

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
                        DATE(date) AS day,  -- Ensure date is truncated to the day part only
                        EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,
                        EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
                        AVG(price) AS avg_daily_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = %s
                        AND item_type = %s
                        AND category = %s
                        AND vendor_type = 'Supermarket'
                    GROUP BY DATE(date), EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
                ),
                MonthlyAverages AS (
                    SELECT
                        year,
                        month,
                        AVG(avg_daily_price) AS avg_monthly_price
                    FROM DailyAverages
                    GROUP BY year, month
                ),
                MonthToMonthComparison AS (
                    SELECT
                        year,
                        month,
                        avg_monthly_price,
                        LAG(avg_monthly_price) OVER (PARTITION BY year ORDER BY month) AS previous_month_avg_price
                    FROM MonthlyAverages
                )
                SELECT
                    month,
                    avg_monthly_price,
                    previous_month_avg_price,
                    CASE 
                        WHEN previous_month_avg_price IS NOT NULL THEN
                            ((avg_monthly_price - previous_month_avg_price) / previous_month_avg_price) * 100
                        ELSE
                            NULL  -- Handling cases where there is no previous month data
                    END AS percentage_change
                FROM MonthToMonthComparison
                WHERE 
                    year = EXTRACT(YEAR FROM CURRENT_DATE) AND 
                    month = EXTRACT(MONTH FROM CURRENT_DATE);
            """,
                (food_item, item_type, category, year),
            )

            records = cur.fetchone()
            # (
            #     current_month,
            #     avg_monthly_price,
            #     previous_month_avg_price,
            #     percentage_change,
            # ) = records
            print(records)
            # data = [
            #     {
            #         "current_month": current_month,
            #         "avg_monthly_price": avg_monthly_price,
            #         "previous_month_avg_price": previous_month_avg_price,
            #         "percentage_change": (
            #             percentage_change if percentage_change is not None else "N/A"
            #         ),
            #     }
            # ]
            # return jsonify({"data": data})
