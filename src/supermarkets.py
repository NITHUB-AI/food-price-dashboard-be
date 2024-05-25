import os
import psycopg2

from datetime import datetime
from flask import jsonify, request
from flask_restx import Resource, Namespace

from src.utils import *
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


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
            # TODO: Forward Fill
            # Some categories don't have data on some days in the date range. They may have records for 2024-5-11 and then 2024-5-14. 
            # In that case, The 12th and 13th have to be filled with the price on the 11th. 
            records = cur.fetchall()
            data = [
                {"date": row[0], "average_price": float("{:.2f}".format(row[1]))}
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

            # TODO: Forward Fill
            # Some categories don't have data on some days in the date range. They may have records for 2024-5-11 and then 2024-5-14. 
            # In that case, The 12th and 13th have to be filled with the price on the 11th.

            data = [
                {"date": row[0], "average_price": float("{:.2f}".format(row[1]))}
                for row in records
            ]
        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/average-item-types-price/?food_item=tomato
@api.route("/average-item-types-price/")
class AverageItemTypesPrice(Resource):
    """Returns the current average price of all the item_types of a food_item."""

    def get(self):
        food_item = str(request.args.get("food_item"))

        with get_db_connection().cursor() as cur:
            # TODO: We are not yet filtering by the categories of interest in our JSON.
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
                    continue # average_price = 0

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


# http://127.0.0.1:5000/supermarkets/monthly-average-price/?food_item=tomato&item_type=tomato&category=150%20g
@api.route("/monthly-average-price/")
class MonthlyAverage(Resource):
    """Returns the monthly average price for the food_item, item_type and category chosen in the current year."""

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            # NOTE: This query has been updated to return the values 
            # for the last 12 months. Not just the months in the current year.
            cur.execute(
                """
                SELECT EXTRACT(MONTH FROM CAST(date AS DATE)) AS month, AVG(price) AS monthly_avg_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s
                    AND item_type = %s
                    AND category = %s
                    AND vendor_type = 'Supermarket'
                GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
                ORDER BY EXTRACT(YEAR FROM CAST(date AS DATE)) DESC, EXTRACT(MONTH FROM CAST(date AS DATE)) DESC
                LIMIT 12;
                """,
                (food_item, item_type, category),
            )
            records = cur.fetchall()
            data = [
                {
                    "month": int(row[0]),
                    "monthly_avg_price": float("{:.2f}".format(row[1])),
                }
                for row in records[::-1]
            ]
        return jsonify({"data": data})


@api.route("/mom-percentage/")
class MonthOnMonthPercentage(Resource):
    """Returns the current month on month percentage change and the average price for the most recent month. """

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
                GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
                ORDER BY EXTRACT(YEAR FROM CAST(date AS DATE)) DESC, EXTRACT(MONTH FROM CAST(date AS DATE)) DESC
                LIMIT 2;
                """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()
            (current_month, current_month_average_price), (_, previous_month_avg_price) = records
            percentage_change = (current_month_average_price - previous_month_avg_price) * 100 / previous_month_avg_price

            data = [
                {
                    "current_month": int(current_month),
                    "current_month_average_price": current_month_average_price,
                    "previous_month_avg_price": previous_month_avg_price,
                    "percentage_change": percentage_change,
                }
            ]
            return jsonify({"data": data})


@api.route("/dod-percentage/")
class DayOverDayPercentage(Resource):
    """Returns the current day over day percentage change and average price for the most recent day. """

    def get(self):
        food_item = request.args.get("food_item")
        item_type = request.args.get("item_type")
        category = request.args.get("category")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                SELECT date, AVG(price) AS daily_avg_price
                FROM "Cleaned-Food-Prices"
                WHERE food_item = %s
                    AND item_type = %s
                    AND category = %s
                    AND vendor_type = 'Supermarket'
                GROUP BY date
                ORDER BY date DESC
                LIMIT 2;
                """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()
            (current_day, current_day_average_price), (_, previous_day_avg_price) = records
            percentage_change = (current_day_average_price - previous_day_avg_price) * 100 / previous_day_avg_price

            data = [
                {
                    "current_month": str(current_day),
                    "current_month_average_price": current_day_average_price,
                    "previous_month_avg_price": previous_day_avg_price,
                    "percentage_change": percentage_change,
                }
            ]
            return jsonify({"data": data})


# # http://127.0.0.1:5000/supermarkets/latest-price/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
# @api.route("/latest-price/")
# class LatestPrice(Resource):
#     """Returns the latest daily price of a category's food item."""

#     def get(self):
#         food_item = request.args.get("food_item")
#         item_type = request.args.get("item_type")
#         category = request.args.get("category")

#         with get_db_connection().cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT date,AVG(price) AS avg_price
#                 FROM "Cleaned-Food-Prices"
#                 WHERE food_item = %s
#                     AND item_type = %s
#                     AND category = %s
#                     AND vendor_type = 'Supermarket'
#                 GROUP BY date
#                 ORDER BY date DESC
#                 LIMIT 1;
#                 """,
#                 (food_item, item_type, category),
#             )
#             date, latest_price = cur.fetchone()
#             data = {"date": date, "latest_price": float("{:.2f}".format(latest_price))}
#         return jsonify({"data": data})