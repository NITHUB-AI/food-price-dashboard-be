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


# http://127.0.0.1:5000/nbs/year/?food_item=oil&item_type=vegetable&category=1000%20ml&year=2017
@api.route("/year/")
class FilterByYear(Resource):
    """Returns the prices over the specified year and the year before for a particular food item, item type, and category."""

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


# http://127.0.0.1:5000/nbs/average-item-types-price/?food_item=oil&year=2018
@api.route("/average-item-types-price/")
class AverageItemTypesPrice(Resource):
    """Returns the current average price of all the item_types of a food_item."""

    # TODO: Still in the works
    def get(self):
        food_item = request.args.get("food_item")
        year = request.args.get("year")

        with get_db_connection().cursor() as cur:
            cur.execute(
                """
                    WITH NormalizedPrices AS (
                        SELECT
                            item_type,
                            CASE
                                WHEN category LIKE '%g' THEN price / (NULLIF(REGEXP_REPLACE(category, '\D', '', 'g'), '')::float / 1000)
                                WHEN category LIKE '%ml' THEN price / (NULLIF(REGEXP_REPLACE(category, '\D', '', 'g'), '')::float / 1000)
                                WHEN category LIKE '%pcs' OR category LIKE '%piece' THEN price / NULLIF(REGEXP_REPLACE(category, '\D', '', 'g'), '')::float
                                ELSE price
                            END AS normalized_price
                            FROM "Cleaned-Food-Prices"
                            WHERE food_item = %s
                            AND EXTRACT(YEAR FROM CAST(date AS DATE)) = %s AND source = 'NBS'
                        ),
                        Averages AS (
                            SELECT
                                item_type,
                                AVG(normalized_price) AS average_price
                            FROM NormalizedPrices
                            GROUP BY item_type
                        )
                        SELECT * FROM Averages;

                """,
                (food_item, year),
            )

            records = cur.fetchall()
            data = [
                {
                    "item_type": record[0],
                    "average_price": float(f"{record[1]:.2f}"),
                }
                for record in records
            ]

        return jsonify({"data": data})

# http://127.0.0.1:5000/nbs/average-price-over-years/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/average-price-over-years/")
class AveragePriceOverYears(Resource):
    """Returns the average price in each year for a particular food item, item type and category. """

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


# http://127.0.0.1:5000/nbs/mom-percentage/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/mom-percentage/")
class MonthOnMonthPercentage(Resource):
    """Returns the current month on month percentage."""

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
                        (current_month_price - previous_month_price) * 100 / previous_month_price
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

# http://127.0.0.1:5000/nbs/yoy-percentage/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/yoy-percentage/")
class YearOnYearPercentage(Resource):
    """Returns the current year on year percentage."""

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
                ORDER BY year DESC
                LIMIT 2;
                """,
                (food_item, item_type, category),
            )

            records = cur.fetchall()
            if records:
                (year, current_year_price), (_, previous_year_price) = records
                percentage_change = (
                    (
                        (current_year_price - previous_year_price) * 100 / previous_year_price
                    )
                    if previous_year_price
                    else 0
                )
                data = {
                    "year": year,
                    "current_year_price": float(f"{current_year_price:.2f}"),
                    "previous_year_price": float(f"{previous_year_price:.2f}"),
                    "percentage_change": float(f"{percentage_change:.2f}"),
                }

        return jsonify(data)


# # http://127.0.0.1:5000/nbs/yearly-average-price/?food_item=oil&item_type=vegetable&category=1000%20ml
# @api.route("/yearly-average-price/")
# class YearlyAveragePrice(Resource):
#     """Returns the current year's average price of the food item, item type and category chosen."""

#     def get(self):
#         food_item = request.args.get("food_item")
#         item_type = request.args.get("item_type")
#         category = request.args.get("category")

#         with get_db_connection().cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT
#                 EXTRACT(YEAR FROM CURRENT_DATE) AS year,
#                 AVG(price) AS average_price
#                 FROM "Cleaned-Food-Prices"
#                 WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS' AND
#                 EXTRACT(YEAR FROM CAST(date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)

#                 """,
#                 (food_item, item_type, category),
#             )

#             records = cur.fetchone()
#             year, average_price = records

#             data = [
#                 {
#                     "year": int(year),
#                     "average_price": float(f"{average_price:.2f}"),
#                 }
#             ]

#         return jsonify({"data": data})

# # http://127.0.0.1:5000/nbs/latest-price/?food_item=oil&item_type=vegetable&category=1000%20ml
# @api.route("/latest-price/")
# class LatestPrice(Resource):
#     """ "Returns the latest price of the category picked for the month."""

#     def get(self):
#         food_item = request.args.get("food_item")
#         item_type = request.args.get("item_type")
#         category = request.args.get("category")

#         with get_db_connection().cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT price, date
#                 FROM "Cleaned-Food-Prices"
#                 WHERE food_item = %s AND item_type = %s AND category = %s AND source = 'NBS'
#                 ORDER BY date DESC
#                 LIMIT 1
#             """,
#                 (food_item, item_type, category),
#             )

#             records = cur.fetchone()
#             price, date = records

#             data = {"date": date, "price": f"{price:.2f}"}
#         return jsonify({"data": data})
