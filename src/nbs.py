import os
import psycopg2
import json

from flask import jsonify, request, abort
from flask_restx import Resource, Namespace
from .utils import validate_nbs_food_item


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn


api = Namespace("NBS", description="NBS food price data operations")

conversion_dictionary = {"g": [1000, "kg"], "ml": [1000, "L"], "pcs": [1, "pcs"]}

# http://127.0.0.1:5000/nbs/year/?food_item=oil&item_type=vegetable&category=1000%20ml&year=2017

with open("dashboard_items/nbs_dashboard.json", "r") as file:
    nbs_dashboard_file = json.load(file)


# http://127.0.0.1:5000/nbs/year/?food_item=oil&item_type=vegetable&category=1%20ltr&year=2017
@api.route("/year/")
@api.doc(
    description="Returns the prices over the specified year and the year before for a particular food item, item type, and category.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Local",
        "category": "Category e.g. 1000 g",
        "year": "Year (starting from 2016) e.g. 2017",
    },
)
class FilterByYear(Resource):
    """Returns the prices over the specified year and the year before for a particular food item, item type, and category."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()
            year = request.args.get("year", "0").strip()

            # Calculate the previous year
            previous_year = str(int(year) - 1)

            if not all([food_item, item_type, category, year]):
                return abort(400, "Missing required parameters")
            
            if int(year) < 2016:
                return abort(400, "Invalid year. The earliest year is 2016.")

            check = validate_nbs_food_item(food_item, nbs_dashboard_file)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = [
                    {"date": record[0], "value": float("{:.2f}".format(record[1]))}
                    for record in records
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/average-item-types-price/?food_item=oil&year=2018
@api.route("/average-item-types-price/")
@api.doc(
    description="Returns the current average price of all the item_types of a food_item.",
    params={"food_item": "Specify the food item to get average prices"},
)
class AverageItemTypesPrice(Resource):
    """Returns the current average price of all the item_types of a food_item."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()

            if not all([food_item]):
                return abort(400, "Missing required parameters")

            check = validate_nbs_food_item(food_item, nbs_dashboard_file)
            if check is not None:
                return check

            item_type_filter = ",".join(
                f"'{item_type}'" for item_type in nbs_dashboard_file[food_item]
            )

            with get_db_connection().cursor() as cur:
                cur.execute(
                    f"""
                    WITH latest AS (
                        SELECT *, DATE_TRUNC('month', MAX(CAST(date AS TIMESTAMP)) OVER()) AS max_date
                        FROM "Cleaned-Food-Prices" 
                        WHERE category IS NOT NULL AND LENGTH(category) > 0
                        AND food_item = %s AND source = 'NBS'
                    ), datapoints AS (
                        SELECT 
                            item_type, 
                            category, 
                            price, 
                            SPLIT_PART(category, ' ', 1) AS numeric_part,
                            SPLIT_PART(category, ' ', 2) AS unit,
                            price / NULLIF(CAST(SPLIT_PART(category, ' ', 1) AS numeric), 0) AS unit_price
                        FROM latest
                        WHERE DATE_TRUNC('month', CAST(date AS TIMESTAMP)) = max_date  AND item_type in ({item_type_filter})
                    ) 
                    SELECT item_type, AVG(unit_price) AS average_price, unit, MIN(numeric_part) AS min_numeric_part, MAX(price) AS max_price
                    FROM datapoints 
                    GROUP BY item_type, unit;
                    """,
                    (food_item,),
                )

                records = cur.fetchall()

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = []
                for record in records:
                    item_type, average_price, unit, min_numeric_part, max_price = record
                    if unit in conversion_dictionary:
                        conversion_factor, new_unit = conversion_dictionary[unit]
                        converted_price = average_price * conversion_factor
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
                                "average_price": round(average_price, 2),
                                "unit": unit,
                            }
                        )

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/average-price-over-years/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/average-price-over-years/")
@api.doc(
    description="Returns the average price in each year for a particular food item, item type and category",
    params={
        "food_item": "Specify the food item e.g. potato",
        "item_type": "Specify its item_type e.g. irish",
        "category": "Specify the category e.g. 1000 g",
    },
)
class AveragePriceOverYears(Resource):
    """Returns the average price in each year for a particular food item, item type and category."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_nbs_food_item(food_item, nbs_dashboard_file)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = [
                    {"year": int(year), "average_price": float(f"{average_price:.2f}")}
                    for year, average_price in records
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/nbs/mom-percentage/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/mom-percentage/")
@api.doc(
    description="Returns the current month on month percentage.",
    params={
        "food_item": "Specify the food item e.g. potato",
        "item_type": "Specify its item_type e.g. irish",
        "category": "Specify the category e.g. 1000 g",
    },
)
class MonthOnMonthPercentage(Resource):
    """Returns the current month on month percentage."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_nbs_food_item(food_item, nbs_dashboard_file)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                if records:
                    month, current_month_price, previous_month_price = records
                    percentage_change = (
                        (
                            (current_month_price - previous_month_price)
                            * 100
                            / previous_month_price
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

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify(data)


# http://127.0.0.1:5000/nbs/yoy-percentage/?food_item=oil&item_type=vegetable&category=1000%20ml
@api.route("/yoy-percentage/")
@api.doc(
    description="Returns the current year on year percentage.",
    params={
        "food_item": "Specify the food item e.g. potato",
        "item_type": "Specify its item_type e.g. irish",
        "category": "Specify the category e.g. 1000 g",
    },
)
class YearOnYearPercentage(Resource):
    """Returns the current year on year percentage."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_nbs_food_item(food_item, nbs_dashboard_file)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                if records:
                    (year, current_year_price), (_, previous_year_price) = records
                    percentage_change = (
                        (
                            (current_year_price - previous_year_price)
                            * 100
                            / previous_year_price
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

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify(data)


# # http://127.0.0.1:5000/nbs/yearly-average-price/?food_item=oil&item_type=vegetable&category=1000%20ml
# @api.route("/yearly-average-price/")
# class YearlyAveragePrice(Resource):
#     """Returns the current year's average price of the food item, item type and category chosen."""

#     def get(self):
#         food_item = request.args.get("food_item").lower().strip()
#         item_type = request.args.get("item_type").lower().strip()
#         category = request.args.get("category").lower().strip()

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
#         food_item = request.args.get("food_item").lower().strip()
#         item_type = request.args.get("item_type").lower().strip()
#         category = request.args.get("category").lower().strip()

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
