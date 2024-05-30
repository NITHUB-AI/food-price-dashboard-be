import os
import json
import psycopg2

from flask import jsonify, request, abort
from flask_restx import Resource, Namespace

from src.utils import validate_supermarkets_food_item


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

with open("dashboard_items/supermarkets_dashboard.json", "r") as file:
    dashboard_items = json.load(file)


# http://127.0.0.1:5000/supermarkets/all-time/?food_item=tomato&item_type=tomato&category=1000%20g
@api.route("/all-time/")
@api.doc(
    description="Returns the price of a category's food item for all time.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Long grain",
        "category": "Category e.g. 4500 g",
    },
)
class AllTime(Resource):
    """Returns the price of a category's food item for all time."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

            with get_db_connection().cursor() as cur:
                cur.execute(
                    f"""
                    WITH RECURSIVE date_series AS (
                        SELECT 
                            generate_series(
                                (SELECT MIN(date_trunc('day', CAST(date AS DATE))) 
                                FROM "Cleaned-Food-Prices"
                                WHERE food_item = '{food_item}' AND item_type = '{item_type}' 
                                    AND category = '{category}' AND vendor_type = 'Supermarket'),
                                (SELECT MAX(date_trunc('day', CAST(date AS DATE))) 
                                FROM "Cleaned-Food-Prices"
                                WHERE food_item = '{food_item}' AND item_type = '{item_type}' 
                                    AND category = '{category}' AND vendor_type = 'Supermarket'),
                                '1 day'::interval
                            )::date AS date
                    ),
                    cleaned_data AS (
                        SELECT 
                            CAST(date AS date) as date,
                            AVG(price) AS avg_price
                        FROM "Cleaned-Food-Prices"
                        WHERE food_item = '{food_item}' 
                            AND item_type = '{item_type}' 
                            AND category = '{category}' 
                            AND vendor_type = 'Supermarket'
                        GROUP BY CAST(date AS date)
                    ),
                    joined_data AS (
                        SELECT 
                            ds.date,
                            cd.avg_price
                        FROM date_series ds
                        LEFT JOIN cleaned_data cd ON ds.date = cd.date
                    ),
                    recursive_filled_data AS (
                        SELECT 
                            date,
                            avg_price,
                            avg_price AS filled_avg_price
                        FROM joined_data
                        WHERE avg_price IS NOT NULL
                        
                        UNION ALL
                        
                        SELECT 
                            jd.date,
                            jd.avg_price,
                            rfd.filled_avg_price
                        FROM joined_data jd
                        JOIN recursive_filled_data rfd ON jd.date = rfd.date + INTERVAL '1 day'
                        WHERE jd.avg_price IS NULL
                    )
                    SELECT 
                        date,
                        filled_avg_price AS avg_price
                    FROM recursive_filled_data
                    ORDER BY date;
                    """,
                )

                records = cur.fetchall()

                if not records:
                    return abort(404, "No data found")
                data = [
                    {
                        "date": str(row[0]),
                        "average_price": float("{:.2f}".format(row[1])),
                    }
                    for row in records
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=1000%20g
# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=1000%20g&current_month=true
# http://127.0.0.1:5000/supermarkets/year/?food_item=tomato&item_type=tomato&category=1000%20g&current_week=true
@api.route("/year/")
@api.doc(
    description="Returns the price of a category's food item for the current year with filter for current month and/or current week.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Long grain",
        "category": "Category e.g. 4500 g",
        "current_month": "Filter for current month. Default is false.",
        "current_week": "Filter for current week. Default is false.",
    },
)
class FilterByCurrentYear(Resource):
    """Returns the price of a category's food item for the current year with filter for current month and/or current week."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            current_month = request.args.get("current_month", "false").lower().strip()
            current_week = request.args.get("current_week", "false").lower().strip()
            assert current_month in ["true", "false"], "Invalid Current Month."
            assert current_week in ["true", "false"], "Invalid Current Month."

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

            prequel = f"""
                    WITH RECURSIVE date_series AS (
                        SELECT 
                            generate_series(
                                (SELECT MIN(date_trunc('day', CAST(date AS DATE))) 
                                FROM "Cleaned-Food-Prices"
                                WHERE food_item = '{food_item}' AND item_type = '{item_type}' AND category = '{category}' AND vendor_type = 'Supermarket'),
                                (SELECT MAX(date_trunc('day', CAST(date AS DATE))) 
                                FROM "Cleaned-Food-Prices"
                                WHERE food_item = '{food_item}' AND item_type = '{item_type}' AND category = '{category}' AND vendor_type = 'Supermarket'),
                                '1 day'::interval
                            )::date AS date
                    ),
                    cleaned_data AS ("""

            sequel = """
                    ),
                    joined_data AS (
                        SELECT 
                            ds.date,
                            cd.avg_price
                        FROM date_series ds
                        LEFT JOIN cleaned_data cd ON ds.date = cd.date
                    ),
                    recursive_filled_data AS (
                        SELECT 
                            date,
                            avg_price,
                            avg_price AS filled_avg_price
                        FROM joined_data
                        WHERE avg_price IS NOT NULL
                        
                        UNION ALL
                        
                        SELECT 
                            jd.date,
                            jd.avg_price,
                            rfd.filled_avg_price
                        FROM joined_data jd
                        JOIN recursive_filled_data rfd ON jd.date = rfd.date + INTERVAL '1 day'
                        WHERE jd.avg_price IS NULL
                    )
                    SELECT 
                        date,
                        filled_avg_price AS avg_price
                    FROM recursive_filled_data
                    ORDER BY date;
                    """

            query = f"""
                    SELECT CAST(date AS date) as date, AVG(price) AS avg_price
                    FROM "Cleaned-Food-Prices"
                    WHERE food_item = '{food_item}' 
                        AND item_type = '{item_type}' 
                        AND category = '{category}' 
                        AND vendor_type = 'Supermarket'
                        AND EXTRACT(YEAR FROM CAST(date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)
                    """

            if current_month.lower() == "true":
                query += " AND EXTRACT(MONTH FROM CAST(date AS DATE)) = EXTRACT(MONTH FROM CURRENT_DATE)"

            if current_week.lower() == "true":
                query += " AND EXTRACT(WEEK FROM CAST(date AS DATE)) = EXTRACT(WEEK FROM CURRENT_DATE)"

            query += " GROUP BY CAST(date AS date)\n"  # ORDER BY date;"

            query += sequel
            query = prequel + query

            with get_db_connection().cursor() as cur:
                cur.execute(
                    query,
                )
                records = cur.fetchall()

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = [
                    {
                        "date": str(row[0]),
                        "average_price": float("{:.2f}".format(row[1])),
                    }
                    for row in records
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/average-item-types-price/?food_item=tomato
@api.route("/average-item-types-price/")
@api.doc(
    description="Returns the current average price of all the item_types of a food_item.",
    params={"food_item": "Food Item e.g. Rice"},
)
class AverageItemTypesPrice(Resource):
    """Returns the current average price of all the item_types of a food_item."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()

            if not all([food_item]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

            category_filter = ""
            for item_type, categories in dashboard_items[food_item].items():
                for category in categories:
                    category_filter += (
                        f"(item_type = '{item_type}' AND category = '{category}')"
                    )
                    category_filter += " OR "
            category_filter = category_filter.rstrip(" OR ")

            with get_db_connection().cursor() as cur:
                cur.execute(
                    f"""
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
                            AND food_item = %s AND vendor_type = 'Supermarket'
                            AND {category_filter}

                    )
                    SELECT item_type, AVG(unit_price) AS average_price, unit
                    FROM LatestRecords
                    GROUP BY item_type, unit;
                    """,
                    (food_item, food_item),
                )

                records = cur.fetchall()

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = []
                for item_type, average_price, unit in records:
                    if average_price is None:
                        continue  # average_price = 0

                    if unit in conversion_dictionary:
                        conversion_factor, new_unit = conversion_dictionary[unit]
                        converted_price = (
                            average_price * conversion_factor
                            if average_price != 0
                            else 0
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

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/monthly-average-price/?food_item=tomato&item_type=tomato&category=150%20g
@api.route("/monthly-average-price/")
@api.doc(
    description="Returns the monthly average price for the food_item, item_type and category chosen in the current year.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Long grain",
        "category": "Category e.g. 4500 g",
    },
)
class MonthlyAverage(Resource):
    """Returns the monthly average price for the food_item, item_type and category chosen in the current year."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                data = [
                    {
                        "month": int(row[0]),
                        "monthly_avg_price": float("{:.2f}".format(row[1])),
                    }
                    for row in records[::-1]
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/mom-percentage/?food_item=tomato&item_type=tomato&category=1000%20g
@api.route("/mom-percentage/")
@api.doc(
    description="Returns the current month on month percentage change and the average price for the most recent month.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Long grain",
        "category": "Category e.g. 4500 g",
    },
)
class MonthOnMonthPercentage(Resource):
    """Returns the current month on month percentage change and the average price for the most recent month."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                (current_month, current_month_average_price), (
                    _,
                    previous_month_avg_price,
                ) = records
                percentage_change = (
                    (current_month_average_price - previous_month_avg_price)
                    * 100
                    / previous_month_avg_price
                )

                data = [
                    {
                        "current_month": int(current_month),
                        "current_month_average_price": current_month_average_price,
                        "previous_month_avg_price": previous_month_avg_price,
                        "percentage_change": round(percentage_change, 2),
                    }
                ]

        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# http://127.0.0.1:5000/supermarkets/dod-percentage/?food_item=tomato&item_type=tomato&category=1000%20g
@api.route("/dod-percentage/")
@api.doc(
    description="Returns the current day over day percentage change and average price for the most recent day.",
    params={
        "food_item": "Food item e.g. Rice",
        "item_type": "Item type e.g. Long grain",
        "category": "Category e.g. 4500 g",
    },
)
class DayOverDayPercentage(Resource):
    """Returns the current day over day percentage change and average price for the most recent day."""

    def get(self):
        try:
            food_item = request.args.get("food_item", "").lower().strip()
            item_type = request.args.get("item_type", "").lower().strip()
            category = request.args.get("category", "").lower().strip()

            if not all([food_item, item_type, category]):
                return abort(400, "Missing required parameters")

            check = validate_supermarkets_food_item(food_item, dashboard_items)
            if check is not None:
                return check

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

                if not records:
                    return abort(404, "No records found. Confirm query parameters.")

                (current_day, current_day_average_price), (
                    _,
                    previous_day_avg_price,
                ) = records
                percentage_change = (
                    (current_day_average_price - previous_day_avg_price)
                    * 100
                    / previous_day_avg_price
                )

                data = [
                    {
                        "current_day": str(current_day),
                        "current_day_average_price": current_day_average_price,
                        "previous_day_avg_price": previous_day_avg_price,
                        "percentage_change": round(percentage_change, 2),
                    }
                ]
        except psycopg2.Error as e:
            return abort(500, f"Database error: {str(e)}")

        # except Exception as e:
        #     return abort(500, f"An unexpected error occurred: {str(e)}")

        return jsonify({"data": data})


# # http://127.0.0.1:5000/supermarkets/latest-price/?food_item=tomato&item_type=tomato&category=150%20g&year=2024
# @api.route("/latest-price/")
# class LatestPrice(Resource):
#     """Returns the latest daily price of a category's food item."""

#     def get(self):
#         food_item = request.args.get("food_item").lower().strip()
#         item_type = request.args.get("item_type").lower().strip()
#         category = request.args.get("category").lower().strip()

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
