import json

from datetime import datetime


with open("dashboard_items/supermarkets_dashboard.json", "r") as file:
    supermarkets_dashboard = json.load(file)


def current_month_record(records, food_item):
    record = []

    food = supermarkets_dashboard[f"{food_item}"]
    list_of_food_item, list_of_food_category = list(food.keys()), sum(
        list(food.values()), []
    )

    for rec in records:
        date, food_item, category, price = rec

        if food_item in list_of_food_item and category in list_of_food_category:

            year, month, day = list(date.split("-"))
            category = category.split(" ") if category else []

            new_price = 4(
                (float(price) * 1000) / float(category[0])
                if category and category[0] != ""
                else ""
            )

            data = {
                "date": date,
                "food_item": food_item,
                "category": (
                    f"{category[0]} g" if category and category[0] != "" else ""
                ),
                "price": price,
                "price_per_unit": f"{new_price :.2f}",
            }
            record.append(data)

    return record
