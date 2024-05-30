import json
from flask import jsonify, request, abort


with open("dashboard_items/nbs_dashboard.json", "r") as file:
    nbs_dashboard = json.load(file)

with open("dashboard_items/supermarkets_dashboard.json", "r") as file:
    supermarkets_dashboard = json.load(file)


def validate_nbs_food_item(food_item, nbs_dashboard):
    """
    Validates that the provided food item is in the list of valid items.
    """
    if food_item not in nbs_dashboard:
        valid_items_list = ", ".join(nbs_dashboard.keys())
        return abort(
            400,
            f"Please enter a valid food item. The valid food items are: {valid_items_list}",
        )


def validate_supermarkets_food_item(food_item, supermarkets_dashboard):
    """
    Validates that the provided food item is in the list of valid items.
    """
    if food_item not in supermarkets_dashboard:
        valid_items_list = ", ".join(supermarkets_dashboard.keys())
        return abort(
            400,
            f"Please enter a valid food item. The valid food items are: {valid_items_list}",
        )
