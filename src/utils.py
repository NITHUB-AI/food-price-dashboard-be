import json
from flask import jsonify, request


with open("dashboard_items/supermarkets_dashboard.json", "r") as file:
    supermarkets_dashboard = json.load(file)


def validate_food_item(food_item, valid_items):
    """
    Validates that the provided food item is in the list of valid items.
    """
    if food_item not in valid_items:
        valid_items_list = ", ".join(valid_items.keys())
        return jsonify(
            {
                "message": f"Please enter a valid food item. The valid food items are: {valid_items_list}"
            }
        )

    return None
