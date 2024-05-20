import os
import sys
import psycopg2

from flask import Flask, jsonify, request, Request
from flask_restx import Api, Resource

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.nbs import api as nbs_api
from src.supermarkets import api as supermarkets_api


app = Flask(__name__)

api = Api(
    version="1.0",
    title="Food Price Prediction API",
    description="An API for getting food prices, daily, monthly, and yearly.",
    license="MIT",
    contact="NITDA AI Team.",
)

api.add_namespace(nbs_api, "/nbs")
api.add_namespace(supermarkets_api, "/supermarkets")
api.init_app(app)

if __name__ == "__main__":
    app.run(debug=True)
