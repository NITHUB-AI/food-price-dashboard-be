import os
import psycopg2

from flask import Flask, jsonify, request, Request
from flask_restx import Api, Resource
from .src.nbs import api as nbs_api

app = Flask(__name__)

api = Api(
    version="1.0",
    title="Food Price Prediction API",
    description="An API for getting food prices, daily, monthly, and yearly.",
    license="MIT",
    contact="NITDA AI Team.",
)

api.add_namespace(nbs_api, "/nbs")
api.init_app(app)

if __name__ == "__main__":
    app.run(debug=True)
