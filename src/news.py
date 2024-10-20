import os
import json
import psycopg2

from flask import jsonify, request, abort
from flask_restx import Resource, Namespace

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn