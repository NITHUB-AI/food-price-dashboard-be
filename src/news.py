import os
import json
import psycopg2
import pandas as pd

from flask import jsonify, request, abort
from flask_restx import Resource, Namespace

from src.summary_levels import summarize_gemini 
from datetime import datetime, timedelta


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER_NAME"),
        password=os.getenv("PASSWORD"),
    )
    return conn



api = Namespace("News", description="News summmary as related to real-world influence on food prices")

@api.route("/day-level-summary/")
@api.doc(
    description="Returns the summary of all news related to possible effect on food prices for the previous day."
)
class DayLevelSummary(Resource):
    """Returns summary of all news related to possible effect on food prices for the previous day."""

    def get(self):
        try:
            with get_db_connection() as conn:
                # Get yesterday's date in YYYY-MM-DD format
                yesterday = datetime.today() - timedelta(days=1)

                # Format the date as YYYY-MM-DD
                yesterday_str = yesterday.strftime('%Y-%m-%d')
                sub = pd.read_sql_query(f"""SELECT date, title, categories, article_summary FROM articles WHERE DATE(date) = '{yesterday_str}';""", conn)
                
                sub['date']=sub['date'].apply(lambda x:f"Date News was published: {str(x)}\n\nNews Summary:\n")
                sub['dated_summary']=sub['date']+sub['article_body']
                summaries = sub['dated_summary'].tolist()
                result = {
                    "summary": summarize_gemini("".join(summaries))
                        }
            return json.dumps(result)
        except Exception as err:
            return abort(400, f"Error processing request: {err}")

@api.route("/week-level-summary/")
@api.doc(
    description="Returns the summary of all news related to possible effect on food prices for the previous week."
)
class WeekLevelSummary(Resource):
    """Returns summary of all news related to possible effect on food prices for the previous day."""

    def get(self):
        try:
            with get_db_connection() as conn:
                # Get last week's range date in YYYY-MM-DD format
                yesterday = datetime.today() - timedelta(days=1)
                # Format the date as YYYY-MM-DD
                yesterday_str = yesterday.strftime('%Y-%m-%d')

                last_week = datetime.today() - timedelta(days=8)
                last_week_str = last_week.strftime('%Y-%m-%d')
                sub = pd.read_sql_query(f"""SELECT date, title, categories, article_summary FROM articles WHERE DATE(date) BETWEEN '{yesterday_str}' AND '{last_week_str}';""", conn)
                sub['date']=sub['date'].apply(lambda x:f"Date News was published: {str(x)}\n\nNews Summary:\n")
                sub['dated_summary']=sub['date']+sub['article_body']
                summaries = sub['dated_summary'].tolist()
                result = {
                    "summary": summarize_gemini("".join(summaries))
                        }
             
            return json.dumps(result)
        except:
            return abort(400, "Error processing request")
        

@api.route("/month-level-summary/")
@api.doc(
    description="Returns the summary of all news related to possible effect on food prices for the last 1 month."
)
class MonthLevelSummary(Resource):
    """Returns summary of all news related to possible effect on food prices for the previous day."""

    def get(self):
        try:
            with get_db_connection() as conn:
                # Get last week's range date in YYYY-MM-DD format
                yesterday = datetime.today() - timedelta(days=1)
                # Format the date as YYYY-MM-DD
                yesterday_str = yesterday.strftime('%Y-%m-%d')

                last_month = datetime.today() - timedelta(days=31)
                last_month_str = last_month.strftime('%Y-%m-%d')
                sub = pd.read_sql_query(f"""SELECT date, title, categories, article_summary FROM articles WHERE DATE(date) BETWEEN '{yesterday_str}' AND '{last_month_str}';""", conn)
                sub['date']=sub['date'].apply(lambda x:f"Date News was published: {str(x)}\n\nNews Summary:\n")
                sub['dated_summary']=sub['date']+sub['article_body']
                summaries = sub['dated_summary'].tolist()
                result = {
                    "summary": summarize_gemini("".join(summaries))
                        }
            return json.dumps(result)
        except:
            return abort(400, "Error processing request")