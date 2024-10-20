import os
import json
import psycopg2
import pandas as pd
import datetime

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

conn=get_db_connection()

if conn is not None:
    status='Success'
else:
    status='failes'
print('Connection status: {status}')

df_news = pd.read_sql_query("""SELECT date, title, categories, tags, query, article->'body' as article_body FROM articles""", conn)
api = Namespace("News", description="Summary of news relating to food prices")

@api.route("/daily/")
@api.doc(
    description="Returns Detailed summary for all news relating to food prices for a particular day",
    params={"Date": "YYYY/MM/DD"},
    )

class DailySummary(Resource):
    "Returns the summary of all news articles relating to food prices.The summaries try as much as possible to explain the various factors that contributed to the change in food prices for a particular day"
    
    def get(self):
        
        try:
            query_date=request.args.get("Date", "")
            date= datetime.datetime.strptime(query_date, "%Y/%m/%d")
            date=str(date).split(' ')[0]
  
            df_news['date']=pd.to_datetime(df_news['date'])
            subset=df_news[df_news['date'].dt.strftime('%Y-%m-%d') == date]
            
            subset_articles=subset['article_body'].values.tolist()
            subset_dates=subset['date'].values.tolist()
        
            
        except Exception as e:
            return abort(500, f"An unexpected error occurred: {str(e)}")
        
        
        return jsonify({'Status':'No Summaries yet'})
        #return jsonify({date:article for date , article in zip(subset_dates,subset_articles)})
    
        
        
        
        
        
@api.route("/weekly/")
@api.doc(
    description="Returns Detailed summary for all news relating to food prices for a particular week.",
    params={"Date": "YYYY/MM/DD"},
    )
class WeeklySummary(Resource):
    "Returns the summary of all news articles relating to food prices.The summaries try as much as possible to explain the various factors that contributed to the change in food prices for a particular week"
    
    def get(self):    
        return jsonify({'Status':'No Summaries yet'})
    




@api.route("/monthly/")
@api.doc(
    description="Returns Detailed summary for all news relating to food prices for a particular month.",
    params={"Date": "YYYY/MM/DD"},
    )
class MonthlySummary(Resource):
    "Returns the summary of all news articles relating to food prices.The summaries try as much as possible to explain the various factors that contributed to the change in food prices for a particular month"
    
    def get(self):
        
            
       return jsonify({'Status':'No Summaries yet'})
