from flask import Flask, request, render_template
#import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import psycopg2 as pg
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.orm import relationship, backref
import pandas.io.sql as psql
import pandas as pd
from flask_jsonpify import jsonpify

from flask_cors import CORS, cross_origin

app = Flask(__name__, template_folder="template")
app.config.from_object(__name__)
app.config['DEBUG'] = True
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

con = pg.connect(database="postgres", user="postgres", password="postgres", host="localhost", port="5432")
cursor = con.cursor()

@app.route("/", methods=['get'])
@cross_origin()
def runs_table():
    #dataframe = psql.DataFrame("select * from runs", con)
    #query = "select hash_id, count(*) from runs group by hash_id order by count desc"
    query = "SELECT hash_id, count(*) FROM runs WHERE year_month in ( SELECT MAX(year_month) from runs ) group by hash_id order by count desc"
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data

    """
    df = pd.read_sql_query("select * from runs", con)
    #print(df)
    df.to_csv("runstable.csv", index = False, header = False)
    df = pd.read_csv("/home/ndoss/Documents/fastapi/runstable.csv", names = ["run_id", "station_id", "ptu_id", "utb_id", "hash_id", "start", "duration", "outcome_val", "outcome_msg", "mongo_Id", "year_week", "year_month","order_Id"])

    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return "hello df"
    #return render_template("../test.html", tables = [df.to_html(classes = "data")],titles=df.columns.values )
    cursor.execute("select station_id, outcome_val from runs where outcome_val  <> 0")
    result = cursor.fetchall()
    return jsonify(result)
    """



@app.route("/stations", methods = ["get"])
def station_table():
    df = pd.read_sql_query("select * from stations", con)
    df.to_csv("stationstable.csv", index = False, header = False)

    

if __name__ == "__main__":
    app.run()





