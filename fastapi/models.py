from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import psycopg2

app = Flask(__name__)



con = psycopg2.connect(host="localhost", port = "5432", database="postgres", user="postgres", password="postgres")
cursor = con.cursor()




@app.route('/stations', methods=['GET'])
def station_id_get():
    sql_query ="""SELECT * FROM stations"""
    out = cursor.execute(sql_query)
    context_records = out.fetchall()
    print(context_records)
    ContextRootKeys = []
    outp ="Print each row and it's columns values"
    for row in context_records:
        ContextRootKeys.append(row[1])
    con.commit()
    print(ContextRootKeys)
    return outp

if __name__ == "__main__":
    app.run()