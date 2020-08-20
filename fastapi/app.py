from flask import Flask, request, render_template
import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
from flask_jsonpify import jsonpify

from flask_cors import CORS, cross_origin

app = Flask(__name__, template_folder="template")
app.config.from_object(__name__)
app.config['DEBUG'] = True
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

con = pg.connect(database="final_postgres", user="postgres", password="postgres", host="localhost", port="5432")
cursor = con.cursor()

@app.route("/hashid", methods=['get'])
@cross_origin()
def runs_table():

    #query = "SELECT hash_id, count(*) FROM runs WHERE year_month in ( SELECT MAX(year_month) from runs ) group by hash_id "
    
    query = "Select hash_id, count(*) from runs group by hash_id "
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data


   

@app.route("/test", methods  = ['get'])
@cross_origin()

def test_result():
    query ="select mongo_id, stop_if_fail, count(*)from runs, verifications where runs.run_id = verifications.run_id and hash_id = %s group by runs.mongo_id, verifications.stop_if_fail"
    #query = "select * from runs where hash_id = %s"
    values = request.args.get('id')
    result_list = []
    df = pd.read_sql_query(query, con, params=[values])
    new_df = df[df['stop_if_fail'] == 1]
    length_newdf = len(new_df)
    statistics = df.describe(include='all')
    total_of_0 = len(df) - length_newdf
    result_list.append(values)
    result_list.append(total_of_0)
    result_list.append(length_newdf)
    json_data = jsonpify(result_list)
    return json_data


@app.route("/msg", methods = ["get"])
@cross_origin()

def test_msg():
    query = "select start from runs where hash_id = %s and outcome_val = 0  order by runs.start asc"
    values = request.args.get('id')
    df = pd.read_sql_query(query, con, params = [values])
   
    df['new_date'] = [d.date().strftime('%Y-%m-%d') for d in df['start']]

    df = df.groupby(['new_date']).agg(count_col=pd.NamedAgg(column="new_date", aggfunc="count")).reset_index()
   
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data

    
@app.route("/ptus", methods = ["get"])
@cross_origin()

def list_ptus():
    query =


if __name__ == "__main__":
    app.run()


"""

select hash_Id from runs where mongo_Id = ;
select * from runs where station_id = 
select count(type) from runs, verifications where runs.run_id = verifications.run_id and hash_id = '7cbf9116f7bbef878b8ae5a444dea0cd'
select  runs.run_id, type, count(*), mongo_id from runs,  verifications where runs.run_id = verifications.run_id and hash_id = '7cbf9116f7bbef878b8ae5a444dea0cd' group by runs.run_id, verifications.type order by runs.run_id asc 
select mongo_id, stop_if_fail from runs, verifications where runs.run_id = verifications.run_id and hash_id = '7cbf9116f7bbef878b8ae5a444dea0cd'group by runs.mongo_id, verifications.stop_if_fail

select mongo_id, stop_if_fail, count(*)from runs, verifications where runs.run_id = verifications.run_id and hash_id = '5dfccfc2e46580a4c4505c150a513389'group by runs.mongo_id, verifications.stop_if_fail


3142274b0e2c7848438243ada0814c53
select distinct year_month, count(outcome_msg) from runs where outcome_val = 0 group by runs.year_month, runs.outcome_msg order by runs.year_month asc

select distinct start, count(outcome_msg) from runs where outcome_val = 0 group by runs.start, runs.outcome_msg order by runs.start asc


//for global good and bad cunt
select runs.ptu_id,  count(runs.outcome_val) as good_val, cal_bad_val.bad_val
from runs, (select count(outcome_val) as bad_val
		   from runs
		   where runs.outcome_val != 0 and runs.ptu_id = 1) as cal_bad_val
where runs.outcome_val = 0 and runs.ptu_id = 1
group by runs.ptu_id, cal_bad_val.bad_val


API'S
//ptu_id, date, ratio
select outcome_table.date, outcome_table.ptu_id, round((outcome_table.outcome_zero::decimal / outcome_table.total_test::decimal), 2)as ratio
from (select single_ptu.date, (count(single_ptu.date)) as Total_test, COUNT(CASE single_ptu.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero, (single_ptu.ptu_id) as ptu_id
from (select  runs.station_id, runs.outcome_val, (runs.start::date) as date, (ptus.ptu_serial) as ptu_id
		from runs, ptus
		where runs.ptu_id = ptus.ptu_id and ptu_serial = 35
		group by runs.station_id, runs.outcome_val, runs.start, ptus.ptu_serial
		order by date asc) as single_ptu
group by single_ptu.date, single_ptu.ptu_id) as outcome_table


// ptu_id for each plot
select ptu_serial from ptus


//with order_id and order_qty
"""