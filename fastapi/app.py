from flask import Flask, request, render_template
import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
from flask_jsonpify import jsonpify
import datetime as dt
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

"""

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
    print(df)
    df['new_date'] = [d.date().strftime('%Y-%m-%d') for d in df['start']]

    df = df.groupby(['new_date']).agg(count_col=pd.NamedAgg(column="new_date", aggfunc="count")).reset_index()
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data

 """   

#ptu serial id only
@app.route("/ptu_serial", methods = ["get"])
@cross_origin()

def ptu_serial():
    query = """
                select distinct ptu_serial
                from ptus
                order by ptu_serial asc
            """
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data

# for manufacturers
@app.route("/manufacturer", methods = ["get"])
@cross_origin()

def base_station_manufacturers():
    query = """select distinct ems from utbs"""
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data


# total runs in the databases
@app.route("/total_runs", methods = ["get"])
@cross_origin()

def total_runs():
    query = """select run_id from runs"""
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data



# base stations names in databases
@app.route("/base_stations", methods= ["get"])
@cross_origin()
def base_stations():
    query = """select distinct hardware from stations"""
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data






#ptu, outcome_ratio on every day basis    
@app.route("/ptus", methods = ["get"])
@cross_origin()
def ptus_dw_comparision():
    query = """select outcome_table.date, outcome_table.ptu_id, round((outcome_table.outcome_zero::decimal / outcome_table.total_test::decimal), 2)as ratio
            from (select single_ptu.date, (count(single_ptu.date)) as Total_test, COUNT(CASE single_ptu.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero, (single_ptu.ptu_id) as ptu_id
                from (select  runs.station_id, runs.outcome_val, (runs.start::date) as date, (ptus.ptu_serial) as ptu_id
		        from runs, ptus
		        where runs.ptu_id = ptus.ptu_id and ptu_serial = 16
		        group by runs.station_id, runs.outcome_val, runs.start, ptus.ptu_serial
		        order by date asc) as single_ptu
            group by single_ptu.date, single_ptu.ptu_id) as outcome_table"""
    df = pd.read_sql_query(query, con)
    df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
    df["Week_Number"] = [d.isocalendar()[1] for d in df["date"]]
    del df['date']
    print(df)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data


#dates between range and ptu_id
@app.route("/ptu_filter", methods =['get'])
@cross_origin()

def ptus_daterange():

    query = """ select outcome_table.date, outcome_table.ptu_id, round((outcome_table.outcome_zero::decimal / outcome_table.total_test::decimal), 2)as ratio
                from (select single_ptu.date, (count(single_ptu.date)) as Total_test, COUNT(CASE single_ptu.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero, (single_ptu.ptu_id) as ptu_id
                from (select  runs.station_id, runs.outcome_val, (runs.start::date) as date, (ptus.ptu_serial) as ptu_id
                        from runs, ptus
                        where runs.ptu_id = ptus.ptu_id and ptu_serial = %s
                        group by runs.station_id, runs.outcome_val, runs.start, ptus.ptu_serial
                        order by date asc) as single_ptu
                group by single_ptu.date, single_ptu.ptu_id) 
                as outcome_table
                where outcome_table.date BETWEEN SYMMETRIC %s AND %s
                order by outcome_table.date asc
    """

    values0 = request.args.get('ptu_id')
    values1 = request.args.get('from')
    values2 = request.args.get('to')

    if values0  == None :
        
        values0 = '16'
        print(values0, values1, values2)
        df = pd.read_sql_query(query, con, params = [values0, values1, values2])
        df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
        del df['date']
        print(df)
        df_list = df.values.tolist()
        json_data = jsonpify(df_list)
        return json_data
    elif values1 == None:
  
        
        values1 = '2017-07-03'
        
        print(values0, values1, values2)
        df = pd.read_sql_query(query, con, params = [values0, values1, values2])
        df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
        del df['date']
        print(df)
        df_list = df.values.tolist()
        json_data = jsonpify(df_list)
        return json_data

    elif values2 == None:
        values2 = '2017-08-30'
        print(values0, values1, values2)
        df = pd.read_sql_query(query, con, params = [values0, values1, values2])
        df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
        del df['date']
        print(df)
        df_list = df.values.tolist()
        json_data = jsonpify(df_list)
        return json_data

    else:
        print(values0, values1, values2)
        df = pd.read_sql_query(query, con, params = [values0, values1, values2])
        df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
        del df['date']
        print(df)
        df_list = df.values.tolist()
        json_data = jsonpify(df_list)
        return json_data
    


@app.route("/ptu_serial", methods=["get"])
@cross_origin()

def ptu_serials():
    query = """select distinct ptu_serial from ptus"""
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data





















# limit the number of runs
@app.route("/ptu_limit", methods =['get'])
@cross_origin()

def ptus_limit():
    query = """ select outcome_table.date, outcome_table.ptu_id, round((outcome_table.outcome_zero::decimal / outcome_table.total_test::decimal), 2)as ratio 
                from (select single_ptu.date, (count(single_ptu.date)) as Total_test, COUNT(CASE single_ptu.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero, (single_ptu.ptu_id) as ptu_id
                from (select  runs.station_id, runs.outcome_val, (runs.start::date) as date, (ptus.ptu_serial) as ptu_id
                        from runs, ptus
                        where runs.ptu_id = ptus.ptu_id and ptu_serial = 16
                        group by runs.station_id, runs.outcome_val, runs.start, ptus.ptu_serial
                        order by date asc) as single_ptu
                group by single_ptu.date, single_ptu.ptu_id) 
                as outcome_table
                where outcome_table.date BETWEEN SYMMETRIC '2017-02-02' AND '2017-09-12'
                order by outcome_table.date asc
                limit %s
    """
    values = request.args.get('limit')

    df = pd.read_sql_query(query, con, params = [values])
    df["dates"] =  [d.strftime('%Y-%m-%d') for d in df['date']]
    del df['date']
    print(df)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data


#
@app.route("/order_id", methods=['get'])
@cross_origin()

def order_id():
    query = """
                select order_details.order_id, order_details.number_of_BS, round((order_details.outcome_zero::decimal / order_details.total::decimal), 2) as ratio
                from (select order_id, (count(runs.station_id)) as number_of_BS, (count(runs.outcome_val)) as total, COUNT(CASE runs.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero
                from runs
                where runs.start::date BETWEEN SYMMETRIC %s and %s
                group by order_id) as order_details   
            """
    values0 = request.args.get('from')
    values1 = request.args.get('to')
    df = pd.read_sql_query(query, con, params = [values0, values1])
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data



#order_id in x-axis and y is the ratio of number_of_bs_ok/total number of bs for that order_id
@app.route("/order_id_days", methods=['get'])
@cross_origin()

def order_id_days():
    query = """
            select order_details.order_id, order_details.number_of_BS, round((order_details.outcome_zero::decimal / order_details.total::decimal), 2) as ratio
            from runs, (select order_id, (count(runs.station_id)) as number_of_BS, (count(runs.outcome_val)) as total, COUNT(CASE runs.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero
                from runs
                WHERE runs.start::date > (SELECT MAX(runs.start::date) FROM runs) - interval %s day
                group by order_id) as order_details
            """
    values0 = request.args.get('days')
    df = pd.read_sql_query(query, con, params = [values0])
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data

# last and first dates in databases
@app.route("/date_range", methods=['get'])
@cross_origin()

def date_range():
    query = """
                select max(runs.start::date),  min(runs.start::date)
                from runs
            """
    df = pd.read_sql_query(query, con)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data


@app.route("/gantt", methods=['get'])
@cross_origin()

def gantt():
    query = """
           SELECT distinct TO_CHAR(runs.start::date, 'Month') AS "month_manu", TO_CHAR(runs.start::date, 'YYYY') AS "year", ems

            from utbs, runs
            where utbs.utb_id = runs.utb_id and ems = 'Asteelflash'
            group by month_manu,year, utbs.ems
            """
    df = pd.read_sql_query(query, con)
    df['month_year'] = df['month_manu'] + df['year']
    df['month_year'] = df['month_year'].str.replace(' ', '')
    del df['month_manu']
    del df['year']
    print(df)
    df_list = df.values.tolist()
    json_data = jsonpify(df_list)
    return json_data



if __name__ == "__main__":
    app.run()


"""

http://127.0.0.1:5000/ptus?from=2017-02-01?to=2017-09-12

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


//with order_id and order_qty for particuler date
select distinct (runs.start::date) as date, order_id, count(runs.station_id)
from runs, stations
where stations.station_id = runs.station_id 
group by runs.order_id, runs.start
order by date asc


// in a date number of order_id stacked bar
select distinct order_id, runs.start::date as date
from runs, stations
where stations.station_id = runs.station_id and runs.start::date = '2017-03-13'
group by runs.order_id, runs.start
order by date asc

//order_id, total_number_of_basesattions, ratio of good and total base stations
select order_details.order_id, order_details.number_of_BS, round((order_details.outcome_zero::decimal / order_details.total::decimal), 2) as ratio
from (select order_id, (count(runs.station_id)) as number_of_BS, (count(runs.outcome_val)) as total, COUNT(CASE runs.outcome_val WHEN 0 THEN 1 ELSE NULL END) as outcome_zero
from runs
group by order_id) as order_details



//manufacturer and dates
select distinct ems, runs.start::date
from utbs, runs
where utbs.utb_id = runs.utb_id 
group by ems, runs.start
order by runs.start::date desc

"""