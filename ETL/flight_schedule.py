from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from datetime import datetime 
import datetime as dt
import pandas as pd
import numpy as np
import requests

def get_data():
    RUN_API = False  
    if not RUN_API:
        raise AirflowSkipException("API extraction disabled")

    dep_url = "https://api.aviationstack.com/v1/timetable?iataCode=BOM&type=departure&access_key=5397c300ad75f77cf633c5a1dfa2859c"
    dep_data = requests.get(dep_url)
    dep_df = pd.json_normalize(dep_data.json()['data'])
    dep_df.to_csv("/home/akshata/flight_data/dep_flights.csv", index=False)

    arr_url = "https://api.aviationstack.com/v1/timetable?iataCode=BOM&type=arrival&access_key=5397c300ad75f77cf633c5a1dfa2859c"
    arr_data = requests.get(arr_url)
    arr_df = pd.json_normalize(arr_data.json()['data'])
    arr_df.to_csv("/home/akshata/flight_data/arr_flights.csv", index=False)
    
    
def clean_data():
    dep_df = pd.read_csv("/home/akshata/flight_data/dep_flights.csv")
    arr_df = pd.read_csv("/home/akshata/flight_data/arr_flights.csv")

    columns = ['Unnamed: 0', 'airline.icaoCode', 'flight.icaoNumber', 'flight.number', 'arrival.actualRunway', 'arrival.estimatedRunway', 'arrival.icaoCode', 'departure.actualRunway', 'departure.estimatedRunway', 'departure.icaoCode', 'codeshared', 'codeshared.airline.iataCode', 'codeshared.airline.icaoCode','codeshared.airline.name', 'codeshared.flight.iataNumber', 'codeshared.flight.icaoNumber', 'codeshared.flight.number', 'arrival.baggage', 'departure.baggage', 'arrival.gate', 'departure.gate', 'arrival.terminal', 'departure.terminal']

    dep_df.drop(columns=columns, inplace=True)
    arr_df.drop(columns=columns, inplace=True)

    dep_df.to_csv("/home/akshata/flight_data/dep_filtered.csv", index=False)
    arr_df.to_csv("/home/akshata/flight_data/arr_filtered.csv", index=False)



def merge_transform_data():
    dep_df = pd.read_csv("/home/akshata/flight_data/dep_filtered.csv")
    arr_df = pd.read_csv("/home/akshata/flight_data/arr_filtered.csv")
    df = pd.concat([dep_df, arr_df], ignore_index=True)

    dt_cols = ['arrival.actualTime','arrival.estimatedTime','arrival.scheduledTime', 'departure.actualTime','departure.estimatedTime','departure.scheduledTime']

    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df.to_csv("/home/akshata/flight_data/all_flights.csv", index=False)


def airport_merge():
    df = pd.read_csv("/home/akshata/flight_data/all_flights.csv")
    url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
    cols = ['AirportID','Name','City','Country','IATA','ICAO','Latitude','Longitude', 'Altitude','Timezone','DST','TzDatabaseTimeZone','Type','Source']

    airports_df = pd.read_csv(url, header=None, names=cols, na_values="\\N", index_col=False)
    airports_df = airports_df[['IATA','Country']].dropna(subset=['IATA'])

    df = df.merge(airports_df.rename(columns={'IATA':'departure.iataCode','Country':'orig_country'}), on='departure.iataCode', how='left')

    df = df.merge(airports_df.rename(columns={'IATA':'arrival.iataCode','Country':'dest_country'}), on='arrival.iataCode', how='left')

    df.to_csv("/home/akshata/flight_data/flights_airports.csv", index=False)



def add_columns():
    dt_cols = ['arrival.actualTime','arrival.estimatedTime','arrival.scheduledTime', 'departure.actualTime','departure.estimatedTime','departure.scheduledTime']
    df = pd.read_csv("/home/akshata/flight_data/flights_airports.csv", parse_dates=dt_cols)

    df['departure_hour'] = df['departure.scheduledTime'].dt.hour
    df['arrival_hour'] = df['arrival.scheduledTime'].dt.hour
    df['departure_delay_flag'] = df['departure.delay'] > 0
    df['arrival_delay_flag'] = df['arrival.delay'] > 0
    df['total_delay'] = df['departure.delay'].fillna(0) + df['arrival.delay'].fillna(0)
    df['flight_type'] = np.where(df['orig_country'] == df['dest_country'], 'Domestic','International')
    df['flight_duration'] = (df['arrival.actualTime'] - df['departure.actualTime']).dt.total_seconds()/60

    df.to_csv("/home/akshata/flight_data/final_flights.csv", index=False)


def flag_missing():
    dt_cols = ['arrival.actualTime','arrival.estimatedTime','arrival.scheduledTime', 'departure.actualTime','departure.estimatedTime','departure.scheduledTime']
    
    df = pd.read_csv("/home/akshata/flight_data/final_flights.csv", parse_dates=dt_cols)

    for col in ['arrival.delay','departure.delay','arrival.actualTime','departure.actualTime', 'arrival.estimatedTime','departure.estimatedTime','flight_duration','total_delay']:
        df[f'{col}_missing'] = df[col].isna()

    df.to_csv("/home/akshata/flight_data/final_flights_flagged.csv", index=False)
    #df.to_parquet("/home/akshata/flight_data/final_flights_flagged.parquet", index=False)



default_args = {'owner': 'akshata','start_date': dt.datetime(2024, 10, 10), 'retries': 1, 'retry_delay': dt.timedelta(minutes=5)}

with DAG(dag_id='flight_ETL_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 9, 1)
) as dag:

    t0 = PythonOperator(task_id="get_data", python_callable=get_data)
    t1 = PythonOperator(task_id="clean_data", python_callable=clean_data, trigger_rule=TriggerRule.ALL_DONE)
    t2 = PythonOperator(task_id="merge_transform", python_callable=merge_transform_data)
    t3 = PythonOperator(task_id="airport_merge", python_callable=airport_merge)
    t4 = PythonOperator(task_id="add_columns", python_callable=add_columns)
    t5 = PythonOperator(task_id="flag_missing", python_callable=flag_missing)

t1 >> t2 >> t3 >> t4 >> t5
