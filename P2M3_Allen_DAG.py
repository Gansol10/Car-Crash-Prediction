"""
Milestone 3

Name  : Allen
Batch : FTDS-003-BSD

This program is carried out to connect our dataset and table from SQL into elasticsearch, after this step from elasticsearch the 
next is to connect it into kibana so that we can visualize our data in the form of chart so that clearly for user and reader to
get the point of our data analysis to the topic discussed.

"""

"""
Import Library
"""
import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
'''
This time we will create new server from SQL to load our data set into pandas
we will set the server like below
'''
# A. Fetch from PostgreeSQL
def fetch_data():
    #Config Database
    db_name= 'airflow'
    db_user='airflow'
    db_password= 'airflow'
    db_host= 'postgres'
    db_port='5432'

    # Connect Database
    connection= psycopg2.connect(
        database= db_name,
        user= db_user,
        password= db_password,
        host= db_host,
        port= db_port
    )
    '''
    Now we will load our created table from SQL with limit 30000 from 50000
    '''
    # Get all data
    select_query= 'SELECT* from table_m3 limit 30000;'
    df= pd.read_sql(select_query,connection)

    #Close the connection
    connection.close()

    #Save into csv
    df.to_csv('/opt/airflow/dags/P2M3_Allen_data_raw.csv', index=False)
    '''
    After loading our dataset from table we have to clean the data first to ensure the data in a good way to be analyzed it is crucial
    to cleaned our data because it will affect in the data visualization in Kibana, may the visualization not appear if we dont clean our data
    so we enter to clened data section
    '''
# B. Data Cleaning
def cleaned_text():
    df=pd.read_csv('/opt/airflow/dags/P2M3_Allen_data_raw.csv')
    df.drop(columns=['Latitude','Longitude'], inplace=True)
    df.columns=df.columns.str.strip()
    df.columns=df.columns.str.replace(" ","")
    df.columns=[x.lower() for x in df.columns]
    df.rename(columns={'weekend?':'weekend','collisiontype':'collision_type','injurytype':'injury_type','primaryfactor':'primary_factor'}, inplace=True)
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv('/opt/airflow/dags/P2M3_Allen_data_cleaned.csv', index=False)

    '''
    After doing data cleaning we will connect or input our cleaned data to elastic search, this steps done to connect our cleaned data 
    from elastic search into kibana to do next step is Data Visualization
    '''
# C. Insert into Elastic Search - with Airflow
def insert_into_elastic_manual():
    df= pd.read_csv('/opt/airflow/dags/P2M3_Allen_data_cleaned.csv',on_bad_lines='skip')#delim_whitespace=True)

    #Check Connection
    es = Elasticsearch('http://elasticsearch:9200') 
    print('connection status: ', es.ping())

    #Insert csv file to elastic search
    # failed_insert= []
    for i, r in df.iterrows():
        doc= r.to_json()
            # print(i,r['name'])
        res=es.index(index="frompostgresql",doc_type='doc', body=doc)
        print(res)
        
    '''
    Now we will set time of data withdrawal to updated and downloaded automatically in airflow,
    here we will set our withdrawal data in 6.30 everyday with delay 1 minute after we trigger dag from airflow, dont forget to 
    set the time in accordance with Indonesian time because in airflow using UTC time so UTC time in Indonesia is 7 so we have to set 7 
    our time
    '''
# D. Data Pipeline
default_args = {
    'owner': 'Allen',
    'start_date': dt.datetime(2024, 1, 22) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('P2M3_Allen_DAG.py',
         default_args=default_args,
         schedule_interval= '30 6 * * * ',
         ) as dag:

    getData = PythonOperator(task_id='QueryPostgreSQL',
                                 python_callable=fetch_data)
    
    
    cleaned_data = PythonOperator(task_id='cleaned_text',
                                 python_callable=cleaned_text)

    insertData = PythonOperator(task_id='InsertDataElasticsearch',
                                 python_callable=insert_into_elastic_manual)



getData >> cleaned_data >> insertData
