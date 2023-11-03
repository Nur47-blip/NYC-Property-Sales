'''
=================================================
Milestone 3

Nama  : Muhammad Nur Alamsyah
Batch : FTDS-023-RMT

Program ini dibuat untuk melakukan load data dari file CSV ke PostgreSQL (menggunakan pgAdmin4),
kemudian insert data tersebut menggunakan DAG. Adapun dataset yang dipakai adalah dataset NYC Property Sales
dari tahun 2016 hingga 2017
=================================================
'''

import pandas as pd
import numpy as np
import psycopg2 as db
import warnings
from elasticsearch import Elasticsearch, helpers

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get_data_from_postgresql():
    '''
    This function fetches data from a PostgreSQL database, which can then be used for data cleaning purposes.

    Parameters:
    - dbname (str): The name of the database where data is stored.
    - host (str): The location/address of the PostgreSQL server.
    - user (str): The username used for accessing the database.
    - password (str): The password associated with the given user.
    - table (str): The name of the table containing the desired data.

    Returns:
    - data (Pandas DataFrame): A pandas DataFrame containing the queried data.

    Example usage:
    data = get_data_from_postgresql('gc7', 'localhost', 'postgres', 'password', 'table_gc7')
    '''

    dbname = 'm3'
    host = 'localhost'
    user = 'postgres'
    password = 'password'
    table = 'table_m3'

    # Construct the connection string for PostgreSQL using the provided variables
    conn_string = f"dbname={dbname} host={host} user={user} password={password}"

    # Establish a connection to the PostgreSQL database using the constructed connection string
    conn = db.connect(conn_string)

    # Query the database using the provided table name and store the result in a pandas DataFrame
    data = pd.read_sql(f"select * from {table}", conn)

    # Save the raw data
    data.to_csv('P2M3_nur_alamsyah_data_raw.csv', index=False)

    # Return the DataFrame containing the queried data
    return data


def clean_data():
    '''
    This function cleans the input data by handling missing values, removing duplicates, 
    and standardizing column names. Additionally, specified columns are converted to pandas datetime format.

    Parameters:
    - df (Pandas DataFrame): The input DataFrame to be processed.
    - date_cols (list of str): Columns within the DataFrame to be converted into pandas datetime format.
    - drop_col_index (int): Index of Column within the DataFrame to be dropped.
    - num_cols (list of str): Columns within the DataFrame to be converted into int64.

    Returns:
    - data (Pandas DataFrame): The processed and cleaned DataFrame.

    Example usage:
    cleaned_data = clean_data(data, ['Order Date', 'Ship Date'], 0, ['Price'])
    '''

    df = pd.read_csv('P2M3_nur_alamsyah_data_raw.csv')
    date_cols = ['SALE DATE']
    drop_col_index = 0
    num_cols = ['SALE PRICE']

    # Drop Unused Column
    df.drop(df.columns[drop_col_index], axis=1, inplace=True)

    # Handle `-` as null value
    for col in df.columns:
        df[col].replace(' -  ', np.nan, inplace=True)

    # Erase trailing spaces
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Drop rows with missing values
    df = df.dropna()

    # Remove any duplicate rows
    df = df.drop_duplicates()

    # Convert specified columns to pandas datetime format
    df[date_cols] = df[date_cols].apply(pd.to_datetime)

    # Convert specified columns to int64
    df[num_cols] = df[num_cols].apply(lambda x: x.astype('int64'))

    # Standardize column names by converting to lowercase and replacing spaces and dashes with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    # Save clean data
    df.to_csv('P2M3_nur_alamsyah_data_clean.csv', index=False)

    # Return the cleaned and processed dataframe
    return df

def insert_into_es():
    '''
    This function inserts the provided DataFrame into an ElasticSearch instance at the specified URL.

    Parameters:
    - url (str): Endpoint URL of the ElasticSearch instance.
    - df (Pandas DataFrame): DataFrame to be inserted into ElasticSearch.
    - index_name (str): Name of the target index in ElasticSearch.

    Returns:
    - This function has no return value

    Example usage:
    insert_into_es('http://localhost:9200', cleaned_data, 'superstore')
    '''

    url = 'http://localhost:9200'
    df = pd.read_csv('P2M3_nur_alamsyah_data_clean.csv')
    index_name = 'nyc-sales-final'

    # Initialize the ElasticSearch instance
    es = Elasticsearch(url)

    # Display the connectivity status of the ElasticSearch instance
    print(es.ping())

    # Process each row from the DataFrame for insertion
    for _, row in df.iterrows():
        # Transform the row to a dictionary format
        doc = row.to_dict()
        # Insert the transformed row (document) into the specified Elasticsearch index
        es.index(index=index_name, body=doc)

default_args = {
    'owner': 'alam',
    'start_date': dt.datetime(2023, 10, 24, 10, 27, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
}

with DAG('DAGFlow',
         default_args=default_args,
         schedule_interval='30 23 * * *' # 23.30 UTC = 6.30 WIB
         ) as dag:

    importData = PythonOperator(task_id='import',
                                 python_callable=get_data_from_postgresql)

    cleanData = PythonOperator(task_id='clean',
                                 python_callable=clean_data)
    
    insertData = PythonOperator(task_id='insert',
                                 python_callable=insert_into_es)

importData >> cleanData >> insertData