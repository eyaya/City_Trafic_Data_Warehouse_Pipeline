import airflow
import os,sys
import pandas as pd

from sqlalchemy import create_engine
from datetime import timedelta,datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.append(os.path.abspath("../"))


default_args = {
    "owner":"Eyaya Birara Eneyew",
    "email": ["eyayab21@gmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def load_clean_data(path, table_name):
    """task to load clean data to postgres database

    Args:
        path (str): path to clean data 
        table_name (str): database table name
    """
    "Reading the cleaned date from local storage"
    df = pd.read_csv(path, sep=",", index_col=False)
    print("writing data..............")    
    engine = create_engine("postgresql+psycopg2://eyaya:Nigielove_21@localhost:5432/open_trafic", echo=True)
    df.to_sql(table_name, con=engine, if_exists='replace',index_label='id')
    print("!!!!Done!!!!")
    
    
def clean_data(path):
    """function to clean raw data 

    Args:
        path (str): path of the data to be cleaned
    """
    # Code from tutors for reading and cleaning the raw data
    # Reading the data line by line
    filename = path + "original_uncleaned_data.csv"
    with open(filename, 'r') as file:
        lines = file.readlines()
    print('data loaded successfully!!!')
    
    #clean each line and get set of data values
    lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
    cols = lines_as_lists.pop(0)
    
    vehicles_cols = cols[:4]
    routes_cols = ['track_id'] + cols[4:]
    
    vehicles_info = []
    routes_info = []

    for row in lines_as_lists:
        track_id = row[0]

        # add the first 4 values to track_info
        vehicles_info.append(row[:4]) 

        remaining_values = row[4:]
        # reshape the list into a matrix and add track_id
        routes_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
        # add the matrix rows to trajectory_info
        routes_info = routes_info + routes_matrix
    
    df_vehicles = pd.DataFrame(data= vehicles_info,columns=vehicles_cols)
    df_routes = pd.DataFrame(data= routes_info,columns=routes_cols) 
    df_merged = pd.merge(df_vehicles, df_routes, on='track_id',how='outer')  
    
    try:
        df_merged.to_csv(path+'cleaned_data.csv',index=False)
        
    except Exception as e:
        print(f'error: {e}')
            
    


extrac_load = DAG(
    dag_id='extract_and_Load',
    default_args=default_args,
    description='This extracts the row data from source and loads the cleaned data to the postgres warehouse.',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@once'
)


task_1 = PythonOperator(
    task_id='extract_raw_data',
    python_callable=clean_data,
    op_kwargs={
        "path":"./data/"},
    dag=extrac_load
)

task2 = PostgresOperator(
    sql="sql/create_table.sql",
    task_id="create_table",
    postgres_conn_id="row_trafic",
    dag=extrac_load
)


task3 = PythonOperator(
    task_id='load_clean_data',
    python_callable=load_clean_data,
    op_kwargs={
        "path": "./data/cleaned_data.csv",
        "table_name":"trafic_data"
    },
    dag=extrac_load
)




    
task_1 >> task2 >> task3
    
    

    
