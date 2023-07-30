from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from pytrends.request import TrendReq 
from datetime import datetime

search_terms = ["vpn", "hack", "cyber", "security", "wifi"]


def get_previous_week_dates():
    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday() + 7)
    previous_week_dates = [(start_of_week + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    
    first_day = previous_week_dates[0]
    last_day = previous_week_dates[-1]
    
    # return f"{first_day} {last_day}"
    return first_day, last_day

def select_trends_data(ti, search_terms):
    #get the first and last date of the previous week
    first_day, last_day = get_previous_week_dates() 

    pytrend = TrendReq(retries = 20)#, requests_args=request_args)
    pytrend.build_payload(kw_list = search_terms, timeframe = get_previous_week_dates())
    df = pytrend.interest_by_region()
    json_str = df.to_json()

    # Push the JSON string to XCom
    ti.xcom_push(key='df_pytrends', value=json_str)

def transform_data(ti, search_terms):
    
    json_str = ti.xcom_pull(key='df_pytrends')
    df = pd.read_json(json_str)

    df.reset_index(inplace=True)
    # Melt the DataFrame to transform columns "vpn", "hack", "cyber", "security", "wifi" into rows

    df_transformed = df.melt(id_vars=['geoName'], var_name='search_term', value_name='interest')
    df_transformed = df_transformed[df_transformed['search_term'].isin(search_terms)]

    # # Add week_start and week_end columns
    # df_transformed['week_start'] = '2023-07-17'
    # df_transformed['week_end'] = '2023-07-23'
    df_transformed['week_start'], df_transformed['week_end'] = get_previous_week_dates()
    # Rename the 'geoName' column to 'country'
    df_transformed.rename(columns={'geoName': 'country'}, inplace=True)

    # Reorder the columns as required
    df_transformed = df_transformed[['country', 'week_start', 'week_end', 'search_term', 'interest']]
    df_transformed = df_transformed.to_json()

    ti.xcom_push(key='', )

with DAG("my_dag", start_date = datetime(2021,1,1),
        schedule_interval="@daily", catchup=False
) as dag:

        task_1 = PythonOperator(
            task_id = 'data_from_google_trends',
            python_callable = select_trends_data
        ),

        task_2 = 