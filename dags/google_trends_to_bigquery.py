from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from pytrends.request import TrendReq 
from google.cloud import bigquery
from datetime import datetime, timedelta

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
    pytrend.build_payload(kw_list = search_terms, timeframe = f"{first_day} {last_day}")
    df = pytrend.interest_by_region()
    json_str = df.to_json()

    # Push the JSON string to XCom
    ti.xcom_push(key='df_pytrends', value=json_str)

def transform_data(ti, search_terms):
    
    json_str = ti.xcom_pull(key='df_pytrends')
    df = pd.read_json(json_str)
    print(df.head(10))
    df.reset_index(inplace=True)
    # Melt the DataFrame to transform columns "vpn", "hack", "cyber", "security", "wifi" into rows
    print(df.head(10))
    df_transformed = df.melt(id_vars=['index'], var_name='search_term', value_name='interest')
    df_transformed = df_transformed[df_transformed['search_term'].isin(search_terms)]

    # # Add week_start and week_end columns
    # df_transformed['week_start'] = '2023-07-17'
    # df_transformed['week_end'] = '2023-07-23'
    df_transformed['week_start'], df_transformed['week_end'] = get_previous_week_dates()
    # Rename the 'geoName' column to 'country'
    df_transformed.rename(columns={'index': 'country'}, inplace=True)

    # Reorder the columns as required
    df_transformed = df_transformed[['country', 'week_start', 'week_end', 'search_term', 'interest']]
    df_transformed = df_transformed.to_json()

    ti.xcom_push(key='df_transformed', value=df_transformed)

def filtering_countries_with_same_interests(ti):

    json_str = ti.xcom_pull(key='df_transformed')
    df = pd.read_json(json_str)
    grouped = df.groupby(['country', 'interest'])['search_term'].nunique().reset_index()
    # Filtering out the countries where all search_terms have 0 interest
    filtered_countries = grouped[grouped['search_term'] < 5]
    
    # Moving those countries to list
    countries_with_same_value = filtered_countries['country'].tolist()
    
    # Filtering the dataframe based on the countries in the list
    countries_filtered = df[df['country'].isin(countries_with_same_value)]
    countries_filtered = countries_filtered.to_json()
    ti.xcom_push(key='df_countries_filtered', value=countries_filtered)

"""main function for calculating ranking"""
def rank_search_terms(ti):

    json_str = ti.xcom_pull(key='df_countries_filtered')
    df = pd.read_json(json_str)
    # Apply special case for search_term 'vpn': give it a low numeric value for sorting
    df['sort_priority'] = df.apply(lambda x: 0 if x['search_term'] == 'vpn' else 1, axis=1)

    # Sort dataframe based on 'country', 'week_start', 'value', 'sort_priority', and 'search_term'
    df = df.sort_values(['country', 'week_start', 'interest', 'sort_priority', 'search_term'], ascending=[True, True, False, False, True])

    # Apply ranking within each 'country' and 'week_start' group and add it as a new column 'ranking'
    df['ranking'] = df.groupby(['country', 'week_start'])['interest'].rank(method='first', ascending=False)
    df['ranking'] = df['ranking'].astype(int)

    # Drop the auxiliary column 'sort_priority'
    df = df.drop(columns=['sort_priority'])
    df = df.to_json()
    ti.xcom_push(key='df_ranking', value=df)


def write_to_bigquery(ti, project_id, dataset_id, table_id):
    
    json_str = ti.xcom_pull(key='df_ranking')
    df = pd.read_json(json_str)
    
    client = bigquery.Client()

    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)
    # Check if the table exists
    try:
        client.get_table(table)
        print("Table already exists")
    except Exception as e:
        print("Table does not exist")
        schema = [
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("week_start", "STRING"),
            bigquery.SchemaField("week_end", "STRING"),
            bigquery.SchemaField("search_term", "STRING"),
            bigquery.SchemaField("interest", "INTEGER"),
            bigquery.SchemaField("ranking", "INTEGER")
        ]
        table = bigquery.Table(table, schema=schema)
        table = client.create_table(table)
        
    #for some reason to_gbq() does not work, get error "pyarrow needs to be installed" even though pyarrow is already here
    # df.to_gbq(table, 
    #           project_id=project_id)
    job_config = bigquery.LoadJobConfig(
       write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Choose write disposition according to your needs
    )

    job = client.load_table_from_dataframe(
        df, table, job_config=job_config
    )
    
    job.result()
    print("Data appended to table")


with DAG("my_dag", start_date = datetime(2021,1,1),
        schedule_interval="@daily", catchup=False
) as dag:

        data_from_google_trends = PythonOperator(
            task_id = 'data_from_google_trends',
            python_callable = select_trends_data,
            op_kwargs = {'search_terms':search_terms}
        )

        transform_data = PythonOperator(
            task_id = 'transform_data',
            python_callable = transform_data,
            op_kwargs = {'search_terms':search_terms}

        )

        remove_countries_without_interest = PythonOperator(
            task_id = 'remove_countries_without_interest',
            python_callable = filtering_countries_with_same_interests
        )

        calculate_rankings = PythonOperator(
            task_id = 'calculate_rankings',
            python_callable = rank_search_terms
        )

        data_from_google_trends >> transform_data >> remove_countries_without_interest >> calculate_rankings
        # ,

        # task_5 = PythonOperator(
        #     task_id = 'write_to_bigquery_table',
        #     python_callable = select_trends_data
        # ),