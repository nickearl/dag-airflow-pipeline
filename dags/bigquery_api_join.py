"""
Sample DAG that pulls internal data from BigQuery and merges with a 3rd party API,
normalizes country data using an S3 mapping file,
joins the data, and stores the result back in BigQuery.
"""

import os, math, time, logging, requests, pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def bigquery_api_join(**kwargs):
    logger = logging.getLogger("airflow.task")
    bucket = 'ne-bi-demo'
    try:
        # parameterization via Airflow Variables
        bq_source_query = Variable.get("bq_source_query", default_var="SELECT * FROM `bi.demo.source_table`")
        bq_destination_table = Variable.get("bq_destination_table", default_var="bi.demo.destination_table")
        api_endpoint = Variable.get("api_endpoint", default_var="https://api.worldbank.org/v2/country")
        mapping_bucket = Variable.get("mapping_s3_bucket", default_var=bucket)
        mapping_key = Variable.get("mapping_s3_key", default_var="mapping_files/country_map.csv")

        # get BigQuery data
        bq_client = bigquery.Client()
        bq_df = bq_client.query(bq_source_query).to_dataframe()
        logger.info("Retrieved %d rows from BigQuery.", len(bq_df))

        # get api data with logarithmic retries
        max_attempts = 5
        api_data = None
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info("Attempt %d: Fetching API data.", attempt)
                response = requests.get(api_endpoint)
                response.raise_for_status()
                api_data = response.json()
                break
            except Exception as e:
                if attempt == max_attempts:
                    logger.error("API failed after %d attempts.", attempt)
                    raise
                delay = math.log(attempt + 1) * 2
                logger.warning("API attempt %d failed: %s. Retrying in %.2f seconds.", attempt, str(e), delay)
                time.sleep(delay)
        api_df = pd.DataFrame(api_data)
        logger.info("Retrieved %d rows from API.", len(api_df))

        # get mapping file from S3
        s3_hook = S3Hook(aws_conn_id='aws_default')
        local_mapping = '/tmp/country_map.csv'
        s3_hook.get_key(key=mapping_key, bucket_name=mapping_bucket).download_file(local_mapping)
        mapping_df = pd.read_csv(local_mapping)
        os.remove(local_mapping)
        logger.info("Mapping file loaded with %d rows.", len(mapping_df))

        # normalize country values and join to BigQuery data
        api_df = pd.merge(api_df, mapping_df, how='left', left_on='iso2Code', right_on='country_code')
        logger.info("Normalized API data.")

        joined_df = pd.merge(bq_df, api_df, how='inner', on='country_code')
        logger.info("Joined data contains %d rows.", len(joined_df))

        # push results back to BigQuery
        load_job = bq_client.load_table_from_dataframe(joined_df, bq_destination_table)
        load_job.result()
        logger.info("Uploaded joined data to BigQuery.")
    except Exception as e:
        logger.error("Error in BigQuery API join DAG: %s", str(e))
        raise

with DAG(
    'bigquery_api_join_to_bq',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="DAG for joining BigQuery and API data and writing back to BigQuery",
) as dag:
    join_task = PythonOperator(
        task_id='join_bigquery_api_data',
        python_callable=bigquery_api_join,
        provide_context=True,
    )
