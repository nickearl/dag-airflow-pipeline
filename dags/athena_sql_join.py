"""
DAG to query Athena and upload results to S3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os
import awswrangler as wr
import boto3
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def athena_to_s3(**kwargs):
    logger = logging.getLogger("airflow.task")
    bucket = 'ne-bi-demo'
    path = 'transform/'
    filename = 'athena_results.csv'
    s3_output = f's3://{bucket}/{path}'
    try:
        query = """
            select
                a.url,
                b.lang,
                b.created_at,
                count(distinct a.user_id) as unique_users,
                sum(a.pageviews) as total_pageviews
            from analytics.fact_pageviews a
            left join analytics.dim_pages b
                on a.page_id = b.page_id
            group by 1,2,3
            order by 4 desc 
        """
        database = 'analytics'
        
        # run athena query via AWS Data Wrangler
        df = wr.athena.read_sql_query(sql=query, database=database, s3_output=s3_output)
        logger.info("Retrieved %d records from Athena.", len(df))
        
        # create a temp local csv to upload
        local_csv = f'/tmp/{filename}'
        df.to_csv(local_csv, index=False)
        
        # upload to s3
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename=local_csv, key=f'{s3_output}{filename}', bucket_name=bucket, replace=True)
        logger.info("Uploaded CSV to S3.")
        
        os.remove(local_csv)
    except Exception as e:
        logger.error("Error in Athena DAG: %s", str(e))
        raise

with DAG(
    'athena_join_query_to_csv_to_s3',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="DAG for daily Athena query and CSV upload to S3",
) as dag:
    run_task = PythonOperator(
        task_id='run_athena_query',
        python_callable=athena_to_s3,
        provide_context=True
    )
