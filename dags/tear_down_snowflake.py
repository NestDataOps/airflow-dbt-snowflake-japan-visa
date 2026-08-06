from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
}

# Dropping the database will cascade and drop all schemas, stages, and tables within it.
SQL_TEARDOWN = """
USE ROLE ACCOUNTADMIN;

-- Drop the dedicated warehouse
DROP WAREHOUSE IF EXISTS japan_visa_wh;

-- Drop the database (this drops all tables, schemas, file formats, and stages inside it)
DROP DATABASE IF EXISTS japan_visa_db;
"""

with DAG(
    dag_id='japan_visa_snowflake_teardown',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once', # This is a one-time teardown script
    catchup=False,
    description='Tears down Snowflake environment and drops tables for Japan Visa Data',
    tags=['snowflake', 'dbt', 'teardown']
) as dag:

    # Execute the teardown script
    teardown_infrastructure = SnowflakeOperator(
        task_id='drop_warehouse_and_tables',
        snowflake_conn_id='snowflake_default',
        sql=SQL_TEARDOWN,
        split_statements=True
    )
