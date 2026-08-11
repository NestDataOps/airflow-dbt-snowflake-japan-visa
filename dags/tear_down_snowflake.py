from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Define default arguments for the DAG[cite: 6]
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
}

def teardown_snowflake_environment():
    print("Tearing down Snowflake database and warehouse...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Dropping the database will cascade and drop all schemas, stages, and tables within it.[cite: 6]
    teardown_queries = [
        "USE ROLE ACCOUNTADMIN;",
        "-- Drop the dedicated warehouse",
        "DROP WAREHOUSE IF EXISTS japan_visa_wh;",
        "-- Drop the database (this drops all tables, schemas, file formats, and stages inside it)",
        "DROP DATABASE IF EXISTS japan_visa_db;"
    ]
    
    for query in teardown_queries:
        # Skip comment lines during execution to avoid logging empty executions
        if not query.startswith("--"):
            hook.run(query)
            print(f"Executed: {query.strip()}")
        
    print("Snowflake environment completely torn down.")

with DAG(
    dag_id='japan_visa_snowflake_teardown',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once', # This is a one-time teardown script[cite: 6]
    catchup=False,
    description='Tears down Snowflake environment and drops tables for Japan Visa Data',
    tags=['snowflake', 'dbt', 'teardown']
) as dag:

    # Execute the teardown script[cite: 6]
    teardown_infrastructure = PythonOperator(
        task_id='drop_warehouse_and_tables',
        python_callable=teardown_snowflake_environment
    )
