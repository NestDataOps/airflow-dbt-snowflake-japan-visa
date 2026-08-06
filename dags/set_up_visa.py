from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import plotly.express as px

# Define default arguments for the DAG[cite: 8]
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
}

# The SQL statements are derived from the requested setup[cite: 8]
# Grouped logically for execution and monitoring[cite: 8]

SQL_SETUP_INFRASTRUCTURE = """
USE ROLE ACCOUNTADMIN;

-- Create an X-Small Warehouse
CREATE WAREHOUSE IF NOT EXISTS JAPAN_VISA_WH WAREHOUSE_SIZE = XSMALL;

-- Create Database & Schemas
CREATE DATABASE IF NOT EXISTS JAPAN_VISA_DB;
CREATE SCHEMA IF NOT EXISTS JAPAN_VISA_DB.RAW;
CREATE SCHEMA IF NOT EXISTS JAPAN_VISA_DB.CLEANED;

-- Enable logging, tracing, and metrics
ALTER SCHEMA JAPAN_VISA_DB.RAW SET LOG_LEVEL = 'INFO';
ALTER SCHEMA JAPAN_VISA_DB.RAW SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA JAPAN_VISA_DB.RAW SET METRIC_LEVEL = 'ALL';
"""

SQL_SETUP_STAGE = """
-- Set up File format and external stage[cite: 8]
CREATE OR REPLACE FILE FORMAT JAPAN_VISA_DB.RAW.csv_ff 
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE JAPAN_VISA_DB.RAW.s3_visa_raw
    COMMENT = 'Stage for Grade 10 S3 Files'
    url = 's3://nestdataops-822872386722-us-east-1-an/seeds/'
    file_format = JAPAN_VISA_DB.RAW.csv_ff;

-- Set up internal stage for Python UDF dependencies
CREATE OR REPLACE STAGE JAPAN_VISA_DB.RAW.MY_PYTHON_STAGE
    COMMENT = 'Internal stage for Python packages like pycountry';
"""

SQL_UPLOAD_WHEELS = """
    /* Upload local wheel files to the internal Python stage */
    /* AUTO_COMPRESS=FALSE is crucial so Snowflake doesn't gzip the .whl files! */
    PUT file:///opt/airflow/dags/wheels/pycountry*.whl @JAPAN_VISA_DB.RAW.MY_PYTHON_STAGE/ 
    AUTO_COMPRESS=FALSE 
    OVERWRITE=TRUE;
    """

# Dictionary to hold the table creation and copy statements[cite: 8]
TABLES = {
    "visa_number_in_japan": {
        "create": """
            CREATE OR REPLACE TABLE JAPAN_VISA_DB.RAW.RAW_VISA_NUMBER_IN_JAPAN (
                year VARCHAR,
                country VARCHAR,
                number_of_issued_numerical VARCHAR,
                continent VARCHAR
            ) COMMENT = 'Raw ingested CSV data for Japan Visas';
        """,
        "load": """
            USE WAREHOUSE JAPAN_VISA_WH;

            COPY INTO JAPAN_VISA_DB.RAW.RAW_VISA_NUMBER_IN_JAPAN 
            FROM @JAPAN_VISA_DB.RAW.s3_visa_raw/
            FILES = ('visa_number_in_japan.csv');
        """
    }
}


def generate_and_save_map():
    # 1. Connect to Snowflake and extract data
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    query = "SELECT * FROM JAPAN_VISA_DB.CLEANED.VISA_YEAR_COUNTRY"
    df = snowflake_hook.get_pandas_df(query)
    
    # 2. Force all column headers to lowercase and fix data types
    df.columns = df.columns.str.lower()
    df['number_of_issued_numerical'] = pd.to_numeric(df['number_of_issued_numerical'], errors='coerce')
    
    # 3. NEW: Rename the column to exactly what you want to see on the map!
    df = df.rename(columns={'number_of_issued_numerical': 'Visas Issued'})
    
    # 4. Generate the map
    df = df.sort_values('year')
    fig = px.choropleth(
        df,
        locations="country",
        locationmode="country names",
        color="Visas Issued",             # <--- Now uses the clean name
        hover_name="country",
        range_color=[100000, 100000],
        hover_data={"Visas Issued": ":,.0f", "country": False}, # <--- Formats the clean name with commas
        animation_frame="year",
        color_continuous_scale=px.colors.sequential.Plasma,
        title="Yearly Visas Issued by Country"
    )
    
    # 5. Save and upload (keeping your S3 Hook logic here)
    output_path = "/opt/airflow/dags/japan_visas_animated.html"
    fig.write_html(output_path)
    
    s3_hook = S3Hook(
        aws_conn_id='aws_default',
        extra_args={'ContentType': 'text/html'} 
    )
    s3_hook.load_file(
        filename=output_path,
        key='output/japan_visas_animated.html', 
        bucket_name='nestdataops-822872386722-us-east-1-an', 
        replace=True
    )
    print(f"✅ Map generated and uploaded to S3 successfully!")

with DAG(
    dag_id='japan_visa_snowflake_setup',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once', 
    catchup=False,
    description='Sets up Snowflake environment and loads Japan Visa S3 data',
    tags=['snowflake', 'setup', 's3']
) as dag:

    # 1. Setup Infra (Warehouse, DB, Schemas)[cite: 8]
    setup_infra = SnowflakeOperator(
        task_id='setup_infrastructure',
        snowflake_conn_id='snowflake_default',
        sql=SQL_SETUP_INFRASTRUCTURE,
        split_statements=True
    )

    # 2. Setup Stage and File Format[cite: 8]
    setup_stage = SnowflakeOperator(
        task_id='setup_stage',
        snowflake_conn_id='snowflake_default',
        sql=SQL_SETUP_STAGE,
        split_statements=True
    )


    upload_python_packages = SnowflakeOperator(
        task_id='upload_python_packages',
        snowflake_conn_id='snowflake_default',
        sql=SQL_UPLOAD_WHEELS,
        split_statements=True
    )

    # Optional dbt tasks (kept from your original structure for downstream processing)[cite: 8]
    test_sources = BashOperator(
        task_id='test_sources',
        bash_command=f"""
        cd /opt/airflow/japan_visa_dbt && \
        dbt source freshness --profiles-dir . && \
        dbt test --select "source:*" --profiles-dir .
        """
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
        cd /opt/airflow/japan_visa_dbt && \
        dbt run --profiles-dir .
        """
    )

    test_dbt_models = BashOperator(
        task_id='test_dbt_models',
        bash_command=f"""
        cd /opt/airflow/japan_visa_dbt && \
        dbt test --exclude "source:*" --profiles-dir .
        """
    )

    generate_map_task = PythonOperator(
        task_id='generate_animated_map',
        python_callable=generate_and_save_map
    )


    # Make sure stage setup happens after infra setup[cite: 8]
    setup_infra >> setup_stage 

    # 3 & 4. Dynamically create tasks for the table[cite: 8]
    for table_name, queries in TABLES.items():
        
        create_table_task = SnowflakeOperator(
            task_id=f'create_table_{table_name}',
            snowflake_conn_id='snowflake_default',
            sql=queries['create'],
            split_statements=True
        )

        load_data_task = SnowflakeOperator(
            task_id=f'load_data_{table_name}',
            snowflake_conn_id='snowflake_default',
            sql=queries['load'],
            split_statements=True
        )

        # Set dependencies: 
        # Stage must exist -> Table must be created -> Data is loaded -> downstream dbt tasks run[cite: 8]
        setup_stage >> upload_python_packages >> create_table_task >> load_data_task >> test_sources >> run_dbt_models >> test_dbt_models >> generate_map_task 
