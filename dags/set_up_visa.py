from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import plotly.express as px

# Define default arguments for the DAG[cite: 5]
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
}

def setup_snowflake_environment():
    print("Setting up Snowflake database, schemas, and warehouse...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    setup_queries = [
        "USE ROLE ACCOUNTADMIN;",
        "CREATE WAREHOUSE IF NOT EXISTS JAPAN_VISA_WH WAREHOUSE_SIZE = XSMALL;",
        "CREATE DATABASE IF NOT EXISTS JAPAN_VISA_DB;",
        "CREATE SCHEMA IF NOT EXISTS JAPAN_VISA_DB.RAW;",
        "CREATE SCHEMA IF NOT EXISTS JAPAN_VISA_DB.CLEANED;",
        "ALTER SCHEMA JAPAN_VISA_DB.RAW SET LOG_LEVEL = 'INFO';",
        "ALTER SCHEMA JAPAN_VISA_DB.RAW SET TRACE_LEVEL = 'ALWAYS';",
        "ALTER SCHEMA JAPAN_VISA_DB.RAW SET METRIC_LEVEL = 'ALL';"
    ]
    
    for query in setup_queries:
        hook.run(query)
        print(f"Executed: {query.strip()}")
        
    print("Snowflake infrastructure setup successfully.")

def setup_snowflake_stages():
    print("Setting up Snowflake file format and stages...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    stage_queries = [
        """
        CREATE OR REPLACE FILE FORMAT JAPAN_VISA_DB.RAW.csv_ff 
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"';
        """,
        """
        CREATE OR REPLACE STAGE JAPAN_VISA_DB.RAW.s3_visa_raw
            COMMENT = 'Stage for Grade 10 S3 Files'
            url = 's3://nestdataops-822872386722-us-east-1-an/seeds/'
            file_format = JAPAN_VISA_DB.RAW.csv_ff;
        """,
        """
        CREATE OR REPLACE STAGE JAPAN_VISA_DB.RAW.MY_PYTHON_STAGE
            COMMENT = 'Internal stage for Python packages like pycountry';
        """
    ]
    
    for query in stage_queries:
        hook.run(query)
        print(f"Executed stage query successfully.")

def upload_python_wheels():
    print("Uploading local wheel files to the internal Python stage...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # AUTO_COMPRESS=FALSE is crucial so Snowflake doesn't gzip the .whl files![cite: 5]
    upload_query = """
    PUT file:///opt/airflow/dags/wheels/pycountry*.whl @JAPAN_VISA_DB.RAW.MY_PYTHON_STAGE/ 
    AUTO_COMPRESS=FALSE 
    OVERWRITE=TRUE;
    """
    
    hook.run(upload_query)
    print("Python wheels uploaded successfully.")

def create_and_load_tables():
    print("Creating tables and loading S3 data into Snowflake...")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    table_queries = [
        """
        CREATE OR REPLACE TABLE JAPAN_VISA_DB.RAW.RAW_VISA_NUMBER_IN_JAPAN (
            year VARCHAR,
            country VARCHAR,
            number_of_issued_numerical VARCHAR,
            continent VARCHAR
        ) COMMENT = 'Raw ingested CSV data for Japan Visas';
        """,
        "USE WAREHOUSE JAPAN_VISA_WH;",
        """
        COPY INTO JAPAN_VISA_DB.RAW.RAW_VISA_NUMBER_IN_JAPAN 
        FROM @JAPAN_VISA_DB.RAW.s3_visa_raw/
        FILES = ('visa_number_in_japan.csv');
        """
    ]
    
    for query in table_queries:
        hook.run(query)
        print(f"Executed table/load query successfully.")

def generate_and_save_map():
    # 1. Connect to Snowflake and extract data[cite: 5]
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    query = "SELECT * FROM JAPAN_VISA_DB.CLEANED.VISA_YEAR_COUNTRY"
    df = snowflake_hook.get_pandas_df(query)
    
    # 2. Force all column headers to lowercase and fix data types[cite: 5]
    df.columns = df.columns.str.lower()
    df['number_of_issued_numerical'] = pd.to_numeric(df['number_of_issued_numerical'], errors='coerce')
    
    # 3. NEW: Rename the column to exactly what you want to see on the map![cite: 5]
    df = df.rename(columns={'number_of_issued_numerical': 'Visas Issued'})
    
    # 4. Generate the map[cite: 5]
    df = df.sort_values('year')
    fig = px.choropleth(
        df,
        locations="country",
        locationmode="country names",
        color="Visas Issued",             
        hover_name="country",
        range_color=[100000, 100000],
        hover_data={"Visas Issued": ":,.0f", "country": False}, 
        animation_frame="year",
        color_continuous_scale=px.colors.sequential.Plasma,
        title="Yearly Visas Issued by Country"
    )
    
    # 5. Save and upload (keeping your S3 Hook logic here)[cite: 5]
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

    # Python Tasks replacing SnowflakeOperators
    setup_infra = PythonOperator(
        task_id='setup_infrastructure',
        python_callable=setup_snowflake_environment
    )

    setup_stage = PythonOperator(
        task_id='setup_stages',
        python_callable=setup_snowflake_stages
    )

    upload_packages = PythonOperator(
        task_id='upload_python_packages',
        python_callable=upload_python_wheels
    )
    
    create_load_tables = PythonOperator(
        task_id='create_and_load_tables',
        python_callable=create_and_load_tables
    )

    # dbt Tasks[cite: 5]
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

    # Clean, linear dependency chain
    setup_infra >> setup_stage >> upload_packages >> create_load_tables >> test_sources >> run_dbt_models >> test_dbt_models >> generate_map_task
