-- models/visa1.sql
-- This creates a dbt model directly from your CSV file!

--select * from read_csv_auto('visa_number_in_japan.csv')

-- models/jvisa/visa1.sql
{{ config(materialized='table') }}

-- Point the ref to the output of your Python dbt model
SELECT * 
FROM {{ ref('clean_and_udf_country') }}
WHERE continent != 'All Continents'
