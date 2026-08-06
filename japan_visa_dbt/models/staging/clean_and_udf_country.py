import re
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import StringType

def model(dbt, session):
    dbt.config(
        materialized="table",
        imports=[
            "@JAPAN_VISA_DB.RAW.my_python_stage/pycountry-26.2.16-py3-none-any.whl",
            "@JAPAN_VISA_DB.RAW.my_python_stage/pycountry_convert-0.7.2-py3-none-any.whl"
        ]
    )

    df = session.table('JAPAN_VISA_DB.RAW.RAW_VISA_NUMBER_IN_JAPAN')

    # ---------------------------------------------------------
    # STEP 1: CLEAN THE HEADERS
    # ---------------------------------------------------------
    def clean_header(col_name):
        name = str(col_name).lower()
        name = re.sub(r'[\s\-/\.]+', '_', name)
        name = re.sub(r'[^a-z0-9_]', '', name)
        return name.strip('_')

    new_columns = [clean_header(c) for c in df.columns]
    df = df.to_df(*new_columns)

    # ---------------------------------------------------------
    # STEP 2: CORRECT COUNTRY NAMES (Via UDF)
    # ---------------------------------------------------------
    @udf(name="correct_country_name", is_permanent=False, replace=True, return_type=StringType())
    def correct_country_name(country_name: str) -> str:
        if not country_name:
            return "Unknown"
        
        clean_name = country_name.strip()
        
        country_corrections = {
            'Andra': 'Russia', 'Antigua Berbuda': 'Antigua and Barbuda', 'Barrane': 'Bahrain',
            'Brush': 'Bhutan', 'Komoro': 'Comoros', 'Benan': 'Benin', 'Kiribass': 'Kiribati',
            'Gaiana': 'Guyana', 'Court Jiboire': "Côte d'Ivoire", 'Lesot': 'Lesotho',
            'Macau travel certificate': 'Macao', 'Moldoba': 'Moldova', 'Naure': 'Nauru',
            'Nigail': 'Niger', 'Palao': 'Palau', 'St Christopher Navis': 'Saint Kitts and Nevis',
            'Santa Principa': 'Sao Tome and Principe', 'Saechel': 'Seychelles', 
            'Slinum': 'Saint Helena', 'Swaji Land': 'Eswatini', 'Torque menistan': 'Turkmenistan',
            'Tsubaru': 'Zimbabwe', 'Kosovo': 'Kosovo'
        }
        
        return country_corrections.get(clean_name, clean_name)
        
    df = df.with_column("corrected_country", correct_country_name(col("country")))

    # ---------------------------------------------------------
    # STEP 3: CONTINENT UDF (With Caching to Prevent OOM/Disk Errors)
    # ---------------------------------------------------------
    @udf(
        name="convert_country_to_continent", 
        is_permanent=False, 
        replace=True, 
        return_type=StringType(),
        imports=[
            "@JAPAN_VISA_DB.RAW.my_python_stage/pycountry-26.2.16-py3-none-any.whl",
            "@JAPAN_VISA_DB.RAW.my_python_stage/pycountry_convert-0.7.2-py3-none-any.whl"
        ]
    )
    def convert_country_to_continent(country_name: str) -> str:
        import sys
        import os
        import zipfile
        import tempfile

        # 1. Check if we have ALREADY extracted the files on this worker node
        if not hasattr(sys, '_pycountry_extracted_dir'):
            # If not, create ONE temp directory for the life of this process
            extract_dir = tempfile.mkdtemp()
            import_dir = sys._xoptions.get("snowflake_import_directory")
            
            if import_dir:
                for whl_name in [
                    'pycountry-26.2.16-py3-none-any.whl',
                    'pycountry_convert-0.7.2-py3-none-any.whl'
                ]:
                    whl_path = os.path.join(import_dir, whl_name)
                    with zipfile.ZipFile(whl_path, 'r') as z:
                        z.extractall(extract_dir)
                
                # Add the newly extracted directory to the system path
                sys.path.insert(0, extract_dir)
            
            # Save the state so future rows don't repeat the extraction!
            sys._pycountry_extracted_dir = extract_dir

        # 2. Now that the environment is guaranteed setup, run the conversion
        import pycountry_convert as pc
        
        if not country_name:
            return "Unknown"
            
        clean_name = country_name.strip()
        
        if clean_name.lower() == 'total':
            return 'All Continents'
            
        try:
            country_code = pc.country_name_to_country_alpha2(clean_name, cn_name_format="default")
            continent_code = pc.country_alpha2_to_continent_code(country_code)
            return pc.convert_continent_code_to_continent_name(continent_code)
        except Exception as e:
            return f"UNKNOWN: {e}"

    df = df.with_column("continent", convert_country_to_continent(col("corrected_country")))
    
    # ---------------------------------------------------------
    # STEP 4: FILTER AND FORMAT
    # ---------------------------------------------------------
    df = df.drop("country").with_column_renamed("corrected_country", "country")
    
    df = df.select(
        col("year"), 
        col("country"), 
        col("number_of_issued_numerical"), 
        col("continent")
    ).dropna(subset=["year", "country", "number_of_issued_numerical"])

    return df
