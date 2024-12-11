import os
import pandas as pd
from snowflake.snowpark.session import Session
from subprocess import run, CalledProcessError

# Snowflake connection parameters
connection_parameters = {
    "account": "YM04901.ap-southeast-2",
    "user": "anveshneta",
    "password": "yourpassword",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "ONYX_DB",
    "schema": "GAME_SCHEMA"
}

# File path and dbt project path
file_path = '/Users/Anvesh/work/my_snowflake/data/Data_Engineer_Challenge_input.csv'
dbt_project_path = '/Users/Anvesh/work/my_snowflake/my_dbt_project'

def connect_to_snowflake():
    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Connected to Snowflake successfully!")
        return session
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        exit()

def ingest_data(session):
    try:
        # Read and clean the CSV file
        data = pd.read_csv(file_path, lineterminator='\n', encoding='utf-8')
        data.columns = data.columns.str.upper().str.strip()
        print("CSV file loaded successfully!")
        print(f"Cleaned column names: {data.columns}")

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS GAME_PERFORMANCE_DATA (
            BUS_DATE DATE,
            VENUE_CODE STRING,
            EGM_DESCRIPTION STRING,
            MANUFACTURER STRING,
            FP STRING,
            TURNOVER_SUM FLOAT,
            GMP_SUM FLOAT,
            GAMES_PLAYED_SUM INT
        )
        """
        session.sql(create_table_query).collect()
        print("Table 'GAME_PERFORMANCE_DATA' is ready!")

        # Write data to Snowflake
        session.write_pandas(
            df=data,
            table_name="GAME_PERFORMANCE_DATA",
            database=connection_parameters["database"],
            schema=connection_parameters["schema"],
            overwrite=False
        )
        print("Data successfully ingested into the 'GAME_PERFORMANCE_DATA' table!")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        exit()

def run_dbt_transformations():
    try:
        # Change working directory to dbt project
        os.chdir(dbt_project_path)

        # Run dbt transformations
        print("Running dbt transformations...")
        run(['dbt', 'run'], check=True)
        print("DBT transformations completed successfully!")

        # Run dbt tests
        print("Running dbt tests...")
        run(['dbt', 'test'], check=True)
        print("DBT tests completed successfully!")
    except CalledProcessError as e:
        print(f"Error during dbt transformations: {e}")
        exit()

if __name__ == "__main__":
    session = connect_to_snowflake()
    print("Starting data ingestion...")
    ingest_data(session)
    run_dbt_transformations()
    print("Pipeline executed successfully!")
