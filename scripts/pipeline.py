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

# File paths and dbt project path
full_load_file_path = '/Users/Anvesh/work/my_snowflake/data/Data_Engineer_Challenge_input.csv'
incremental_file_path = '/Users/Anvesh/work/my_snowflake/data/incremental_data.csv'
dbt_project_path = '/Users/Anvesh/work/my_snowflake/my_dbt_project'

def connect_to_snowflake():
    """
    Establish connection to Snowflake.
    """
    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Connected to Snowflake successfully!")
        return session
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        exit()

def load_data_to_snowflake(session, file_path, table_name, overwrite=False):
    """
    Load data from a CSV file to a Snowflake table.
    """
    try:
        # Read and clean the CSV file
        data = pd.read_csv(file_path, lineterminator='\n', encoding='utf-8')
        data.columns = data.columns.str.upper().str.strip()
        print(f"CSV file '{file_path}' loaded successfully!")
        print(f"Cleaned column names: {data.columns}")

        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        print(f"Table '{table_name}' is ready!")

        # Write data to Snowflake
        session.write_pandas(
            df=data,
            table_name=table_name,
            database=connection_parameters["database"],
            schema=connection_parameters["schema"],
            overwrite=overwrite
        )
        print(f"Data successfully ingested into the '{table_name}' table!")
    except Exception as e:
        print(f"Error during ingestion into {table_name}: {e}")
        exit()

def truncate_table(session, table_name):
    """
    Truncate the specified table in Snowflake.
    """
    try:
        truncate_query = f"TRUNCATE TABLE {table_name}"
        session.sql(truncate_query).collect()
        print(f"Table '{table_name}' truncated successfully!")
    except Exception as e:
        print(f"Error truncating table {table_name}: {e}")
        exit()

def run_dbt_model_sequence():
    """
    Execute dbt models in the defined sequence.
    """
    try:
        # Change working directory to dbt project
        os.chdir(dbt_project_path)

        # Define the sequence of dbt models
        models_sequence = [
            "game_performance_incremental",
            "daily_summary",
            "total_turnover_by_venue",
            "revenue_by_egm_and_venue"
        ]

        # Run each model sequentially
        for model in models_sequence:
            print(f"Running dbt model: {model}...")
            run(['dbt', 'run', '--select', model], check=True)
            print(f"Model {model} completed successfully!")
    except CalledProcessError as e:
        print(f"Error running dbt model: {e}")
        exit()

def run_dbt_tests():
    """
    Run dbt tests.
    """
    try:
        print("Running dbt tests...")
        run(['dbt', 'test'], check=True)
        print("DBT tests completed successfully!")
    except CalledProcessError as e:
        print(f"Error during dbt tests: {e}")
        exit()

if __name__ == "__main__":
    session = connect_to_snowflake()

    # Step 1: Full Load
    print("Starting full load...")
    load_data_to_snowflake(session, full_load_file_path, "GAME_PERFORMANCE_DATA", overwrite=True)
    print("Full load completed!")

    # Step 2: Incremental Load
    print("Starting incremental load...")
    load_data_to_snowflake(session, incremental_file_path, "GAME_PERFORMANCE_INCREMENTAL", overwrite=True)
    print("Incremental data loaded into staging table 'GAME_PERFORMANCE_INCREMENTAL'.")

    # Step 3: Run dbt models in sequence
    run_dbt_model_sequence()

    # Step 4: Truncate staging table
    truncate_table(session, "GAME_PERFORMANCE_INCREMENTAL")

    # Step 5: Run dbt tests
    run_dbt_tests()

    print("Pipeline executed successfully!")
