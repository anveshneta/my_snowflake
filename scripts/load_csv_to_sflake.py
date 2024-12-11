import pandas as pd
from snowflake.snowpark.session import Session

# Snowflake connection parameters
connection_parameters = {
    "account": "YM04901.ap-southeast-2",
    "user": "anveshneta",
    "password": "your_password",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "ONYX_DB",
    "schema": "GAME_SCHEMA"
}

# File path
file_path = '/Users/Anvesh/work/my_snowflake/data/Data_Engineer_Challenge_input.csv'

# Step 1: Read the CSV file
try:
    data = pd.read_csv(file_path, lineterminator='\n', encoding='utf-8')
    print("CSV file loaded successfully!")

    # Convert column names to uppercase and strip whitespace or special characters
    data.columns = data.columns.str.upper().str.strip()
    print("Cleaned column names to match Snowflake schema:", data.columns)
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# Step 2: Connect to Snowflake
try:
    session = Session.builder.configs(connection_parameters).create()
    print("Connected to Snowflake successfully!")
except Exception as e:
    print(f"Connection failed: {e}")
    exit()

# Step 3: Create the target table if it doesn't exist
table_name = "GAME_PERFORMANCE_DATA"

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

try:
    session.sql(create_table_query).collect()
    print(f"Table '{table_name}' is ready!")
except Exception as e:
    print(f"Error creating table: {e}")
    exit()

# Step 4: Write data to Snowflake
try:
    session.write_pandas(
        df=data,
        table_name=table_name,
        database=connection_parameters["database"],
        schema=connection_parameters["schema"],
        overwrite=False
    )
    print(f"Data successfully ingested into the '{table_name}' table!")
except Exception as e:
    print(f"Error ingesting data: {e}")
