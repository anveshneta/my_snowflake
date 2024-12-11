from snowflake.snowpark.session import Session

connection_parameters = {
    "account": "re18885.ap-southeast-2.aws",
    "user": "anveshneta",        # Replace with your Snowflake username
    "password": "Hugesnow143$",    # Replace with your Snowflake password
    "role": "ACCOUNTADMIN",         # Replace with the correct role
    "warehouse": "COMPUTE_WH",      # Replace with your active warehouse
    "database": "ONYX_DB",          # Replace with your database
    "schema": "GAME_SCHEMA"         # Replace with your schema
}

try:
    session = Session.builder.configs(connection_parameters).create()
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
