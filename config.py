import os
from snowflake.connector import connect

# Snowflake credentials
SNOWFLAKE_USER = 'dinesh1985'
SNOWFLAKE_PASSWORD = 'Kishan1985@'
SNOWFLAKE_ACCOUNT = 'nxb36304'
SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'CC_QUICKSTART_CORTEX_DOCS'
SNOWFLAKE_SCHEMA = 'DATA'

# Snowflake connection details
def get_snowflake_connection():
    try:
        conn = connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        raise Exception(f"Error connecting to Snowflake: {e}")
