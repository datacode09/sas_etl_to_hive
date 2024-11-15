import pyodbc
import pandas as pd
from sas7bdat import SAS7BDAT

# Update these parameters
HIVE_DSN = 'your_hive_dsn'  # Hive ODBC DSN
HIVE_DATABASE = 'your_database'
HIVE_TABLE_NAME = 'your_table_name'
SAS_FILE_PATH = 'path_to_your_file.sas7bdat'

# Step 1: Read SAS dataset
def read_sas_dataset(file_path):
    with SAS7BDAT(file_path) as file:
        df = file.to_data_frame()
    return df

# Step 2: Generate Hive DDL
def generate_hive_ddl(df, table_name, database_name):
    ddl = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (\n"
    for col, dtype in zip(df.columns, df.dtypes):
        hive_type = map_dtype_to_hive(dtype)
        ddl += f"  `{col}` {hive_type},\n"
    ddl = ddl.rstrip(',\n') + "\n) STORED AS PARQUET;"
    return ddl

def map_dtype_to_hive(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'STRING'

# Step 3: Create table in Hive
def execute_hive_query(query):
    conn = pyodbc.connect(f"DSN={HIVE_DSN}")
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.commit()
    cursor.close()
    conn.close()

# Step 4: Load data into Hive
def load_data_to_hive(df, table_name, database_name):
    conn = pyodbc.connect(f"DSN={HIVE_DSN}")
    cursor = conn.cursor()
    for _, row in df.iterrows():
        values = ', '.join([format_value(value) for value in row])
        query = f"INSERT INTO TABLE {database_name}.{table_name} VALUES ({values});"
        cursor.execute(query)
    cursor.commit()
    cursor.close()
    conn.close()

def format_value(value):
    if pd.isna(value):
        return 'NULL'
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        return str(value)

# Main script
if __name__ == "__main__":
    # Step 1: Load SAS dataset
    print("Reading SAS dataset...")
    dataframe = read_sas_dataset(SAS_FILE_PATH)

    # Step 2: Generate DDL
    print("Generating Hive DDL...")
    ddl_query = generate_hive_ddl(dataframe, HIVE_TABLE_NAME, HIVE_DATABASE)
    print(f"Hive DDL:\n{ddl_query}")

    # Step 3: Create Hive table
    print("Creating Hive table...")
    execute_hive_query(ddl_query)

    # Step 4: Load data into Hive
    print("Loading data into Hive...")
    load_data_to_hive(dataframe, HIVE_TABLE_NAME, HIVE_DATABASE)
    print("Data loaded successfully.")
