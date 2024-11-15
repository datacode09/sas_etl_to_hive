import os
import pyodbc
import pandas as pd
from sas7bdat import SAS7BDAT

# Update these parameters
HIVE_DSN = 'your_hive_dsn'  # Hive ODBC DSN
HIVE_DATABASE = 'your_database'
HIVE_TABLE_NAME = 'your_table_name'
SAS_FILE_PATH = 'path_to_your_file.sas7bdat'
TEMP_CSV_PATH = 'temp_data.csv'  # Path for temporary CSV file
FILE_SIZE_THRESHOLD_MB = 100  # Threshold file size in MB for switching to LOAD DATA

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

# Step 5: Use LOAD DATA for large files
def load_data_via_file(df, table_name, database_name, temp_csv_path):
    # Export DataFrame to CSV
    df.to_csv(temp_csv_path, index=False, header=False)
    print(f"Exported data to temporary file: {temp_csv_path}")
    
    # Construct LOAD DATA query
    load_query = f"""
        LOAD DATA LOCAL INPATH '{temp_csv_path}' 
        INTO TABLE {database_name}.{table_name};
    """
    
    # Execute Hive query
    execute_hive_query(load_query)
    print("Data loaded via LOAD DATA.")

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

    # Step 4: Decide how to load data
    file_size_mb = os.path.getsize(SAS_FILE_PATH) / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f} MB")

    if file_size_mb > FILE_SIZE_THRESHOLD_MB:
        print("File size exceeds threshold. Using LOAD DATA approach...")
        load_data_via_file(dataframe, HIVE_TABLE_NAME, HIVE_DATABASE, TEMP_CSV_PATH)
    else:
        print("File size within threshold. Using row-by-row INSERT approach...")
        load_data_to_hive(dataframe, HIVE_TABLE_NAME, HIVE_DATABASE)

    # Cleanup temporary file if created
    if os.path.exists(TEMP_CSV_PATH):
        os.remove(TEMP_CSV_PATH)
        print(f"Temporary file {TEMP_CSV_PATH} removed.")

    print("Data loading completed.")
