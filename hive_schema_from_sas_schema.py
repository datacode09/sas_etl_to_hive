import pandas as pd

# Load the CSV file into a DataFrame
csv_file_path = 'schema.csv'  # Update with the actual path to your CSV file
output_file_path = 'hive_create_table_query.txt'
schema_name = 'your_schema'  # Update with the desired Hive schema name
table_name = 'your_hive_table'  # Update with the desired Hive table name

# Define the columns to partition by (update these as needed)
partition_columns = ['RPT_PRD_END_DT']  # Replace with actual partition columns

# Read the CSV schema
df = pd.read_csv(csv_file_path)

# Map SAS data types and formats to Hive data types using the Type column
def map_to_hive_type(row):
    if row['Type'] == 'Character':
        return 'STRING'  # STRING provides maximum flexibility for character data in Hive
    elif row['Type'] == 'Numeric':
        if 'DATE' in str(row['Format']).upper():
            return 'DATE'  # Map SAS date formats to Hive DATE
        else:
            return 'DOUBLE'  # Use DOUBLE for larger numeric precision

    # Default to STRING if type is unrecognized
    return 'STRING'

# Apply the mapping function to each row
df['Hive_Type'] = df.apply(map_to_hive_type, axis=1)

# Separate columns into main and partition columns
main_columns = df[~df['Name'].isin(partition_columns)]
partition_columns_df = df[df['Name'].isin(partition_columns)]

# Start building the CREATE TABLE statement with schema name
create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"

# Loop through main columns to add them to the query
for index, row in main_columns.iterrows():
    column_definition = f"    {row['Name']} {row['Hive_Type']}"
    if index < len(main_columns) - 1:
        column_definition += ","
    create_table_query += column_definition + "\n"

# Add PARTITIONED BY clause
create_table_query += ") PARTITIONED BY (\n"
for index, row in partition_columns_df.iterrows():
    partition_definition = f"    {row['Name']} {row['Hive_Type']}"
    if index < len(partition_columns_df) - 1:
        partition_definition += ","
    create_table_query += partition_definition + "\n"

# Specify row format and storage format
create_table_query += """)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY ','\nSTORED AS PARQUET;"""

# Save the query to a text file
with open(output_file_path, 'w') as file:
    file.write(create_table_query)

print(f"Hive CREATE TABLE query saved to {output_file_path}")
