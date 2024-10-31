import pandas as pd

# Load the CSV file containing the SAS dataset schema
csv_file_path = 'schema.csv'  # Path to the CSV file with the schema information
table_name = 'your_hive_table'  # Desired Hive table name
schema_name = 'your_schema'  # Desired Hive schema name
partition_column = 'RPT_PRD_END_DT'  # Replace with the column you want to use for partitioning

# Read the schema from the CSV file
df = pd.read_csv(csv_file_path)

# Function to map SAS types to Hive types based on Type and Length
def map_to_hive_type(row):
    if row['Type'] == 'Character':
        return 'STRING'
    elif row['Type'] == 'Date':
        return 'DATE'
    elif row['Type'] == 'Numeric':
        if row['Length'] <= 4:
            return 'INT'
        elif row['Length'] <= 8:
            return 'BIGINT'
        else:
            return 'DOUBLE'
    else:
        return 'STRING'  # Default to STRING for any unmapped types

# Apply the mapping function to determine Hive types
df['Hive_Type'] = df.apply(map_to_hive_type, axis=1)

# Start building the CREATE TABLE statement
create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"

# Loop through each column, excluding the partition column from the main schema
for index, row in df.iterrows():
    if row['Name'] != partition_column:  # Exclude partition column from main schema
        create_table_query += f"    {row['Name']} {row['Hive_Type']}"
        if index < len(df) - 1:
            create_table_query += ",\n"

# Remove the trailing comma and newline if it exists
create_table_query = create_table_query.rstrip(",\n") + "\n"

# Add the PARTITIONED BY clause with the specified partition column and type
partition_type = df[df['Name'] == partition_column]['Hive_Type'].values[0]
create_table_query += f")\nPARTITIONED BY (\n    {partition_column} {partition_type}\n)"

# Specify row format and storage format with field terminator '\001'
create_table_query += "\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '\\001'\nSTORED AS PARQUET;"

# Print the full CREATE TABLE statement
print("Generated Hive CREATE TABLE Statement:\n")
print(create_table_query)

# Save the query to a text file
with open(output_file_path, 'w') as file:
    file.write(create_table_query)
