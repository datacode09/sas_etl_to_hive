import pyodbc

# Define the connection parameters based on the Hive credentials from the image
connection_string = (
    "DRIVER={Hortonworks Hive ODBC Driver};"  # Replace with your Hive ODBC driver name if different
    "HOST=enchbcclpredgd01.srv.bmogc.net;"   # Server from the image
    "PORT=8443;"
    "AuthMech=3;"                             # Authentication Mechanism (username/password)
    "UID=your_username;"                      # Replace with your actual Hive username
    "PWD=your_password;"                      # Replace with your actual password
    "HTTPPATH=/gateway/default/hive2;"        # HTTP path from the image
    "SSL=1;"                                  # Enable SSL
    "TransportMode=1;"                        # HTTP transport mode
    "ThriftTransport=2;"                      # Set ThriftTransport to SASL (2) if needed
    "Schema=sbx_hacm_arm_common_data;"        # Schema as specified in the query
)

# Establish the connection
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define your SQL query
query = """
SELECT *
FROM sbx_hacm_arm_common_data.cb_arm_models_dev_crdt_rsk_n_v_cmrccl_esr_rpt_dtl_copy
"""

# Execute the query
cursor.execute(query)

# Fetch all results
rows = cursor.fetchall()

# Specify the output file path
output_file = "hive_output.txt"

# Write the results to a text file
with open(output_file, "w") as file:
    # Write column headers
    columns = [column[0] for column in cursor.description]
    file.write("\t".join(columns) + "\n")
    
    # Write each row of data
    for row in rows:
        file.write("\t".join(str(value) if value is not None else "" for value in row) + "\n")

# Close the connection
cursor.close()
conn.close()

print(f"Data successfully written to {output_file}")
