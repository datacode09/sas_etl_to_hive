/* Step 1: Extract metadata from the SAS dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Build the Hive CREATE TABLE statement in parts and store in a dataset */
data hive_query;
    /* Define the Hive schema, table name, and partition column */
    length hive_create_query $1000; /* Adjust length as needed for larger queries */
    %let hive_schema = your_schema;           /* Replace with your Hive schema name */
    %let hive_table = your_hive_table;        /* Replace with your Hive table name */
    %let partition_column = RPT_PRD_END_DT;   /* Replace with the desired partition column */

    /* Initialize the CREATE TABLE statement */
    hive_create_query = "CREATE TABLE IF NOT EXISTS &hive_schema..&hive_table (";

    /* Loop through each column and generate Hive-compatible column definitions */
    set schema_metadata end=last;
    length hive_type $20 col_def $100;

    /* Map SAS types to Hive-compatible types */
    if type = 1 then do; /* Numeric */
        if format in ('DATE9.', 'DDMMYY10.', 'MMDDYY10.', 'YYMMDD10.') then hive_type = "DATE"; /* Map known date formats to Hive DATE */
        else if length <= 4 then hive_type = "INT";       /* Small integers */
        else if length <= 8 then hive_type = "BIGINT";    /* Large integers */
        else hive_type = "DOUBLE";                        /* Floating point */
    end;
    else if type = 2 then hive_type = "STRING"; /* Character type */

    /* Append column definition */
    if name ne "&partition_column" then do;
        col_def = catx(' ', name, hive_type);
        hive_create_query = catx(', ', hive_create_query, col_def);
    end;

    /* Finalize main column definitions if it's the last row */
    if last then do;
        hive_create_query = catx('', hive_create_query, ") PARTITIONED BY (");
        
        /* Add PARTITIONED BY clause */
        col_def = catx(' ', "&partition_column", hive_type);
        hive_create_query = catx('', hive_create_query, col_def, ")");
        
        /* Add Hive-specific table options for row format and storage */
        hive_create_query = catx('', hive_create_query, 
                                 "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET;");
        
        /* Output the final CREATE TABLE statement into the dataset */
        output;
    end;
run;

/* Step 3: View the generated Hive CREATE TABLE statement */
proc print data=hive_query noobs;
    var hive_create_query;
    title "Generated Hive CREATE TABLE Statement";
run;