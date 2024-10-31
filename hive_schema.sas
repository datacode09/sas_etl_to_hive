/* Step 1: Extract metadata from the SAS dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Build the Hive CREATE TABLE statement as a macro variable */
data _null_;
    /* Define the Hive schema, table name, and partition column */
    %let hive_schema = your_schema;           /* Replace with your Hive schema name */
    %let hive_table = your_hive_table;        /* Replace with your Hive table name */
    %let partition_column = RPT_PRD_END_DT;   /* Replace with the desired partition column */

    /* Start building the CREATE TABLE statement in a macro variable */
    call symputx('hive_create_query', 
                 "CREATE TABLE IF NOT EXISTS &hive_schema..&hive_table (");

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

    /* Prepare column definition */
    if name ne "&partition_column" then do;
        col_def = catx(' ', name, hive_type);
        call symputx('hive_create_query', catx(', ', symget('hive_create_query'), col_def));
    end;

    /* Finalize main column definitions if it's the last row */
    if last then do;
        call symputx('hive_create_query', catx('', symget('hive_create_query'), ")"));
        
        /* Add PARTITIONED BY clause for the specified partition column */
        col_def = catx(' ', "&partition_column", hive_type);
        call symputx('hive_create_query', catx(' ', symget('hive_create_query'), "PARTITIONED BY (", col_def, ")"));
        
        /* Add Hive-specific table options for row format and storage */
        call symputx('hive_create_query', catx('', symget('hive_create_query'), 
                                               "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET;"));
    end;

    /* Optional: Print log of each column's mapping */
    putlog 'NOTE: SAS Column=' name ', SAS Type=' type ', Length=' length ', Format=' format ' -> Hive Type=' hive_type;
run;

/* Step 3: Use the generated CREATE TABLE query */
%put &hive_create_query;

/* Example usage in PROC SQL */
proc sql;
    connect to hadoop (your_hive_connection_options); /* Use your connection options */
    execute (&hive_create_query) by hadoop;
    disconnect from hadoop;
quit;
