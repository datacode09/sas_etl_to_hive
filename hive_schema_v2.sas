/* Step 1: Extract metadata from the SAS dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Build the Hive CREATE TABLE statement in parts */
data _null_;
    /* Define the Hive schema, table name, and partition column */
    %let hive_schema = your_schema;           /* Replace with your Hive schema name */
    %let hive_table = your_hive_table;        /* Replace with your Hive table name */
    %let partition_column = RPT_PRD_END_DT;   /* Replace with the desired partition column */

    /* Initialize the CREATE TABLE statement in macro variables */
    call symputx('hive_create_query1', "CREATE TABLE IF NOT EXISTS &hive_schema..&hive_table (");
    call symputx('hive_create_query2', '');

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
    col_def = catx(' ', name, hive_type);

    /* Append the column definition to the macro variable */
    if _n_ = 1 then call symputx('hive_create_query2', trim(symget('hive_create_query2')) || col_def);
    else call symputx('hive_create_query2', trim(symget('hive_create_query2')) || ', ' || col_def);

    /* Finalize main column definitions if it's the last row */
    if last then do;
        call symputx('hive_create_query3', ') PARTITIONED BY (');
        call symputx('hive_create_query4', catx(' ', "&partition_column", hive_type, ")"));
        
        /* Add Hive-specific table options for row format and storage */
        call symputx('hive_create_query5', "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS PARQUET;");
    end;

    /* Optional: Print log of each column's mapping */
    putlog 'NOTE: SAS Column=' name ', SAS Type=' type ', Length=' length ', Format=' format ' -> Hive Type=' hive_type;
run;

/* Step 3: Display the full query */
%put &hive_create_query1 &hive_create_query2 &hive_create_query3 &hive_create_query4 &hive_create_query5;

/* Step 4: Use the query in PROC SQL */
proc sql;
    connect to hadoop (your_hive_connection_options); /* Use your connection options */
    execute (
        &hive_create_query1 &hive_create_query2 &hive_create_query3 &hive_create_query4 &hive_create_query5
    ) by hadoop;
    disconnect from hadoop;
quit;
