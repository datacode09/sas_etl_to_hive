/* Step 1: Extract metadata from the SAS dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Generate the Hive CREATE TABLE statement */
data _null_;
    /* Define the Hive schema, table name, and partition column */
    %let hive_schema = your_schema;           /* Replace with your Hive schema name */
    %let hive_table = your_hive_table;        /* Replace with your Hive table name */
    %let partition_column = RPT_PRD_END_DT;   /* Replace with the desired partition column */

    /* Specify the file path to save the generated Hive CREATE TABLE statement */
    file '/path/to/hive_create_table.sql'; /* Replace with your desired file path */

    /* Start the CREATE TABLE statement */
    put "CREATE TABLE IF NOT EXISTS &hive_schema..&hive_table (";

    /* Loop through each column and generate Hive-compatible column definitions */
    set schema_metadata end=last;
    length hive_type $20;

    /* Map SAS types to Hive-compatible types */
    if type = 1 then do; /* Numeric */
        if format in ('DATE9.', 'DDMMYY10.', 'MMDDYY10.', 'YYMMDD10.') then hive_type = "DATE"; /* Map known date formats to Hive DATE */
        else if length <= 4 then hive_type = "INT";       /* Small integers */
        else if length <= 8 then hive_type = "BIGINT";    /* Large integers */
        else hive_type = "DOUBLE";                        /* Floating point */
    end;
    else if type = 2 then hive_type = "STRING"; /* Character type */

    /* Write each column with its corresponding Hive type */
    if name ne "&partition_column" then do;
        put '    ' name ' ' hive_type @;
        if not last then put ',';
    end;

    /* Finalize the main column definitions */
    if last then put ')';

    /* Add PARTITIONED BY clause for the specified partition column */
    if last then do;
        /* Find the partition column and define it with the correct type */
        if name = "&partition_column" then do;
            put "PARTITIONED BY (";
            put '    ' name ' ' hive_type;
            put ")";
        end;

        /* Add Hive-specific table options for row format and storage */
        put "ROW FORMAT DELIMITED";
        put "FIELDS TERMINATED BY ','";
        put "STORED AS PARQUET;";
    end;

    /* Optional: Print detailed log of each column's mapping */
    putlog 'NOTE: SAS Column=' name ', SAS Type=' type ', Length=' length ', Format=' format ' -> Hive Type=' hive_type;
run;
