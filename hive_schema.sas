/* Step 1: Use PROC CONTENTS to extract metadata from the SAS dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Create the Hive CREATE TABLE statement */
data _null_;
    /* Define the Hive table name and schema */
    %let hive_schema = your_schema;    /* Replace with your desired Hive schema name */
    %let hive_table = your_hive_table; /* Replace with your desired Hive table name */

    /* Open an external file to write the Hive CREATE TABLE statement */
    file '/path/to/hive_create_table.sql'; /* Replace with the path where you want to save the SQL file */

    /* Start the CREATE TABLE statement */
    put "CREATE TABLE IF NOT EXISTS &hive_schema..&hive_table (";

    /* Loop through each column and generate the corresponding Hive column definition */
    set schema_metadata end=last;
    length hive_type $20;

    /* Map SAS types to Hive types */
    if type = 1 then do; /* Numeric type */
        if format = 'DATE9.' then hive_type = "DATE";
        else if length <= 4 then hive_type = "INT";
        else if length <= 8 then hive_type = "BIGINT";
        else hive_type = "DOUBLE";
    end;
    else if type = 2 then hive_type = "STRING"; /* Character type */

    /* Write the column definition */
    put '    ' name ' ' hive_type @;
    if not last then put ',';
    else put ')';

    /* Add table options for row format and storage */
    if last then do;
        put "ROW FORMAT DELIMITED";
        put "FIELDS TERMINATED BY ','";
        put "STORED AS PARQUET;";
    end;
run;
