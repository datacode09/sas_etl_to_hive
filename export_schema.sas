/* proc export data=mydata.sas_dataset
    outfile='/path/to/exported_file.csv'
    dbms=csv
    replace;
run;
 */

/* Step 1: Use PROC CONTENTS to get the metadata of the dataset */
proc contents data=mydata.sas_dataset out=schema_metadata(keep=name type length format) noprint;
run;

/* Step 2: Export the metadata to a CSV file */
proc export data=schema_metadata
    outfile='/path/to/schema.csv' /* Update with the desired file path */
    dbms=csv
    replace;
run;

