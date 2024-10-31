proc export data=mydata.sas_dataset
    outfile='/path/to/exported_file.csv'
    dbms=csv
    replace;
run;
