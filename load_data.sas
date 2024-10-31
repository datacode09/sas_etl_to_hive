/* Define the path where the .sas7bdat file is located */
%let sas_dataset_path = '/path/to/your/dataset'; /* Replace with actual path */

/* Assign a library to the specified path */
libname mydata "&sas_dataset_path";

/* Check if the dataset exists in the library */
proc contents data=mydata._all_ nods;
   title "Listing of Datasets in the mydata Library";
run;

/* Attempt to load the dataset if it exists */
%let dataset_name = ag_data; /* Replace with the actual file name without .sas7bdat */

%if %sysfunc(exist(mydata.&dataset_name)) %then %do;
   data work.my_dataset_copy;
      set mydata.&dataset_name;
   run;

   /* Display the first few rows of the dataset */
   proc print data=work.my_dataset_copy (obs=10);
   run;
%end;
%else %do;
   %put ERROR: The dataset &dataset_name does not exist in the path &sas_dataset_path;
%end;

/* Clear the library */
libname mydata clear;
