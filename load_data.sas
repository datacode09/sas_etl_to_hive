/* Define the path where the .sas7bdat file is located */
%let sas_dataset_path = '/path/to/your/dataset';

/* Assign a library to the specified path */
libname mydata "&sas_dataset_path";

/* Read the .sas7bdat file into a new dataset in the WORK library */
data work.my_dataset_copy;
   set mydata.your_dataset_name; /* Replace 'your_dataset_name' with the actual file name without the .sas7bdat extension */
run;

/* Display the first few rows of the dataset */
proc print data=work.my_dataset_copy (obs=10);
run;

/* Clear the library */
libname mydata clear;
