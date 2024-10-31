/* Include the external file to retrieve username and password */
%include '/path/to/useps.sas';

/* Configure SAS options for Hadoop and RESTful interaction */
options set=SAS_HADOOP_RESTFUL="1";
options set=HADOOP_GATEWAY_URL="https://enchbcclpredgp01.srv.bmogc.net:8443/gateway/default";
options set=SAS_HADOOP_CONFIG_PATH="/opt/sas/hadoop/files/core-site.xml";
options set=SAS_HADOOP_JAR_PATH="/opt/sas/hadoop/jars/lib";

%let hive_server=enchbcclpredgp01.srv.bmogc.net;
%let hive_port=8443;
%let hive_schema=abx_pacb_arm_common_data;

/* Establish a LIBNAME connection to Hive */
libname hadoop hadoop 
    server="&hive_server"
    port=&hive_port
    schema="&hive_schema"
    user="&hive_username"
    password="&hive_password"
    read_method=hdfs;

/* Use PROC CONTENTS to get metadata information about the table (similar to DESCRIBE FORMATTED) */
proc contents data=hadoop.account_universe_202405 out=table_metadata(keep=name type length format) noprint;
run;

/* Print the metadata information */
proc print data=table_metadata label;
   title "Metadata Information for abx_pacb_arm_common_data.account_universe_202405";
   label name = "Column Name"
         type = "Data Type"
         length = "Column Length"
         format = "Format";
run;

/* Clear the LIBNAME connection */
libname hadoop clear;
