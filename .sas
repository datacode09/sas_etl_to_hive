/* Include the external file to retrieve username and password */
%include '/path/to/useps.sas';

/* Configure SAS options for Hadoop and RESTful interaction */
options set=SAS_HADOOP_RESTFUL="1";
options set=HADOOP_GATEWAY_URL="https://enchbcclpredgp01.srv.gc.net:8443/gateway/default";
options set=SAS_HADOOP_CONFIG_PATH="/opt/sas/hadoop/files/core-site.xml";
options set=SAS_HADOOP_JAR_PATH="/opt/sas/hadoop/jars/lib";

%let hive_server=enchbcclpredgp01.srv.bmogc.net;
%let hive_port=8443;
%let hive_uri=https://enchbcclpredgp01.srv.bmogc.net:8443/;
%let hive_transport_mode=http;
%let hive_http_path=gateway/default/hive2;
%let hive_schema=abx_pacb_arm_common_data;

proc sql;
   /* Establish connection to Hive using PROC SQL and connect to hadoop */
   connect to hadoop 
      (server="&hive_server"
       port=&hive_port
       uri="&hive_uri;ssl=true;transportMode=&hive_transport_mode;httpPath=&hive_http_path"
       schema="&hive_schema"
       user="&hive_username"
       password="&hive_password"
       read_method=hdfs);

   /* Execute DESCRIBE FORMATTED command in Hive */
   execute (DESCRIBE FORMATTED abx_pacb_arm_common_data.account_universe_202405) by hadoop;

   /* Disconnect from Hive */
   disconnect from hadoop;
quit;
