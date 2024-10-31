%let hive_server=enchbcclpredgp01.srv.bmogc.net;
%let hive_port=8443;
%let hive_uri=https://enchbcclpredgp01.srv.bmogc.net:8443/;
%let hive_transport_mode=http;
%let hive_http_path=gateway/default/hive2;
%let hive_schema=abx_pacb_arm_common_data;
%let hive_username=your_username; /* Replace with your actual username */
%let hive_password=your_password; /* Replace with your actual password */

proc sql;
   connect to hadoop 
      (server="&hive_server"
       port=&hive_port
       uri="&hive_uri;ssl=true;transportMode=&hive_transport_mode;httpPath=&hive_http_path"
       schema="&hive_schema"
       user=&hive_username
       password=&hive_password
       read_method=hdfs);

   /* Execute Hive commands */
   execute (DROP TABLE IF EXISTS abx_user.userid.TESTTABLE) by hadoop;
   execute (DESCRIBE FORMATTED abx_pacb_arm_common_data.account_universe_202405) by hadoop;

   disconnect from hadoop;
quit;
