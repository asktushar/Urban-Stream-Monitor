# Urban Stream Monitor (Spark Pipeline Management Suite)

Urban Stream Monitor(USM) is a spark pipeline(Batch/Realtime) management application designed to automate pipeline monitoring, continuous health check, and support. Significant features are listed below:-

## Product Detail
Product details can be found at http://urbanstreammonitor.com/

## Features

1. Continuous monitoring of spark pipeline, both batch and real-time.
2. Constant health check of the real-time pipelines. 
3. Easy to configure unlimited pipelines.
4. Restarts broken/stuck pipelines based on the provided count query not increasing in configured time.
5. Kills broken/stuck pipelines based on the provided count query not increasing in configured time.
6. Designed to use Hive/Phonix/JDBC Connectors.
7. Creates logs of every run and monitors it continuously. 
8. Designed to work on cloud and on-premise clusters. Tested on GCP Data Proc. 


## Installation

Use MVN to install USM locally.

```bash
clean compile install
```
You need to create a hive table for logging purposes -

```bash
 ##DEV_HIVE
 CREATE TABLE IF NOT EXISTS RealTimeLogs(
  app_name string,
  application_id string,
  status string,
  count string,
  log_datetime string,
  desc string
  )
  PARTITIONED BY (
  LogDate date);
```

## Application Configuration
All the configuration goes inside the config/ folder in respective environment folders.

#### appConfig.prop
appConfig.prop contains application-level configurations. One row per spark pipeline you would want to configure using USM. Explained with the example below-
```
APP_NAME,COUNT_QUERY_TYPE,CURRENT_COUNT_QUERY,IDLE_TIME_TO_RESTART,IDLE_TIME_TO_FAIL
APP_SCRIPT_RUN_RT_TRANSACTION_APP,Phoenix,select count(*) as count from "PROD_HBASE"."TransactionRT",180,720
```
1. APP_NAME indicates your spark pipeline yarn application name.
2. CURRENT_COUNT_QUERY indicates the count query of the target table for the above yarn application. The USM is designed to check this count and take action if the count is not increasing enough.
3. COUNT_QUERY_TYPE indicates the connector for the above count query. Currently accepts (Phoenix/Hive/JDBC)
4. IDLE_TIME_TO_RESTART shows the time in Minutes to restart the spark pipeline if the count from CURRENT_COUNT_QUERY is increasing within the time limit.
5. IDLE_TIME_TO_FAIL indicates the time in Minutes to kill the spark pipeline if the count from CURRENT_COUNT_QUERY is not increased within this time limit. The objective here is to indicate a major issue in the real-time pipeline which might need human intervention. 

#### envConfig.prop
envConfig.prop is meant to store envLevel configurations which are supposed to be common across all the pipelines you will configure using USM. Explained with the example below-

```
hiveConnectionUrl=<"Insert Connection detail here">
PhoenixConnectionUrl=<"Insert Connection detail here">
insertLogQuery=insert into dev_hive.RealTimeLogs PARTITION(LogDate=?) (app_name,application_id,status,count,log_datetime,desc) values(?,?,?,?,?,?)
logSqlQuery=select count from (select * from dev_hive.RealTimeLogs where app_name=? and log_datetime > ? and logdate >= ? order by log_datetime asc limit 1)a
timestampSqlQuery=select log_datetime from (select * from dev_hive.RealTimeLogs where app_name=? and log_datetime > ? and logdate >= ? order by log_datetime asc limit 1)a
getFirstLogQuery=select count(*) as count from dev_hive.RealTimeLogs where app_name=?
lastRestartQuery=select count from (select count(*) as count from dev_hive.RealTimeLogs where app_name=? and status=? and log_datetime > ? and logdate>=?  limit 5)a
```

1. hiveConnectionUrl indicates your Zookeeper URL for connecting to hive.
2. PhoenixConnectionUrl indicates your zookeeper URL for connecting to phoenix.

[You do not have to change the below properties]

3. insertLogQuery indicates your hive insert prepared statement to create logs of USM.
4. logSqlQuery indicates a prepared statement to get the count for IDLE_TIME_TO_RESTART and IDLE_TIME_TO_FAIL checks.
5. timestampSqlQuery/getFirstLogQuery/lastRestartQuery also also used for the functionality.

#### kerbros.prop
Stores the Kerberos authentication details.  

## Functionality Configuration
After using the below Usage method to run USM Application. If the Application needs to start/restart the spark pipeline. Tt triggers the APP_SCRIPT_RUN_RT_TRANSACTION_APP.sh file available in appScripts folder. The .sh file name and the APP_NAME configured above should be the same. This file should ideally contain the code to run your spark pipeline you are configuring into USM.


## Usage
To run USM utility, you need to use scripts/execute-realtime-utility.sh which expects APP_NAME as an input.
 
```
./execute-realtime-utility.sh APP_SCRIPT_RUN_RT_TRANSACTION_APP
```




## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update the tests as appropriate.

For any issues/change requests/suggestions/help kindly contact contact@asktushar.com

## License
Made for the open source to be used accross Data Intensive projects. Do leave your feedback if you are using it.
