#!/usr/bin/env bash

source ../config/$1/kerbros.prop

echo $keytabFile
echo $principalName

random_file_postfix=`mktemp -u cacheXXXXXXX`
host=`hostname`
user=`whoami`
echo "User : $user"
echo "Host Name : $host"
echo "random_file_postfix :" $random_file_postfix
# get a ticket if one is not already obtained
echo "Loading kerberos principal : $principalName"
echo "Keytab : $keytabFile"
set +e
echo "Checking if keytab exists"
ls -ltr $keytabFile
echo "Current kerberos details:"
klist 2>&1
date_postfix=`date +"%Y%m%d%H%M"`
cache_file_name="/tmp/krb5cc_$user""_$date_postfix""_$random_file_postfix"
echo "New Kerberos Cache Ticket File : $cache_file_name"

kinit -c FILE:$cache_file_name  $principalName -kt $keytabFile 2>&1
export KRB5CCNAME="$cache_file_name"
#unset HADOOP_TOKEN_FILE_LOCATION
ls -ltr $cache_file_name
if [ $? -ne 0 ]
then
        echo "Error while initializing kerberos"
else
        echo "Kerberos authentication completed"
fi ;
set -e
klist


/usr/hdp/current/spark2-client/bin/spark-submit --master local[*] --keytab $keytabFile --principal $principalName --class com.ps.data.pipeline.management.app --jars /usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/hive-client/lib/hive-hbase-handler-1.2.1000.2.6.4.69-1.jar,/usr/hdp/current/phoenix-client/lib/phoenix-spark-4.7.0.2.6.4.69-1.jar,/usr/hdp/current/phoenix-client/phoenix-server.jar --files /etc/spark/conf/hbase-site.xml,/usr/hdp/current/spark-client/conf/hive-site.xml --verbose ../realtime-monitor-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2
