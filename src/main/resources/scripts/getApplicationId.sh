#!/usr/bin/env bash
appName=$1
#echo $appName
yarn application -list | awk -v appName="$appName" '$2 == appName { print $1 }'