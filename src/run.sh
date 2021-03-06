#!/bin/sh


set -o errexit
set -o nounset


echo "wait"


start-history-server.sh
echo "History server is sarting ...."
sleep 2
echo "Master is sarting ...."
start-master.sh

sleep 1
echo "Master started at port 8080 ...."

echo "worker is sarting ...."

start-slave.sh spark://app:7077

sleep 1
echo "worker started at port 8081 ...."

sleep 1
echo "worker started at port 8081 ...."

echo "airflow database init ...."

airflow initdb

sleep 5

echo "airflow app started ...."

airflow webserver -p 5050 & sleep 5 & airflow scheduler