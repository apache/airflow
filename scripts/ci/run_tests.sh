#!/usr/bin/env bash
set -o verbose

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"

pwd

mkdir ~/airflow/
echo Backend: $AIRFLOW__CORE__SQL_ALCHEMY_CONN
./run_unit_tests.sh
