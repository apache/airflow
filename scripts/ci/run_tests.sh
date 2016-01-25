#!/usr/bin/env bash
set -o verbose

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"

pwd

mkdir ~/airflow/
echo creating a unittest config with sql_alchemy_conn $BACKEND_SQL_ALCHEMY_CONN
sed -e "s#sql_alchemy_conn.*#sql_alchemy_conn\ =\ $BACKEND_SQL_ALCHEMY_CONN#g" ${TRAVIS_BUILD_DIR}/scripts/ci/airflow_travis.cfg > ~/airflow/unittests.cfg
./run_unit_tests.sh
