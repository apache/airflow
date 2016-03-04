#!/bin/sh

export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg

# any argument received is overriding the default nose execution arguments: 

nose_args=$@
if [ -z "$nose_args" ]; then
  nose_args="--with-coverage \
--cover-erase \
--cover-html \
--cover-package=airflow \
--cover-html-dir=airflow/www/static/coverage \
-s \
-v \
--logging-level=DEBUG "
fi

#--with-doctest

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

echo "Initializing the DB"
yes | airflow resetdb
airflow initdb

echo "Starting the unit tests with the following nose arguments: "$nose_args
nosetests $nose_args

# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
