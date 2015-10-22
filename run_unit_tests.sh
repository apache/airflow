export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

echo "Initializing the DB"
airflow initdb

echo "Starting the unit tests"
nosetests \
    --with-coverage \
    --cover-erase \
    --cover-html \
    --cover-package=airflow \
    --cover-html-dir=airflow/www/static/coverage \
    -v \
    --logging-level=DEBUG \
    --exclude-test=tests.core.HivePrestoTest
    #--with-doctest \
# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
