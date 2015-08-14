export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg

# Generate the `airflow` executable if needed
which airflow > /dev/null || python setup.py develop

# initialize the test db
AIRFLOW_DB=$AIRFLOW_HOME/unittests.db
ls -s $AIRFLOW_DB > /dev/null 2>&1 || airflow initdb # if it's missing
ls -s $AIRFLOW_DB | egrep '^0 ' > /dev/null && airflow initdb # if it's blank

nosetests --with-doctest \
          --with-coverage \
          --cover-erase \
          --cover-html \
          --cover-package=airflow \
          --cover-html-dir=airflow/www/static/coverage \
          -v \
          --logging-level=DEBUG
# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
