export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}
export AIRFLOW_CONFIG=$AIRFLOW_HOME/unittests.cfg

nosetests --with-doctest --with-coverage --cover-html --cover-package=airflow -v --cover-html-dir=airflow/www/static/coverage
# To run individual tests:
# nosetests tests.core:CoreTest.test_scheduler_job
