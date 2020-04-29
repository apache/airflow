#!/bin/sh

# WARNING: Run this script only during initial db setup. DO NOT run this script on an existing Airflow DB.

IS_INITDB=True
AIRFLOW_USER=airflow_test_user
AIRFLOW_PASSWORD=airflow_test_password
AIRFLOW_USER_EMAIL=airflow@airflow.com

if [ $IS_INITDB ]; then

  echo "Initializing Airflow DB setup and Admin user setup because value of IS_INITDB is $IS_INITDB"
  echo " Airflow admin username will be $AIRFLOW_USER"

  docker exec -ti airflow_cont airflow initdb && echo "Initialized airflow DB"
  docker exec -ti airflow_cont airflow create_user --role Admin --username $AIRFLOW_USER --password $AIRFLOW_PASSWORD -e $AIRFLOW_USER_EMAIL -f airflow -l airflow && echo "Created airflow Initial admin user with username $AIRFLOW_USER"

else
  echo "Skipping InitDB and InitUser setup because value of IS_INITDB is $IS_INITDB"
fi
