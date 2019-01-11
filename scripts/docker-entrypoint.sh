#!/usr/bin/env bash

set -e

echo starting airflow with command:
echo airflow $AIRFLOW_CMD

airflow $AIRFLOW_CMD
