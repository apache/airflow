#!/usr/bin/env bash

set -e

echo Starting Apache Airflow with command:
echo airflow $@

exec airflow $@
