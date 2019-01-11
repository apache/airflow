#!/usr/bin/env bash

set -e

echo starting airflow with command:
echo airflow $@

exec airflow $@
