#!/usr/bin/env bash
cp -r ../dags ./dags
cp -r ../plugins ./plugins
docker build -t registry.centron.cn:5000/airflow/airflow:latest --no-cache .
