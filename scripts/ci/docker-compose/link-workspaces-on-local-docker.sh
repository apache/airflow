#!/bin/sh
 
if [ ! -d /workspaces ] ; then
  # Running in docker so create missing link from /opt/airflow to /workspaces/airflow
  #
  # This is created automatically when running in codespaces so we need to replicate it
  # in the local docker environment
  mkdir /workspaces && ln -s /opt/airflow /workspaces/airflow
fi
