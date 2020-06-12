#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

# This part is to allow fast iteration with the kubernetes tests
# Pre-installed Airflow from the production image is removed and Airflow is re-installed from the
# sources added during preparing the kubernetes image. This way when we deploy the image
# to KinD we only add latest sources and only that most recent layer is sent to kind
# and airflow always runs with compiled dist web files from pre-compiled dist installed in prod image


echo
echo "Save minimised web files"
echo

mv "$(python -m site | grep ^USER_SITE | awk '{print $2}' | tr -d "'")/airflow/www/static/dist/" \
    "/tmp"

echo
echo "Uninstalling pre-installed airflow"
echo

# Uninstall preinstalled Apache Airflow
pip uninstall -y apache-airflow


echo
echo "Installing airflow from the sources"
echo

# Installing airflow from the sources copied to the Kubernetes image
pip install --user "${AIRFLOW_SOURCES}"

echo
echo "Restore minimised web files"
echo

mv "/tmp/dist" "$(python -m site | grep ^USER_SITE | awk '{print $2}' | tr -d "'")/airflow/www/static/"

echo
echo "Airflow prepared. Running ${1}"
echo


if [[ "$1" = "webserver" ]]
then
    exec airflow webserver
fi

if [[ "$1" = "scheduler" ]]
then
    exec airflow scheduler
fi

echo
echo "Entering bash"
echo

exec /bin/bash
