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

echo
echo "Initializing the Airflow db"
echo


# Init and upgrade the database to latest heads
cd "${AIRFLOW_SOURCES}"/airflow || exit 1

airflow initdb
alembic upgrade heads

echo
echo "Initialized the database"
echo

# Create Airflow User if it does not exist
airflow create_user \
    --username airflow \
    --lastname airflow \
    --firstname jon \
    --email airflow@apache.org \
    --role Admin --password airflow || true

echo
echo "Created airflow user"
echo
