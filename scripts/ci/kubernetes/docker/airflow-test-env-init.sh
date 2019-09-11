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

set -x

cd /usr/local/lib/python2.7/dist-packages/airflow && \
cp -R example_dags/* /root/airflow/dags/ && \
cp -R contrib/example_dags/example_kubernetes_*.py /root/airflow/dags/ && \
cp -a contrib/example_dags/libs /root/airflow/dags/ && \
airflow initdb && \
alembic upgrade heads && \
(airflow create_user -u airflow -l airflow -f jon -e airflow@apache.org -r Admin -p airflow || true) && \
echo "retrieved from mount" > /root/test_volume/test.txt
