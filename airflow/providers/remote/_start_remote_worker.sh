#!/bin/bash

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

unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
unset AIRFLOW__CELERY__RESULT_BACKEND
unset POSTGRES_HOST_PORT
unset BACKEND
unset POSTGRES_VERSION
unset DATABASE_ISOLATION

export AIRFLOW_ENABLE_AIP_44=true
export AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION=True
export AIRFLOW__CORE__INTERNAL_API_URL=http://localhost:8080/remote_worker/v1/rpcapi
export AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION=False

airflow remote worker
