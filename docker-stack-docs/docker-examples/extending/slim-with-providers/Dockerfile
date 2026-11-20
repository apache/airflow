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

# This is an example Dockerfile. It is not intended for PRODUCTION use
# [START Dockerfile]
FROM apache/airflow:slim-3.5.0
RUN PYTHON_MAJOR_MINOR="$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')" \
  && pip install --no-cache-dir \
       "apache-airflow==${AIRFLOW_VERSION}" \
       apache-airflow-providers-postgres \
       apache-airflow-providers-cncf-kubernetes \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt"
# [END Dockerfile]
