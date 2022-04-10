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
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

# Some of our provider sources are not Airflow 2.1 compliant any more
# We replace them with 2.1 compliant versions from PyPI to run the checks

cd "${AIRFLOW_SOURCES}" || exit 1
rm -rvf dist/apache_airflow_providers_cncf_kubernetes* dist/apache_airflow_providers_celery*
pip download --no-deps --dest dist apache-airflow-providers-cncf-kubernetes==3.0.0 \
    apache-airflow-providers-celery==2.1.3
