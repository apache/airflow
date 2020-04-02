#!/usr/bin/env bats


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

@test "convert volume list to docker params" {
  load bats_utils

  initialize_common_environment

  run convert_local_mounts_to_docker_params
  diff <(echo "${output}") - << EOF
-v
${AIRFLOW_SOURCES}/.bash_aliases:/root/.bash_aliases:cached
-v
${AIRFLOW_SOURCES}/.bash_history:/root/.bash_history:cached
-v
${AIRFLOW_SOURCES}/.coveragerc:/opt/airflow/.coveragerc:cached
-v
${AIRFLOW_SOURCES}/.dockerignore:/opt/airflow/.dockerignore:cached
-v
${AIRFLOW_SOURCES}/.flake8:/opt/airflow/.flake8:cached
-v
${AIRFLOW_SOURCES}/.github:/opt/airflow/.github:cached
-v
${AIRFLOW_SOURCES}/.inputrc:/root/.inputrc:cached
-v
${AIRFLOW_SOURCES}/.kube:/root/.kube:cached
-v
${AIRFLOW_SOURCES}/.rat-excludes:/opt/airflow/.rat-excludes:cached
-v
${AIRFLOW_SOURCES}/CHANGELOG.txt:/opt/airflow/CHANGELOG.txt:cached
-v
${AIRFLOW_SOURCES}/Dockerfile.ci:/opt/airflow/Dockerfile.ci:cached
-v
${AIRFLOW_SOURCES}/LICENSE:/opt/airflow/LICENSE:cached
-v
${AIRFLOW_SOURCES}/MANIFEST.in:/opt/airflow/MANIFEST.in:cached
-v
${AIRFLOW_SOURCES}/NOTICE:/opt/airflow/NOTICE:cached
-v
${AIRFLOW_SOURCES}/airflow:/opt/airflow/airflow:cached
-v
${AIRFLOW_SOURCES}/common:/opt/airflow/common:cached
-v
${AIRFLOW_SOURCES}/dags:/opt/airflow/dags:cached
-v
${AIRFLOW_SOURCES}/dev:/opt/airflow/dev:cached
-v
${AIRFLOW_SOURCES}/docs:/opt/airflow/docs:cached
-v
${AIRFLOW_SOURCES}/files:/files:cached
-v
${AIRFLOW_SOURCES}/dist:/dist:cached
-v
${AIRFLOW_SOURCES}/hooks:/opt/airflow/hooks:cached
-v
${AIRFLOW_SOURCES}/logs:/root/airflow/logs:cached
-v
${AIRFLOW_SOURCES}/pylintrc:/opt/airflow/pylintrc:cached
-v
${AIRFLOW_SOURCES}/pytest.ini:/opt/airflow/pytest.ini:cached
-v
${AIRFLOW_SOURCES}/requirements:/opt/airflow/requirements:cached
-v
${AIRFLOW_SOURCES}/scripts:/opt/airflow/scripts:cached
-v
${AIRFLOW_SOURCES}/scripts/ci/in_container/entrypoint_ci.sh:/entrypoint_ci.sh:cached
-v
${AIRFLOW_SOURCES}/setup.cfg:/opt/airflow/setup.cfg:cached
-v
${AIRFLOW_SOURCES}/setup.py:/opt/airflow/setup.py:cached
-v
${AIRFLOW_SOURCES}/tests:/opt/airflow/tests:cached
-v
${AIRFLOW_SOURCES}/tmp:/opt/airflow/tmp:cached
EOF
}
