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

@test "yaml parsing missing file" {
  load bats_utils

  initialize_breeze_environment

  run parse_yaml
  [ "${status}" == 1 ]
  [ "${output}" == "Please provide yaml filename as first parameter." ]
}

@test "yaml parsing existing file" {
  load bats_utils

  initialize_breeze_environment

  run parse_yaml "scripts/ci/docker-compose/local.yml"
  diff <(echo "${output}") - << 'EOF'
1="--"
version="2.2"
services_airflow-testing_volumes_1="../../../.bash_aliases:/root/.bash_aliases:cached"
services_airflow-testing_volumes_2="../../../.bash_history:/root/.bash_history:cached"
services_airflow-testing_volumes_3="../../../.coveragerc:/opt/airflow/.coveragerc:cached"
services_airflow-testing_volumes_4="../../../.dockerignore:/opt/airflow/.dockerignore:cached"
services_airflow-testing_volumes_5="../../../.flake8:/opt/airflow/.flake8:cached"
services_airflow-testing_volumes_6="../../../.github:/opt/airflow/.github:cached"
services_airflow-testing_volumes_7="../../../.inputrc:/root/.inputrc:cached"
services_airflow-testing_volumes_8="../../../.kube:/root/.kube:cached"
services_airflow-testing_volumes_9="../../../.rat-excludes:/opt/airflow/.rat-excludes:cached"
services_airflow-testing_volumes_10="../../../CHANGELOG.txt:/opt/airflow/CHANGELOG:cached"
services_airflow-testing_volumes_11="../../../Dockerfile:/opt/airflow/Dockerfile:cached"
services_airflow-testing_volumes_12="../../../LICENSE:/opt/airflow/LICENSE:cached"
services_airflow-testing_volumes_13="../../../MANIFEST.in:/opt/airflow/MANIFEST.in:cached"
services_airflow-testing_volumes_14="../../../NOTICE:/opt/airflow/NOTICE:cached"
services_airflow-testing_volumes_15="../../../airflow:/opt/airflow/airflow:cached"
services_airflow-testing_volumes_16="../../../common:/opt/airflow/common:cached"
services_airflow-testing_volumes_17="../../../dags:/opt/airflow/dags:cached"
services_airflow-testing_volumes_18="../../../dev:/opt/airflow/dev:cached"
services_airflow-testing_volumes_19="../../../docs:/opt/airflow/docs:cached"
services_airflow-testing_volumes_20="../../../files:/files:cached"
services_airflow-testing_volumes_21="../../../dist:/dist:cached"
services_airflow-testing_volumes_22="../../../hooks:/opt/airflow/hooks:cached"
services_airflow-testing_volumes_23="../../../logs:/root/airflow/logs:cached"
services_airflow-testing_volumes_24="../../../pylintrc:/opt/airflow/pylintrc:cached"
services_airflow-testing_volumes_25="../../../pytest.ini:/opt/airflow/pytest.ini:cached"
services_airflow-testing_volumes_26="../../../scripts:/opt/airflow/scripts:cached"
services_airflow-testing_volumes_27="../../../scripts/ci/in_container/entrypoint_ci.sh:/entrypoint_ci.sh:cached"
services_airflow-testing_volumes_28="../../../setup.cfg:/opt/airflow/setup.cfg:cached"
services_airflow-testing_volumes_29="../../../setup.py:/opt/airflow/setup.py:cached"
services_airflow-testing_volumes_30="../../../tests:/opt/airflow/tests:cached"
services_airflow-testing_volumes_31="../../../tmp:/opt/airflow/tmp:cached"
services_airflow-testing_environment_1="HOST_USER_ID"
services_airflow-testing_environment_2="HOST_GROUP_ID"
services_airflow-testing_environment_3="PYTHONDONTWRITEBYTECODE"
services_airflow-testing_ports_1="${WEBSERVER_HOST_PORT}:8080"
EOF
}


@test "convert yaml docker file to docker params" {
  load bats_utils

  initialize_breeze_environment

  run convert_docker_mounts_to_docker_params
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
${AIRFLOW_SOURCES}/CHANGELOG.txt:/opt/airflow/CHANGELOG:cached
-v
${AIRFLOW_SOURCES}/Dockerfile:/opt/airflow/Dockerfile:cached
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
