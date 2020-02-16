#!/usr/bin/env bats
# shellcheck disable=SC2030
# shellcheck disable=SC2031
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


setup() {
  load bats_utils
  rm -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
}

teardown() {
  load bats_utils
  rm -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
}

@test "Test missing value for a parameter" {
  load bats_utils
  export _BREEZE_ALLOWED_TEST_PARAMS=" a b c "
  run check_for_allowed_params "TEST_PARAM"  "Test Param" "--message"
  diff <(echo "${output}") - <<EOF

ERROR:  Allowed Test Param: [ a b c ]. Is: ''.

Switch to supported value with --message flag.
EOF
  [ "${status}" == "1" ]
}

@test "Test wrong value for a parameter but proper stored in the .build/PARAM" {
  load bats_utils

  initialize_breeze_environment

  export _BREEZE_ALLOWED_TEST_PARAMS=" a b c "
  export TEST_PARAM=x
  echo "a" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run check_for_allowed_params "TEST_PARAM"  "Test Param" "--message"
  diff <(echo "${output}") - <<EOF

ERROR:  Allowed Test Param: [ a b c ]. Is: 'x'.

Switch to supported value with --message flag.
EOF
  [ -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" ]
  diff <(cat "${AIRFLOW_SOURCES}/.build/.TEST_PARAM") <(echo "a")
  [ "${status}" == "1" ]
}

@test "Test wrong value for a parameter stored in the .build/PARAM" {
  load bats_utils

  initialize_breeze_environment

  export _BREEZE_ALLOWED_TEST_PARAMS=" a b c "
  export TEST_PARAM=x
  echo "x" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run check_for_allowed_params "TEST_PARAM"  "Test Param" "--message"
  diff <(echo "${output}") - <<EOF

ERROR:  Allowed Test Param: [ a b c ]. Is: 'x'.

Switch to supported value with --message flag.

Removing ${AIRFLOW_SOURCES}/.build/.TEST_PARAM. Next time you run it, it should be OK.
EOF
  [ ! -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" ]
  [ "${status}" == "1" ]
}


@test "Test correct value for a parameter" {
  load bats_utils

  initialize_breeze_environment

  export _BREEZE_ALLOWED_TEST_PARAMS=" a b c "
  export TEST_PARAM=a
  run check_for_allowed_params "TEST_PARAM"  "Test Param" "--message"
  diff <(echo "${output}") <(echo "")
  [ -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" ]
  diff <(echo "a") <(cat "${AIRFLOW_SOURCES}/.build/.TEST_PARAM")
  [ "${status}" == "0" ]
}

@test "Test read_parameter from missing file" {
  load bats_utils

  initialize_breeze_environment

  run read_from_file TEST_PARAM
  [ -z "${TEST_FILE}" ]
  diff <(echo "${output}") <(echo "")
  [ ! -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" ]
  [ "${status}" == "0" ]
}

@test "Test read_parameter from file" {
  load bats_utils

  initialize_breeze_environment

  echo "a" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run read_from_file TEST_PARAM
  diff <(echo "${output}") <(echo "a")
  [ -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" ]
  [ "${status}" == "0" ]
}
