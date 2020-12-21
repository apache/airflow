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
  load ../bats_utils
  rm -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
}

teardown() {
  rm -f "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
}

@test "Test missing value for a parameter" {
  export _breeze_allowed_test_params="a b c"
  run parameters::check_and_save_allowed_param "TEST_PARAM"  "Test Param" "--message"
  assert_output --regexp "Allowed Test Param: \[ a b c \]\. Passed: ''"
  assert_failure
}

@test "Test wrong value for a parameter but proper stored in the .build/PARAM" {
  export _breeze_allowed_test_params="a b c"
  export TEST_PARAM=x
  echo "a" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run parameters::check_and_save_allowed_param "TEST_PARAM"  "Test Param" "--message"
  assert_output --regexp "Allowed Test Param: \[ a b c \]\. Passed: 'x"
  assert_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_file_contains "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" "^a$"
  assert_failure 1
}

@test "Test wrong value for a parameter stored in the .build/PARAM" {
  export _breeze_allowed_test_params="a b c"
  export TEST_PARAM=x
  echo "x" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run parameters::check_and_save_allowed_param "TEST_PARAM"  "Test Param" "--message"
  assert_output --regexp "Allowed Test Param: \[ a b c \]\. Passed: 'x'"
  assert_not_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_failure 1
}


@test "Test correct value for a parameter" {
  export _breeze_allowed_test_params="a b c"
  export TEST_PARAM=a
  run parameters::check_and_save_allowed_param "TEST_PARAM"  "Test Param" "--message"
  assert_output ""
  assert_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_file_contains "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" "^a$"
  assert_success
}

@test "Test correct value for a parameter from multi-line values" {
  _breeze_allowed_test_params=$(cat <<-EOF
a
b
c
EOF
)
  export _breeze_allowed_test_params
  export TEST_PARAM=a
  run parameters::check_and_save_allowed_param "TEST_PARAM"  "Test Param" "--message"
  assert_output ""
  assert_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_file_contains "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" "^a$"
  assert_success
}


@test "Test read_parameter from missing file" {
  run parameters::read_from_file TEST_PARAM
  assert [ -z "${TEST_FILE}" ]
  assert_output ""
  assert_not_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_success
}

@test "Test read_parameter from file" {
  echo "a" > "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  run parameters::read_from_file TEST_PARAM
  assert_exist "${AIRFLOW_SOURCES}/.build/.TEST_PARAM"
  assert_file_contains "${AIRFLOW_SOURCES}/.build/.TEST_PARAM" "^a$"
  assert_success
}
