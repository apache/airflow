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

# shellcheck disable=SC2030,SC2031

@test "Test build_regexp_string" {
  load bats_utils

  run selective_tests::build_regexp_string "x" "y" "z"
  assert_output "x|y|z"
}


@test "Test changed files match" {
  load bats_utils

  export changed_files="y"
  run selective_tests::print_changed_files_matching_patterns "x" "y" "z"
  assert_output "
Changed files

y"
}

@test "Test changed files do not natch" {
  load bats_utils

  export changed_files="v"
  run selective_tests::print_changed_files_matching_patterns "x" "y" "z"
  assert_output "
Changed files"
}

@test "Test changed files match count 1" {
  load bats_utils

  export changed_files="y"
  run selective_tests::count_changed_files_matching_patterns "x" "y" "z"
  assert_output "1"
}

@test "Test changed files match count 2" {
  load bats_utils

  export changed_files="y x"
  run selective_tests::count_changed_files_matching_patterns "x" "y" "z"
  assert_output "1"
}

@test "Test changed files do not match count 0" {
  load bats_utils

  export changed_files="v"
  run selective_tests::count_changed_files_matching_patterns "x" "y" "z"
  assert_output "0"
}


@test "Check event type: All tests on event name push" {
  load bats_utils

  export CI_EVENT_TYPE="push"
  run selective_tests::check_event_type
  assert_output '
Always run all tests on push

::set-output name=test-types::[]
::set-output name=backends::["sqlite","postgres","mysql"]
::set-output name=directories::["."]'
}

@test "Check event type: Nothing on event name pull" {
  load bats_utils

  export CI_EVENT_TYPE="pull"
  export CI_EVENT_TYPE
  run selective_tests::check_event_type
  assert_output ''
}


@test "Check all tests triggered on script changes" {
  load bats_utils

  export changed_files="
scripts/ci/images/ci_push_ci_images.sh
"
  run selective_tests::check_scripts_changed
  assert_output '
Always run all tests when scripts change


Changed files

scripts/ci/images/ci_push_ci_images.sh

::set-output name=test-types::[]
::set-output name=backends::["sqlite","postgres","mysql"]
::set-output name=directories::["."]'

}


@test "Check only some tests triggered on script changes" {
  load bats_utils

  export changed_files="
airflow/core.py
scripts/ci/images/ci_push_ci_images.sh
"
  run selective_tests::check_scripts_changed
  assert_output '
Always run all tests when scripts change


Changed files

scripts/ci/images/ci_push_ci_images.sh

::set-output name=test-types::[]
::set-output name=backends::["sqlite","postgres","mysql"]
::set-output name=directories::["."]'

}

@test "Check scripts not triggered on no script changes" {
  load bats_utils

  export changed_files="
airflow/core.py
"
  run selective_tests::check_scripts_changed
  assert_output ''

}
