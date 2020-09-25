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
setup() {
    load bats_utils
    source "${AIRFLOW_SOURCES}/scripts/in_container/_in_container_utils.sh"
}

@test "test success exit" {
    container_utils::in_container_script_end 1
    assert_success
}

@test "test hearbeat start and stop" {
    container_utils::start_output_heartbeat "Creating kubernetes cluster" 1
    container_utils::stop_output_heartbeat
    assert [ -z "$(ps -p $HEARTBEAT_PID -o pid=)" ] && [ -n "$HEARTBEAT_PID" ]
}

@test "test not in_container" {
    run container_utils::assert_in_container
    assert [ $status -eq 1 ]
}

@test "test install_airflow" {
    container_utils::install_released_airflow_version "1.10.2"
    airflow_version=$(airflow version)
    assert [ airflow_version=="1.10.2" ]
}
