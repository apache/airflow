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


function docker_engine_resources::print_overall_stats() {
    docker stats --all --no-stream
    echo
    docker run --rm --entrypoint /bin/sh "alpine:latest" -c "free -m"
    echo
    df -h \
        --exclude-type devtmpfs \
        --exclude-type overlay \
        --exclude-type squashfs \
        | grep -v " /run" \
        | grep -v " /sys" \
        | grep -v "/dev/shm" \
    || true
}

function docker_engine_resources::get_available_cpus_in_docker() {
    CPUS_AVAILABLE_FOR_DOCKER=$(docker run --rm "debian:bullseye-slim" grep -cE 'cpu[0-9]+' /proc/stat)
    export CPUS_AVAILABLE_FOR_DOCKER
}

function docker_engine_resources::get_available_memory_in_docker() {
    MEMORY_AVAILABLE_FOR_DOCKER=$(docker run --rm  --entrypoint /bin/bash "debian:bullseye-slim" -c 'echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / (1024 * 1024)))')
    export MEMORY_AVAILABLE_FOR_DOCKER
}

function docker_engine_resources::check_all_resources() {
    docker_v run -t "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/bin/bash"  \
        "${AIRFLOW_CI_IMAGE_WITH_TAG}" \
        -c "python /opt/airflow/scripts/in_container/run_resource_check.py"
}
