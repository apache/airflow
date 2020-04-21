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
export FORCE_ANSWER_TO_QUESTIONS=quit

# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

function refresh_pylint_todo() {
    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --env PYTHONDONTWRITEBYTECODE \
        --env VERBOSE \
        --env VERBOSE_COMMANDS \
        --env HOST_USER_ID="$(id -ur)" \
        --env HOST_GROUP_ID="$(id -gr)" \
        --rm \
        "${AIRFLOW_CI_IMAGE}" \
        /opt/airflow/scripts/ci/in_container/refresh_pylint_todo.sh \
        | tee -a "${OUTPUT_LOG}"
}

prepare_ci_build

rebuild_ci_image_if_needed

refresh_pylint_todo
