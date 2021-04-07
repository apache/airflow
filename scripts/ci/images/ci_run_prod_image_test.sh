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

initialization::set_output_color_variables

job_name=$1
file=$2

parallel::get_parallel_job_status_file "${job_name}"
traps::add_trap "parallel::save_status" EXIT HUP INT TERM

echo
echo "${COLOR_BLUE}Running prod image build test ${job_name} via: ${file}.${COLOR_RESET}"
echo

set +e

if [[ ${file} == *".sh" ]]; then
    bash "${file}"
    res=$?
elif [[ ${file} == *"Dockerfile" ]]; then
    cd "$(dirname "${file}")" || exit 1
    docker build . --tag "${job_name}"
    res=$?
    docker rmi --force "${job_name}"
else
    echo "Bad file ${file}. Should be either a Dockerfile or script"
    exit 1
fi

echo

if [[ ${res} == "0" ]]; then
    echo "${COLOR_GREEN}Extend PROD image test ${job_name} succeeded${COLOR_RESET}"
else
    echo "${COLOR_RED}Extend PROD image test ${job_name} failed${COLOR_RESET}"
fi

exit ${res}
