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
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

if [[ ${#@} == "0" ]]; then
    print_in_container_info
    print_in_container_info "Running flake8 with no parameters"
    print_in_container_info
else
    print_in_container_info
    print_in_container_info "Running flake8 with parameters: $*"
    print_in_container_info
fi

set +e

flake8 "$@"

RES="$?"

set -e

if [[ "${RES}" != 0 ]]; then
    echo >&2
    echo >&2 "There were some flake8 errors. Exiting"
    echo >&2
    exit 1
else
    echo
    echo "Flake8 check succeeded"
    echo
fi
