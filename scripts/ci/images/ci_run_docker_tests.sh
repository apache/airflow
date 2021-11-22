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

interactive="false"
initialize_only="false"
declare -a tests_to_run
declare -a pytest_args

tests_to_run=()

function parse_tests_to_run() {
    if [[ $# != 0 ]]; then
        if [[ $1 == "--help" || $1 == "-h" ]]; then
            echo
            echo "Running Docker tests"
            echo
            echo "    $0 TEST [TEST ...]      - runs tests (from docker_tests folder)"
            echo "    $0 [-i|--interactive]   - Activates virtual environment ready to run tests and drops you in"
            echo "    $0 [--initialize]       - Initialize virtual environment and exit"
            echo "    $0 [--help]             - Prints this help message"
            echo
            exit
        elif [[ $1 == "--interactive" || $1 == "-i" ]]; then
            echo
            echo "Entering interactive environment for docker testing"
            echo
            interactive="true"
        elif [[ $1 == "--initialize" ]]; then
            echo
            echo "Initializing environment for docker testing"
            echo
            initialize_only="true"
        else
            tests_to_run=("${@}")
        fi
        pytest_args=(
            "--pythonwarnings=ignore::DeprecationWarning"
            "--pythonwarnings=ignore::PendingDeprecationWarning"
            "-n" "auto"
        )
    else
        echo "You must select the tests to run."
        exit 1
    fi
}

function create_virtualenv() {
    HOST_PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
    readonly HOST_PYTHON_VERSION

    local virtualenv_path="${BUILD_CACHE_DIR}/.docker_venv/host_python_${HOST_PYTHON_VERSION}"

    mkdir -pv "${BUILD_CACHE_DIR}/.docker_venv/"
    if [[ ! -d ${virtualenv_path} ]]; then
        echo
        echo "Creating virtualenv at ${virtualenv_path}"
        echo
        python3 -m venv "${virtualenv_path}"
    fi

    . "${virtualenv_path}/bin/activate"

    pip install --upgrade "pip==${AIRFLOW_PIP_VERSION}" "wheel==${WHEEL_VERSION}"

    local constraints=(
        --constraint
        "https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-${HOST_PYTHON_VERSION}.txt"
    )
    if [[ -n ${GITHUB_REGISTRY_PULL_IMAGE_TAG=} ]]; then
        # Disable constraints when building in CI with specific version of sources
        # In case there will be conflicting constraints
        constraints=()
    fi

    pip install pytest pytest-xdist "${constraints[@]}"
}

function run_tests() {
    pytest "${pytest_args[@]}" "${tests_to_run[@]}"
}

cd "${AIRFLOW_SOURCES}" || exit 1

set +u
parse_tests_to_run "${@}"
set -u

create_virtualenv

if [[ ${interactive} == "true" ]]; then
    echo
    echo "Activating the virtual environment for docker testing"
    echo
    echo "You can run testing via 'pytest docker_tests/....'"
    echo "You can add -s to see the output of your tests on screen"
    echo
    echo "You are entering the virtualenv now. Type exit to exit back to the original shell"
    echo
    exec "${SHELL}"
elif [[ ${initialize_only} == "true" ]]; then
    exit 0
else
    run_tests
fi
