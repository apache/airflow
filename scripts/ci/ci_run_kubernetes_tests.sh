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
# shellcheck source=scripts/ci/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_script_init.sh"

# adding trap to exiting trap
HANDLERS="$( trap -p EXIT | cut -f2 -d \' )"
# shellcheck disable=SC2064
trap "${HANDLERS}${HANDLERS:+;}dump_kind_logs" EXIT

INTERACTIVE="false"

declare -a TESTS
declare -a PYTEST_ARGS

TESTS=()

if [[ $# != 0 ]]; then
    if [[ $1 == "--help" || $1 == "-h" ]]; then
        echo
        echo "Running kubernetes tests"
        echo
        echo "    $0                      - runs all kubernetes tests"
        echo "    $0 TEST [TEST ...]      - runs selected kubernetes tests (from kubernetes_tests folder)"
        echo "    $0 [-i|--interactive]   - Activates virtual environment ready to run tests and drops you in"
        echo "    $0 [--help]             - Prints this help message"
        echo
        exit
    elif [[ $1 == "--interactive" || $1 == "-i" ]]; then
        echo
        echo "Entering interactive environment for kubernetes testing"
        echo
        INTERACTIVE="true"
    else
        TESTS=("${@}")
    fi
    PYTEST_ARGS=(
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
    )
else
    TESTS=("kubernetes_tests")
    PYTEST_ARGS=(
        "--verbosity=1"
        "--strict-markers"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=html:airflow/www/static/coverage/"
        "--color=yes"
        "--maxfail=50"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        )

fi

get_environment_for_builds_on_ci
initialize_kind_variables

cd "${AIRFLOW_SOURCES}" || exit 1

VIRTUALENV_PATH="${BUILD_CACHE_DIR}/.kubernetes_venv"

if [[ ! -d ${VIRTUALENV_PATH} ]]; then
    echo
    echo "Creating virtualenv at ${VIRTUALENV_PATH}"
    echo
    python -m venv "${VIRTUALENV_PATH}"
fi

. "${VIRTUALENV_PATH}/bin/activate"

pip install pytest freezegun pytest-cov \
    --constraint "requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"

pip install -e ".[kubernetes]" \
    --constraint "requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"

if [[ ${INTERACTIVE} == "true" ]]; then
    echo
    echo "Activating the virtual environment for kubernetes testing"
    echo
    echo "You can run kubernetes testing via 'pytest kubernetes_tests/....'"
    echo "You can add -s to see the output of your tests on screen"
    echo
    echo "The webserver is available at http://localhost:30809/"
    echo
    echo "User/password: admin/admin"
    echo
    echo "You are entering the virtualenv now. Type exit to exit back to the original shell"
    echo
    kubectl config set-context --current --namespace=airflow
    exec "${SHELL}"
else
    pytest "${PYTEST_ARGS[@]}" "${TESTS[@]}"
fi
