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
if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
    set -x
fi

# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. /opt/airflow/scripts/ci/in_container/_in_container_script_init.sh

AIRFLOW_SOURCES=$(cd "${IN_CONTAINER_DIR}/../../.." || exit 1; pwd)

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.6}
BACKEND=${BACKEND:=sqlite}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

: "${AIRFLOW_SOURCES:?"ERROR: AIRFLOW_SOURCES not set !!!!"}"

echo
echo "Airflow home: ${AIRFLOW_HOME}"
echo "Airflow sources: ${AIRFLOW_SOURCES}"
echo "Airflow core SQL connection: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}"
if [[ -n "${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS:=}" ]]; then
    echo "Airflow collation for IDs: ${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS}"
fi

echo

RUN_TESTS=${RUN_TESTS:="false"}
CI=${CI:="false"}
INSTALL_AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION:=""}"

if [[ ${CI} == "false" ]]; then
    # Create links for useful CLI tools
    # shellcheck source=scripts/ci/in_container/run_cli_tool.sh
    source <(bash scripts/ci/in_container/run_cli_tool.sh)
fi

if [[ ${AIRFLOW_VERSION} == *1.10* || ${INSTALL_AIRFLOW_VERSION} == *1.10* ]]; then
    export RUN_AIRFLOW_1_10="true"
else
    export RUN_AIRFLOW_1_10="false"
fi

if [[ ${INSTALL_AIRFLOW_VERSION} == "" ]]; then
    if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/node_modules" ]]; then
        echo
        echo "Installing node modules as they are not yet installed (Sources mounted from Host)"
        echo
        pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
        yarn install --frozen-lockfile
        echo
        popd &>/dev/null || exit 1
    fi
    if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/static/dist" ]]; then
        pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
        echo
        echo "Building production version of javascript files (Sources mounted from Host)"
        echo
        echo
        yarn run prod
        echo
        echo
        popd &>/dev/null || exit 1
    fi
    # Cleanup the logs, tmp when entering the environment
    sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
    sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
    mkdir -p "${AIRFLOW_SOURCES}"/logs/
    mkdir -p "${AIRFLOW_SOURCES}"/tmp/
    export PYTHONPATH=${AIRFLOW_SOURCES}
else
    install_released_airflow_version "${INSTALL_AIRFLOW_VERSION}"
fi


export RUN_AIRFLOW_1_10=${RUN_AIRFLOW_1_10:="false"}

# Added to have run-tests on path
export PATH=${PATH}:${AIRFLOW_SOURCES}

# This is now set in conftest.py - only for pytest tests
unset AIRFLOW__CORE__UNIT_TEST_MODE

mkdir -pv "${AIRFLOW_HOME}/logs/"
cp -f "${IN_CONTAINER_DIR}/airflow_ci.cfg" "${AIRFLOW_HOME}/unittests.cfg"

set +e
"${IN_CONTAINER_DIR}/check_environment.sh"
ENVIRONMENT_EXIT_CODE=$?
set -e
if [[ ${ENVIRONMENT_EXIT_CODE} != 0 ]]; then
    echo
    echo "Error: check_environment returned ${ENVIRONMENT_EXIT_CODE}. Exiting."
    echo
    exit ${ENVIRONMENT_EXIT_CODE}
fi


if [[ ${INTEGRATION_KERBEROS:="false"} == "true" ]]; then
    set +e
    setup_kerberos
    RES=$?
    set -e

    if [[ ${RES} != 0 ]]; then
        echo
        echo "ERROR !!!!Kerberos initialisation requested, but failed"
        echo
        echo "I will exit now, and you need to run 'breeze --integration kerberos restart'"
        echo "to re-enter breeze and restart kerberos."
        echo
        exit 1
    fi
fi


# Set up ssh keys
echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -m PEM -P '' -f ~/.ssh/id_rsa \
    >"${AIRFLOW_HOME}/logs/ssh-keygen.log" 2>&1

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ln -s -f ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
chmod 600 ~/.ssh/*

# SSH Service
sudo service ssh restart >/dev/null 2>&1

# Sometimes the server is not quick enough to load the keys!
while [[ $(ssh-keyscan -H localhost 2>/dev/null | wc -l) != "3" ]] ; do
    echo "Not all keys yet loaded by the server"
    sleep 0.05
done

ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null

# shellcheck source=scripts/ci/in_container/configure_environment.sh
. "${IN_CONTAINER_DIR}/configure_environment.sh"

cd "${AIRFLOW_SOURCES}"

set +u
# If we do not want to run tests, we simply drop into bash
if [[ "${RUN_TESTS}" != "true" ]]; then
    exec /bin/bash "${@}"
fi
set -u

if [[ "${CI}" == "true" ]]; then
    EXTRA_PYTEST_ARGS=(
        "--verbosity=0"
        "--strict-markers"
        "--instafail"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=html:airflow/www/static/coverage/"
        "--color=yes"
        "--maxfail=50"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        )
else
    EXTRA_PYTEST_ARGS=()
fi

declare -a TESTS_TO_RUN
TESTS_TO_RUN=("tests")

if [[ ${#@} -gt 0 && -n "$1" ]]; then
    TESTS_TO_RUN=("${@}")
fi

if [[ -n ${RUN_INTEGRATION_TESTS:=""} ]]; then
    for INT in ${RUN_INTEGRATION_TESTS}
    do
        EXTRA_PYTEST_ARGS+=("--integration" "${INT}")
    done
    EXTRA_PYTEST_ARGS+=("-rpfExX")
elif [[ ${ONLY_RUN_LONG_RUNNING_TESTS:=""} == "true" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "-m" "long_running"
        "--include-long-running"
        "--verbosity=1"
        "--reruns" "3"
        "--timeout" "90")
elif [[ ${ONLY_RUN_QUARANTINED_TESTS:=""} == "true" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "-m" "quarantined"
        "--include-quarantined"
        "--verbosity=1"
        "--reruns" "3"
        "--timeout" "90")
fi

ARGS=("${EXTRA_PYTEST_ARGS[@]}" "${TESTS_TO_RUN[@]}")

if [[ ${RUN_SYSTEM_TESTS:="false"} == "true" ]]; then
    "${IN_CONTAINER_DIR}/run_system_tests.sh" "${ARGS[@]}"
else
    "${IN_CONTAINER_DIR}/run_ci_tests.sh" "${ARGS[@]}"
fi
