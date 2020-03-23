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
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

TRAVIS=${TRAVIS:=}

AIRFLOW_SOURCES=$(cd "${MY_DIR}/../../.." || exit 1; pwd)

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.6}
BACKEND=${BACKEND:=sqlite}
KUBERNETES_MODE=${KUBERNETES_MODE:=""}
KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}
ENABLE_KIND_CLUSTER=${ENABLE_KIND_CLUSTER:="false"}
RUNTIME=${RUNTIME:=""}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

if [[ -z ${AIRFLOW_SOURCES:=} ]]; then
    echo >&2
    echo >&2 AIRFLOW_SOURCES not set !!!!
    echo >&2
    exit 1
fi

echo
echo "Airflow home: ${AIRFLOW_HOME}"
echo "Airflow sources: ${AIRFLOW_SOURCES}"
echo "Airflow core SQL connection: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}"
if [[ -n "${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS:=}" ]]; then
    echo "Airflow collation for IDs: ${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS}"
fi

echo

ARGS=( "$@" )

RUN_TESTS=${RUN_TESTS:="true"}
INSTALL_AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION:=""}"

if [[ ${AIRFLOW_VERSION} == *1.10* || ${INSTALL_AIRFLOW_VERSION} == *1.10* ]]; then
    export RUN_AIRFLOW_1_10="true"
else
    export RUN_AIRFLOW_1_10="false"
fi

if [[ ${INSTALL_AIRFLOW_VERSION} == "current" ]]; then
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

export HADOOP_DISTRO="${HADOOP_DISTRO:="cdh"}"
export HADOOP_HOME="${HADOOP_HOME:="/opt/hadoop-cdh"}"

if [[ ${VERBOSE} == "true" ]]; then
    echo
    echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"
    echo
fi

# Added to have run-tests on path
export PATH=${PATH}:${AIRFLOW_SOURCES}

# This is now set in conftest.py - only for pytest tests
unset AIRFLOW__CORE__UNIT_TEST_MODE

# Fix codecov build path
# TODO: Check this - this should be made travis-independent
if [[ ! -h /home/travis/build/apache/airflow ]]; then
  sudo mkdir -p /home/travis/build/apache
  sudo ln -s "${AIRFLOW_SOURCES}" /home/travis/build/apache/airflow
fi

mkdir -pv "${AIRFLOW_HOME}/logs/"
cp -f "${MY_DIR}/airflow_ci.cfg" "${AIRFLOW_HOME}/unittests.cfg"


"${MY_DIR}/check_environment.sh"

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


if [[ "${RUNTIME}" == "" ]]; then
    # Start MiniCluster
    java -cp "/opt/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster \
        >"${AIRFLOW_HOME}/logs/minicluster.log" 2>&1 &

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
fi


export KIND_CLUSTER_OPERATION="${KIND_CLUSTER_OPERATION:="start"}"
export KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}

if [[ ${RUNTIME:=""} == "kubernetes" ]]; then
    unset KRB5_CONFIG
    unset KRB5_KTNAME
    export AIRFLOW_KUBERNETES_IMAGE=${AIRFLOW_CI_IMAGE}-kubernetes
    AIRFLOW_KUBERNETES_IMAGE_NAME=$(echo "${AIRFLOW_KUBERNETES_IMAGE}" | cut -f 1 -d ":")
    export AIRFLOW_KUBERNETES_IMAGE_NAME
    AIRFLOW_KUBERNETES_IMAGE_TAG=$(echo "${AIRFLOW_KUBERNETES_IMAGE}" | cut -f 2 -d ":")
    export AIRFLOW_KUBERNETES_IMAGE_TAG
fi

if [[ "${ENABLE_KIND_CLUSTER}" == "true" ]]; then
    export CLUSTER_NAME="airflow-python-${PYTHON_MAJOR_MINOR_VERSION}-${KUBERNETES_VERSION}"
    "${MY_DIR}/kubernetes/setup_kind_cluster.sh"
    if [[ ${KIND_CLUSTER_OPERATION} == "stop" ]]; then
        exit 1
    fi
fi

# shellcheck source=scripts/ci/in_container/configure_environment.sh
. "${MY_DIR}/configure_environment.sh"

set +u
# If we do not want to run tests, we simply drop into bash
if [[ "${RUN_TESTS}" == "false" ]]; then
    if [[ ${#ARGS} == 0 ]]; then
        exec /bin/bash
    else
        exec /bin/bash -c "$(printf "%q " "${ARGS[@]}")"
    fi
fi

set -u

if [[ "${TRAVIS}" == "true" ]]; then
    CI_ARGS=(
        "--verbosity=0"
        "--strict-markers"
        "--instafail"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=html:airflow/www/static/coverage/"
        "--maxfail=50"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        )
else
    CI_ARGS=()
fi

if [[ -n ${RUN_INTEGRATION_TESTS:=""} ]]; then
    CI_ARGS+=("--integrations" "${RUN_INTEGRATION_TESTS}" "-rpfExX")
fi

TESTS_TO_RUN="tests/"

if [[ ${#@} -gt 0 && -n "$1" ]]; then
    TESTS_TO_RUN="$1"
fi

if [[ -n ${RUNTIME} ]]; then
    CI_ARGS+=("--runtime" "${RUNTIME}" "-rpfExX")
    TESTS_TO_RUN="tests/runtime"
    if [[ ${RUNTIME} == "kubernetes" ]]; then
        export SKIP_INIT_DB=true
        "${MY_DIR}/deploy_airflow_to_kubernetes.sh"
    fi
fi


ARGS=("${CI_ARGS[@]}" "${TESTS_TO_RUN}")

if [[ ${RUN_SYSTEM_TESTS:="false"} == "true" ]]; then
    "${MY_DIR}/run_system_tests.sh" "${ARGS[@]}"
else
    "${MY_DIR}/run_ci_tests.sh" "${ARGS[@]}"
fi
