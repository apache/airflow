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

#
# Bash sanity settings (error on exit, complain for undefined vars, error when pipe fails)
set -euo pipefail

MY_DIR=$(cd "$(dirname "$0")" || exit 1; pwd)

if [[ ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
    set -x
fi

# shellcheck source=scripts/ci/in_container/_in_container_utils.sh
. "${MY_DIR}/_in_container_utils.sh"

in_container_basic_sanity_check

in_container_script_start

TRAVIS=${TRAVIS:=}

AIRFLOW_SOURCES=$(cd "${MY_DIR}/../../.." || exit 1; pwd)

PYTHON_VERSION=${PYTHON_VERSION:=3.6}
ENV=${ENV:=docker}
BACKEND=${BACKEND:=sqlite}
KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}
KUBERNETES_MODE=${KUBERNETES_MODE:=""}

export PYTHONPATH="${AIRFLOW_SOURCES}"

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

if [[ -z "${AIRFLOW_SOURCES:=}" ]]; then
    echo >&2
    echo >&2 AIRFLOW_SOURCES not set !!!!
    echo >&2
    exit 1
fi

echo
echo "Airflow home: ${AIRFLOW_HOME}"
echo "Airflow sources: ${AIRFLOW_SOURCES}"
echo "Airflow core SQL connection: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}"
echo

CLEAN_FILES=${CLEAN_FILES:=false}

if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www_rbac/node_modules" && "${CLEAN_FILES}" == "false" ]]; then
    echo
    echo "Installing NPM modules as they are not yet installed (sources are mounted from the host)"
    echo
    pushd "${AIRFLOW_SOURCES}/airflow/www_rbac/"
    npm ci
    echo
    popd
fi
if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www_rbac/static/dist" && ${CLEAN_FILES} == "false" ]]; then
    pushd "${AIRFLOW_SOURCES}/airflow/www_rbac/"
    echo
    echo "Building production version of javascript files (sources are mounted from the host)"
    echo
    echo
    npm run prod
    echo
    echo
    popd
fi

ARGS=( "$@" )

RUN_TESTS=${RUN_TESTS:="true"}

if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www_rbac/node_modules" ]]; then
    echo
    echo "Installing NPM modules as they are not yet installed (Sources mounted from Host)"
    echo
    pushd "${AIRFLOW_SOURCES}/airflow/www_rbac/" &>/dev/null || exit 1
    npm ci
    echo
    popd &>/dev/null || exit 1
fi
if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www_rbac/static/dist" ]]; then
    pushd "${AIRFLOW_SOURCES}/airflow/www_rbac/" &>/dev/null || exit 1
    echo
    echo "Building production version of javascript files (Sources mounted from Host)"
    echo
    echo
    npm run prod
    echo
    echo
    popd &>/dev/null || exit 1
fi

export HADOOP_DISTRO="${HADOOP_DISTRO:="cdh"}"
export HADOOP_HOME="${HADOOP_HOME:="/opt/hadoop-cdh"}"

if [[ ${AIRFLOW_CI_VERBOSE} == "true" ]]; then
    echo
    echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"
    echo
fi

export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_SOURCES}/tests/dags"

# Added to have run-tests on path
export PATH=${PATH}:${AIRFLOW_SOURCES}

export AIRFLOW__CORE__UNIT_TEST_MODE=True

# Make sure all AWS API calls default to the us-east-1 region
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:='us-east-1'}

# Fix codecov build path
# TODO: Check this - this should be made travis-independent
if [[ ! -h /home/travis/build/apache/airflow ]]; then
  sudo mkdir -p /home/travis/build/apache
  sudo ln -s "${AIRFLOW_SOURCES}" /home/travis/build/apache/airflow
fi

# Cleanup the logs, tmp when entering the environment
sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
mkdir -p "${AIRFLOW_SOURCES}"/logs/
mkdir -p "${AIRFLOW_SOURCES}"/tmp/

if [[ "${ENV}" == "docker" ]]; then
    # Start MiniCluster
    java -cp "/opt/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster \
        >"${AIRFLOW_HOME}/logs/minicluster.log" 2>&1 &

    # Set up ssh keys
    echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -P '' -f ~/.ssh/id_rsa \
        >"${AIRFLOW_HOME}/logs/ssh-keygen.log" 2>&1

    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    ln -s -f ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
    chmod 600 ~/.ssh/*

    # SSH Service
    sudo service ssh restart >/dev/null 2>&1

    # Setting up kerberos

    FQDN=$(hostname)
    ADMIN="admin"
    PASS="airflow"
    KRB5_KTNAME=/etc/airflow.keytab

    if [[ ${AIRFLOW_CI_VERBOSE} == "true" ]]; then
        echo
        echo "Hosts:"
        echo
        cat /etc/hosts
        echo
        echo "Hostname: ${FQDN}"
        echo
    fi

    sudo cp "${MY_DIR}/krb5/krb5.conf" /etc/krb5.conf

    set +e
    echo -e "${PASS}\n${PASS}" | \
        sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "addprinc -randkey airflow/${FQDN}" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs/kadmin_1.log" >/dev/null
    RES_1=$?

    sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "ktadd -k ${KRB5_KTNAME} airflow" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs/kadmin_2.log" >/dev/null
    RES_2=$?

    sudo kadmin -p "${ADMIN}/admin" -w "${PASS}" -q "ktadd -k ${KRB5_KTNAME} airflow/${FQDN}" 2>&1 \
          | sudo tee "${AIRFLOW_HOME}/logs/kadmin_3.log" >/dev/null
    RES_3=$?
    set -e

    if [[ ${RES_1} != 0 || ${RES_2} != 0 || ${RES_3} != 0 ]]; then
        echo
        echo "ERROR:  There was a problem communicating with kerberos"
        echo "Errors produced by kadmin commands are in : ${AIRFLOW_HOME}/logs/kadmin*.log"
        echo
        echo "Action! Please restart the environment!"
        echo "Run './scripts/ci/local_ci_stop_environment.sh' and re-enter the environment"
        echo
        exit 1
    fi

    sudo chmod 0644 "${KRB5_KTNAME}"
fi

# Exporting XUNIT_FILE so that we can see summary of failed tests
# at the end of the log
export XUNIT_FILE="${AIRFLOW_HOME}/logs/all_tests.xml"
mkdir -pv "${AIRFLOW_HOME}/logs/"

cp -f "${MY_DIR}/airflow_ci.cfg" "${AIRFLOW_HOME}/unittests.cfg"

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

KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}

if [[ "${TRAVIS}" == "true" ]]; then
    TRAVIS_ARGS=(
        "--junitxml=${XUNIT_FILE}"
        "--verbosity=0"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=html:airflow/www_rbac/static/coverage/"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        )
else
    TRAVIS_ARGS=()
fi


if [[ -z "${KUBERNETES_VERSION}" ]]; then
    ARGS=("${TRAVIS_ARGS[@]}" "tests/")
    "${MY_DIR}/run_ci_tests.sh" "${ARGS[@]}"
else
    export SKIP_INIT_DB=true
    echo "Set up Kubernetes cluster for tests"
    "${MY_DIR}/../kubernetes/setup_kubernetes.sh"
    "${MY_DIR}/../kubernetes/app/deploy_app.sh" -d "${KUBERNETES_MODE}"

    ARGS=("${TRAVIS_ARGS[@]}" "tests/integration/kubernetes")
    "${MY_DIR}/run_ci_tests.sh" "${ARGS[@]}"
fi

in_container_script_end
