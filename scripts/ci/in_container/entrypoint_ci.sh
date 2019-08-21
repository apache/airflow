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

AIRFLOW_ROOT="${MY_DIR}/../../.."

PYTHON_VERSION=${PYTHON_VERSION:=3.6}
ENV=${ENV:=docker}
BACKEND=${BACKEND:=sqlite}
KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}
KUBERNETES_MODE=${KUBERNETES_MODE:=""}

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

ARGS=( "$@" )

RUN_TESTS=${RUN_TESTS:="true"}

if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/node_modules" ]]; then
    echo
    echo "Installing NPM modules as they are not yet installed (Sources mounted from Host)"
    echo
    pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
    npm ci
    echo
    popd &>/dev/null || exit 1
fi
if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/static/dist" ]]; then
    pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
    echo
    echo "Building production version of javascript files (Sources mounted from Host)"
    echo
    echo
    npm run prod
    echo
    echo
    popd &>/dev/null || exit 1
fi

if [[ ${AIRFLOW_CI_VERBOSE} == "true" ]]; then
    echo
    echo "Using ${HADOOP_DISTRO:=} distribution of Hadoop from ${HADOOP_HOME:=}"
    echo
fi

export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_SOURCES}/tests/dags"

# add test/test_utils to PYTHONPATH (TODO: Do we need it?)
export PYTHONPATH=${PYTHONPATH:-${AIRFLOW_SOURCES}/tests/test_utils}

# Added to have run-tests on path
export PATH=${PATH}:${AIRFLOW_SOURCES}

export AIRFLOW__CORE__UNIT_TEST_MODE=True
export HADOOP_DISTRO

# Fix codecov build path
# TODO: Check this - this should be made travis-independent
if [[ ! -h /home/travis/build/apache/airflow ]]; then
  sudo mkdir -p /home/travis/build/apache
  sudo ln -s "${AIRFLOW_ROOT}" /home/travis/build/apache/airflow
fi

# Fix file permissions
if [[ -d "${HOME}/.minikube" ]]; then
    sudo chown -R "${AIRFLOW_USER}.${AIRFLOW_USER}" "${HOME}/.kube" "${HOME}/.minikube"
fi

# Cleanup the logs, tmp when entering the environment
sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
mkdir -p "${AIRFLOW_SOURCES}"/logs/
mkdir -p "${AIRFLOW_SOURCES}"/tmp/

if [[ "${ENV}" == "docker" ]]; then
    # Start MiniCluster
    java -cp "/tmp/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster \
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
        echo "ERROR! There was a problem communicating with kerberos"
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

if [[ ${#ARGS} == 0 ]]; then
    ARGS=("--with-coverage"
          "--cover-erase"
          "--cover-html"
          "--cover-package=airflow"
          "--cover-html-dir=airflow/www/static/coverage"
          "--with-ignore-docstrings"
          "--rednose"
          "--with-xunit"
          "--xunit-file=${XUNIT_FILE}"
          "--with-timer"
          "-v"
          "--logging-level=INFO")
    echo
    echo "Running ALL Tests"
    echo
else
    echo
    echo "Running tests with ${ARGS[*]}"
    echo
fi
set -u

KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}

if [[ -z "${KUBERNETES_VERSION}" ]]; then
    echo
    echo "Running CI tests with ${ARGS[*]}"
    echo
    "${MY_DIR}/run_ci_tests.sh" "${ARGS[@]}"
else
    export KUBERNETES_VERSION
    export MINIKUBE_IP
    # This script runs inside a container, the path of the kubernetes certificate
    # is /home/travis/.minikube/client.crt but the user in the container is `root`
    # TODO: Check this. This should be made travis-independent :D
    if [[ ! -d /home/travis ]]; then
        sudo mkdir -p /home/travis
    fi
    sudo ln -s /root/.minikube /home/travis/.minikube
    echo
    echo "Running CI tests with ${ARGS[*]}"
    echo
    "${MY_DIR}/run_ci_tests.sh" tests.minikube "${ARGS[@]}"
fi

in_container_script_end
