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
BACKEND=${BACKEND:=sqlite}
KUBERNETES_MODE=${KUBERNETES_MODE:=""}
KUBERNETES_VERSION=${KUBERNETES_VERSION:=""}
RECREATE_KIND_CLUSTER=${RECREATE_KIND_CLUSTER:="true"}
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
echo

ARGS=( "$@" )

RUN_TESTS=${RUN_TESTS:="true"}

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

    if [[ ${DEPS:="true"} == "true" ]]; then
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
              | sudo tee "${AIRFLOW_HOME}/logs``/kadmin_3.log" >/dev/null
        RES_3=$?
        set -e

        if [[ ${RES_1} != 0 || ${RES_2} != 0 || ${RES_3} != 0 ]]; then
            if [[ -n ${KRB5_CONFIG:=} ]]; then
                echo
                echo "ERROR !!!!Kerberos initialisation requested, but failed"
                echo
                echo "I will exit now, and you need to run 'breeze --stop-environment' to kill kerberos."
                echo
                echo "Then you can again run 'breeze --integration kerberos' to start it again"
                echo
            fi
            echo
            echo "No kerberos. If you want to start it, exit and run 'breeze --integration kerberos'"
            echo
        else
            echo
            echo "Kerberos enabled and working."
            echo
            sudo chmod 0644 "${KRB5_KTNAME}"
        fi

    fi
fi

mkdir -pv "${AIRFLOW_HOME}/logs/"

cp -f "${MY_DIR}/airflow_ci.cfg" "${AIRFLOW_HOME}/unittests.cfg"

export KIND_CLUSTER_OPERATION="${KIND_CLUSTER_OPERATION:="start"}"

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
    export CLUSTER_NAME="airflow-python-${PYTHON_VERSION}-${KUBERNETES_VERSION}"
    "${MY_DIR}/kubernetes/setup_kind_cluster.sh"
    if [[ ${KIND_CLUSTER_OPERATION} == "stop" ]]; then
        exit 1
    fi
fi

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
    CI_ARGS=(
        "--verbosity=0"
        "--strict-markers"
        "--instafail"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=html:airflow/www/static/coverage/"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        )
else
    CI_ARGS=()
fi

if [[ -n ${RUN_INTEGRATION_TESTS:=""} ]]; then
    CI_ARGS+=("--integrations" "${RUN_INTEGRATION_TESTS}" "-rpfExX")
fi

TEST_DIR="tests/"

if [[ -n ${RUNTIME} ]]; then
    CI_ARGS+=("--runtime" "${RUNTIME}" "-rpfExX")
    TEST_DIR="tests/runtime"
    if [[ ${RUNTIME} == "kubernetes" ]]; then
        export SKIP_INIT_DB=true
        "${MY_DIR}/deploy_airflow_to_kubernetes.sh"
    fi
fi

export PYTHONPATH=${AIRFLOW_SOURCES}

ARGS=("${CI_ARGS[@]}" "${TEST_DIR}")
"${MY_DIR}/run_ci_tests.sh" "${ARGS[@]}"

in_container_script_end
