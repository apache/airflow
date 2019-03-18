#!/usr/bin/env bash

#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -euo pipefail

MY_DIR=$(cd "$(dirname "$0")"; pwd)

if [[ ${AIRFLOW_CI_VERBOSE:="false"} == "true" ]]; then
    set -x
fi

AIRFLOW_ROOT="${MY_DIR}/../.."

PYTHON_VERSION=${PYTHON_VERSION:=3.6}
ENV=${ENV:=docker}
BACKEND=${BACKEND:=sqlite}
KUBERNETES_VERSION=${KUBERNETES_VERSION:=}

RUN_TESTS=${RUN_TESTS:="true"}

ARGS=$@

if [[ -z "${AIRFLOW_HOME:=}" ]]; then
    echo
    echo AIRFLOW_HOME not set !!!!
    echo
    exit 1
fi

# Fix file permissions
if [[ -d $HOME/.minikube ]]; then
    sudo chown -R airflow.airflow $HOME/.kube $HOME/.minikube
fi

if [[ ${PYTHON_VERSION} == 3* ]]; then
    PIP=pip3
else
    PIP=pip2
fi

if [[ "${ENV}" == "docker" ]]; then
    ${MY_DIR}/setup_env.sh
    ${MY_DIR}/setup_kdc.sh
fi

if [[ "${BACKEND}" == "mysql" ]]; then
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://root@mysql/airflow
    export AIRFLOW__CELERY__RESULT_BACKEND=db+mysql://root@mysql/airflow
    ${MY_DIR}/setup_mysql.sh >/dev/null
elif [[ "${BACKEND}" == "sqlite" ]]; then
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
    export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
elif [[ "${BACKEND}" == "postgres" ]]; then
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:airflow@postgres/airflow
    export AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://postgres:airflow@postgres/airflow
else
    echo
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "Wrong backend ${BACKEND}"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit 2
fi


if [[ -z ${KUBERNETES_VERSION} ]]; then
    if [[ "${RUN_TESTS}" == "true" ]]; then
        echo
        echo Running CI tests with ${ARGS}
        echo
        ${MY_DIR}/run_ci_tests.sh ${ARGS}
        codecov -e py${PYTHON_VERSION}-backend_${BACKEND}-env_${ENV}
    else
        if [[ -z "${ARGS}" ]]; then
            /bin/bash
        else
            /bin/bash -c "${ARGS}"
        fi
    fi
else
  # This script runs inside a container, the path of the kubernetes certificate
  # is /home/travis/.minikube/client.crt but the user in the container is `airflow`
  # TODO: Check this. This should be made travis-independent
  if [[ ! -d /home/travis ]]; then
    sudo mkdir -p /home/travis
  fi
  sudo ln -s /home/airflow/.minikube /home/travis/.minikube
    if [[ "${RUN_TESTS}" == "true" ]]; then
        echo
        echo Running CI tests with ${ARGS}
        echo
        ${MY_DIR}/run_ci_tests.sh tests.contrib.minikube \
                     --with-coverage \
                     --cover-erase \
                     --cover-html \
                     --cover-package=airflow \
                     --cover-html-dir=airflow/www/static/coverage \
                     --with-ignore-docstrings \
                     --rednose \
                     --with-timer \
                     -v \
                     --logging-level=INFO ${ARGS}
        codecov -e py${PYTHON_VERSION}-backend_${BACKEND}-env_${ENV}
    else
        if [[ -z "${ARGS}" ]]; then
            /bin/bash
        else
            /bin/bash -c "${ARGS}"
        fi
    fi
fi
