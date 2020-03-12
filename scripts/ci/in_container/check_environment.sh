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
# Script to check licences for all code. Can be started from any working directory
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

export EXIT_CODE=0
export DISABLED_INTEGRATIONS=""

function check_integration {
    INTEGRATION_NAME=$1
    CALL=$2
    MAX_CHECK=${3:=1}

    ENV_VAR_NAME=INTEGRATION_${INTEGRATION_NAME^^}
    if [[ ${!ENV_VAR_NAME:=} != "true" ]]; then
        DISABLED_INTEGRATIONS="${DISABLED_INTEGRATIONS} ${INTEGRATION_NAME}"
        return
    fi

    echo "-----------------------------------------------------------------------------------------------"
    echo "             Checking integration ${INTEGRATION_NAME}"
    echo "-----------------------------------------------------------------------------------------------"
    while true
    do
        set +e
        LAST_CHECK_RESULT=$(eval "${CALL}" 2>&1)
        RES=$?
        set -e
        if [[ ${RES} == 0 ]]; then
            echo
            echo "             Integration ${INTEGRATION_NAME} OK!"
            echo
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries while checking ${INTEGRATION_NAME} integration. Exiting"
            echo
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo "        ERROR: Integration ${INTEGRATION_NAME} could not be started!"
        echo
        echo "${LAST_CHECK_RESULT}"
        echo
        export EXIT_CODE=${RES}
    fi
    echo "-----------------------------------------------------------------------------------------------"
}

function check_db_connection {
    MAX_CHECK=${1:=3}

    if [[ ${BACKEND} == "postgres" ]]; then
        HOSTNAME=postgres
        PORT=5432
    elif [[ ${BACKEND} == "mysql" ]]; then
        HOSTNAME=mysql
        PORT=3306
    else
        return
    fi
    echo "-----------------------------------------------------------------------------------------------"
    echo "             Checking DB ${BACKEND}"
    echo "-----------------------------------------------------------------------------------------------"
    while true
    do
        set +e
        LAST_CHECK_RESULT=$(nc -zvv ${HOSTNAME} ${PORT} 2>&1)
        RES=$?
        set -e
        if [[ ${RES} == 0 ]]; then
            echo
            echo "             Backend ${BACKEND} OK!"
            echo
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries while checking ${BACKEND} db. Exiting"
            echo
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo "        ERROR: ${BACKEND} db could not be reached!"
        echo
        echo "${LAST_CHECK_RESULT}"
        echo
        export EXIT_CODE=${RES}
    fi
    echo "-----------------------------------------------------------------------------------------------"
}

function check_mysql_logs {
    MAX_CHECK=${1:=60}
    # Wait until mysql is ready!
    MYSQL_CONTAINER=$(docker ps -qf "name=mysql")
    if [[ -z ${MYSQL_CONTAINER} ]]; then
        echo
        echo "ERROR! MYSQL container is not started. Exiting!"
        echo
        exit 1
    fi
    echo
    echo "Checking if MySQL is ready for connections (double restarts in the logs)"
    echo
    while true
    do
        CONNECTION_READY_MESSAGES=$(docker logs "${MYSQL_CONTAINER}" 2>&1 | \
            grep -c "mysqld: ready for connections" )
        # MySQL when starting from dockerfile starts a temporary server first because it
        # starts with an empty database first and it will create the airflow database and then
        # it will start a second server to serve this newly created database
        # That's why we should wait until docker logs contain "ready for connections" twice
        # more info: https://github.com/docker-library/mysql/issues/527
        if [[ ${CONNECTION_READY_MESSAGES} -gt 1 ]];
        then
            echo
            echo
            echo "MySQL is ready for connections!"
            echo
            break
        else
            echo -n "."
        fi
        MAX_CHECK=$((MAX_CHECK-1))
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries while waiting for MySQL. Exiting"
            echo
            echo "Last check: ${CONNECTION_READY_MESSAGES} connection ready messages (expected >=2)"
            echo
            echo "==============================================================================================="
            echo
            exit 1
        else
            sleep 1
        fi
    done
}

if [[ -n ${BACKEND:=} ]]; then
    echo "==============================================================================================="
    echo "             Checking backend: ${BACKEND}"
    echo "==============================================================================================="

    set +e
    if [[ ${BACKEND} == "mysql" ]]; then
        check_mysql_logs 60
    fi

    check_db_connection 5

    set -e
    if [[ ${EXIT_CODE} == 0 ]]; then
        echo "==============================================================================================="
        echo "             Backend database is sane"
        echo "==============================================================================================="
        echo
    fi
else
    echo "==============================================================================================="
    echo "             Skip checking backend - BACKEND not set"
    echo "==============================================================================================="
    echo
fi

check_integration kerberos "nc -zvv kerberos 88" 30
check_integration mongo "nc -zvv mongo 27017" 20
check_integration redis "nc -zvv redis 6379" 20
check_integration rabbitmq "nc -zvv rabbitmq 5672" 20
check_integration cassandra "nc -zvv cassandra 9042" 20
check_integration openldap "nc -zvv openldap 389" 20

if [[ ${EXIT_CODE} != 0 ]]; then
    echo
    echo "Error: some of the CI environment failed to initialize!"
    echo
    exit ${EXIT_CODE}
fi

if [[ ${DISABLED_INTEGRATIONS} != "" ]]; then
    echo
    echo "Disabled integrations:${DISABLED_INTEGRATIONS}"
    echo
    echo "Enable them via --integration <INTEGRATION_NAME> flags (you can use 'all' for all)"
    echo
fi
