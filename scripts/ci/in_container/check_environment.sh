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

EXIT_CODE=0

DISABLED_INTEGRATIONS=""

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
        EXIT_CODE=${RES}
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
        EXIT_CODE=${RES}
    fi
    echo "-----------------------------------------------------------------------------------------------"
}

function resetdb_if_requested() {
    if [[ ${DB_RESET:="false"} == "true" ]]; then
        if [[ ${RUN_AIRFLOW_1_10} == "true" ]]; then
                airflow resetdb -y
        else
                airflow db reset -y
        fi
    fi
    return $?
}

if [[ -n ${BACKEND:=} ]]; then
    echo "==============================================================================================="
    echo "             Checking backend: ${BACKEND}"
    echo "==============================================================================================="

    set +e
    check_db_connection 20
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
    # Fixed exit code on initialization
    # If the environment fails to initialize it is re-started several times
    exit 254
fi

resetdb_if_requested

if [[ ${DISABLED_INTEGRATIONS} != "" ]]; then
    echo
    echo "Disabled integrations:${DISABLED_INTEGRATIONS}"
    echo
    echo "Enable them via --integration <INTEGRATION_NAME> flags (you can use 'all' for all)"
    echo
fi

exit 0
