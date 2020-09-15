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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

EXIT_CODE=0

DISABLED_INTEGRATIONS=""

#######################################################################################################
#
# Checks the status of a service
#
# Arguments:
#   Service name
#   Call, command to check the service status
#   Max Checks
#
# Returns:
#   None
#
#######################################################################################################
function check_environment::check_service {
    INTEGRATION_NAME=$1
    CALL=$2
    MAX_CHECK=${3:=1}

    echo -n "${INTEGRATION_NAME}: "
    while true
    do
        set +e
        LAST_CHECK_RESULT=$(eval "${CALL}" 2>&1)
        RES=$?
        set -e
        if [[ ${RES} == 0 ]]; then
            echo -e " \e[32mOK.\e[0m"
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo -e " \e[31mERROR!\e[0m"
            echo "Maximum number of retries while checking service. Exiting"
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo "Service could not be started!"
        echo
        echo "$ ${CALL}"
        echo "${LAST_CHECK_RESULT}"
        echo
        EXIT_CODE=${RES}
    fi
}

#######################################################################################################
#
# Checks the status of an integration service
#
# Arguments:
#   Integration name
#   Call, command to check the service status
#   Max Checks
#
# Used globals:
#   DISABLED_INTEGRATIONS
#
# Returns:
#   None
#
#######################################################################################################
function check_environment::check_integration {
    local integration_label=$1
    local integration_name=$2
    local call=$3
    local max_check=${4:=1}

    local env_var_name=INTEGRATION_${integration_name^^}
    if [[ ${!env_var_name:=} != "true" ]]; then
        DISABLED_INTEGRATIONS="${DISABLED_INTEGRATIONS} ${integration_name}"
        return
    fi
    check_service "${INTEGRATION_LABEL}" "${CALL}" "${MAX_CHECK}"
}

#######################################################################################################
#
# Status check for different db backends
#
# Arguments:
#   Max Checks
#
# Used globals:
#   BACKEND
#
# Returns:
#   None for success and 1 for error.
#
#######################################################################################################
function check_environment::check_db_backend {
    MAX_CHECK=${1:=1}

    if [[ ${BACKEND} == "postgres" ]]; then
        check_environment::check_service "PostgresSQL" "nc -zvv postgres 5432" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "mysql" ]]; then
        check_environment::check_service "MySQL" "nc -zvv mysql 3306" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "sqlite" ]]; then
        return
    else
        echo "Unknown backend. Supported values: [postgres,mysql,sqlite]. Current value: [${BACKEND}]"
        exit 1
    fi
}

#######################################################################################################
#
# Resets the Airflow's meta db.
#
# Used globals:
#   DB_RESET
#   RUN_AIRFLOW_1_10
#
# Returns:
#   0 if the db is reset, non-zero on error.
#######################################################################################################
function check_environment::resetdb_if_requested() {
    if [[ ${DB_RESET:="false"} == "true" ]]; then
        if [[ ${RUN_AIRFLOW_1_10} == "true" ]]; then
            airflow resetdb -y
        else
            airflow db reset -y
        fi
    fi
    return $?
}

function startairflow_if_requested() {
    if [[ ${START_AIRFLOW:="false"} == "true" ]]; then


        export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=${LOAD_DEFAULT_CONNECTIONS}
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}

        . "$( dirname "${BASH_SOURCE[0]}" )/configure_environment.sh"

        # initialize db and create the admin user if it's a new run
        if [[ ${RUN_AIRFLOW_1_10} == "true" ]]; then
            airflow initdb
            airflow create_user -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email || true
        else
            airflow db init
            airflow users create -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email
        fi

        . "$( dirname "${BASH_SOURCE[0]}" )/run_init_script.sh"

    fi
    return $?
}

echo "==============================================================================================="
echo "             Checking integrations and backends"
echo "==============================================================================================="
if [[ -n ${BACKEND=} ]]; then
    check_environment::check_db_backend 20
    echo "-----------------------------------------------------------------------------------------------"
fi
check_environment::check_integration "Kerberos" "kerberos" "nc -zvv kdc-server-example-com 88" 30
check_environment::check_integration "MongoDB" "mongo" "nc -zvv mongo 27017" 20
check_environment::check_integration "Redis" "redis" "nc -zvv redis 6379" 20
check_environment::check_integration "RabbitMQ" "rabbitmq" "nc -zvv rabbitmq 5672" 20
check_environment::check_integration "Cassandra" "cassandra" "nc -zvv cassandra 9042" 20
check_environment::check_integration "OpenLDAP" "openldap" "nc -zvv openldap 389" 20
check_environment::check_integration "Presto (HTTP)" "presto" "nc -zvv presto 8080" 40
check_environment::check_integration "Presto (HTTPS)" "presto" "nc -zvv presto 7778" 40
check_environment::check_integration "Presto (API)" "presto" \
    "curl --max-time 1 http://presto:8080/v1/info/ | grep '\"starting\":false'" 20
echo "-----------------------------------------------------------------------------------------------"

if [[ ${EXIT_CODE} != 0 ]]; then
    echo
    echo "Error: some of the CI environment failed to initialize!"
    echo
    # Fixed exit code on initialization
    # If the environment fails to initialize it is re-started several times
    exit 254
fi

resetdb_if_requested
startairflow_if_requested

if [[ -n ${DISABLED_INTEGRATIONS=} ]]; then
    echo
    echo "Disabled integrations:${DISABLED_INTEGRATIONS}"
    echo
    echo "Enable them via --integration <INTEGRATION_NAME> flags (you can use 'all' for all)"
    echo
fi

exit 0
