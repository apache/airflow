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
EXIT_CODE=0

DISABLED_INTEGRATIONS=""

# We want to avoid misleading messages and perform only forward lookup of the service IP address.
# Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
# lookup name does not match the original name even if the host is reachable via IP. This happens
# randomly with docker-compose in GitHub Actions.
# Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
# And use the IP in NC and add '-n' switch to disable any DNS use.
# Even if this message might be harmless, it might hide the real reason for the problem
# Which is the long time needed to start some services, seeing this message might be totally misleading
# when you try to analyse the problem, that's why it's best to avoid it,
function run_nc() {
    local host=${1}
    local port=${2}
    local ip
    ip=$(python -c "import socket; print(socket.gethostbyname('${host}'))")

    nc -zvvn "${ip}" "${port}"
}

function check_service {
    local label=$1
    local call=$2
    local max_check=${3:=1}

    echo -n "${label}: "
    while true
    do
        set +e
        local last_check_result
        last_check_result=$(eval "${call}" 2>&1)
        local res=$?
        set -e
        if [[ ${res} == 0 ]]; then
            echo  "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo -n "."
            max_check=$((max_check-1))
        fi
        if [[ ${max_check} == 0 ]]; then
            echo "${COLOR_RED}ERROR: Maximum number of retries while checking service. Exiting ${COLOR_RESET}"
            break
        else
            sleep 1
        fi
    done
    if [[ ${res} != 0 ]]; then
        echo "Service could not be started!"
        echo
        echo "$ ${call}"
        echo "${last_check_result}"
        echo
        EXIT_CODE=${res}
    fi
}

function check_integration {
    local integration_label=$1
    local integration_name=$2
    local call=$3
    local max_check=${4:=1}

    local env_var_name
    env_var_name=INTEGRATION_${integration_name^^}
    if [[ ${!env_var_name:=} != "true" || ${!env_var_name} != "True" ]]; then
        if [[ ! ${DISABLED_INTEGRATIONS} == *" ${integration_name}"* ]]; then
            DISABLED_INTEGRATIONS="${DISABLED_INTEGRATIONS} ${integration_name}"
        fi
        return
    fi
    check_service "${integration_label}" "${call}" "${max_check}"
}

function check_db_backend {
    local max_check=${1:=1}

    if [[ ${BACKEND} == "postgres" ]]; then
        check_service "PostgreSQL" "run_nc postgres 5432" "${max_check}"
    elif [[ ${BACKEND} == "mysql" ]]; then
        check_service "MySQL" "run_nc mysql 3306" "${max_check}"
    elif [[ ${BACKEND} == "mssql" ]]; then
        check_service "MSSQL" "run_nc mssql 1433" "${max_check}"
        check_service "MSSQL Login Check" "airflow db check" "${max_check}"
    elif [[ ${BACKEND} == "sqlite" ]]; then
        return
    else
        echo "Unknown backend. Supported values: [postgres,mysql,mssql,sqlite]. Current value: [${BACKEND}]"
        exit 1
    fi
}

function resetdb_if_requested() {
    if [[ ${DB_RESET:="false"} == "true" || ${DB_RESET} == "True" ]]; then
        echo
        echo "Resetting the DB"
        echo
        airflow db reset -y
        echo
        echo "Database has been reset"
        echo
    fi
    return $?
}

function startairflow_if_requested() {
    if [[ ${START_AIRFLOW:="false"} == "true" || ${START_AIRFLOW} == "True" ]]; then
        echo
        echo "Starting Airflow"
        echo
        export AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=${LOAD_DEFAULT_CONNECTIONS}
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}

        . "$( dirname "${BASH_SOURCE[0]}" )/configure_environment.sh"

        airflow db init
        airflow users create -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email

        . "$( dirname "${BASH_SOURCE[0]}" )/run_init_script.sh"

    fi
    return $?
}

echo
echo "${COLOR_BLUE}Checking integrations and backends.${COLOR_RESET}"
echo

if [[ -n ${BACKEND=} ]]; then
    check_db_backend 50
fi
echo
check_integration "Kerberos" "kerberos" "run_nc kdc-server-example-com 88" 50
check_integration "MongoDB" "mongo" "run_nc mongo 27017" 50
check_integration "Redis" "redis" "run_nc redis 6379" 50
check_integration "Cassandra" "cassandra" "run_nc cassandra 9042" 50
check_integration "OpenLDAP" "openldap" "run_nc openldap 389" 50
check_integration "Trino (HTTP)" "trino" "run_nc trino 8080" 50
check_integration "Trino (HTTPS)" "trino" "run_nc trino 7778" 50
check_integration "Trino (API)" "trino" \
    "curl --max-time 1 http://trino:8080/v1/info/ | grep '\"starting\":false'" 50
check_integration "Pinot (HTTP)" "pinot" "run_nc pinot 9000" 50
CMD="curl --max-time 1 -X GET 'http://pinot:9000/health' -H 'accept: text/plain' | grep OK"
check_integration "Pinot (Controller API)" "pinot" "${CMD}" 50
CMD="curl --max-time 1 -X GET 'http://pinot:9000/pinot-controller/admin' -H 'accept: text/plain' | grep GOOD"
check_integration "Pinot (Controller API)" "pinot" "${CMD}" 50
CMD="curl --max-time 1 -X GET 'http://pinot:8000/health' -H 'accept: text/plain' | grep OK"
check_integration "Pinot (Broker API)" "pinot" "${CMD}" 50
check_integration "RabbitMQ" "rabbitmq" "run_nc rabbitmq 5672" 50

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

if [[ -n ${DISABLED_INTEGRATIONS=} && (${VERBOSE=} == "true" || ${VERBOSE} == "True") ]]; then
    echo
    echo "${COLOR_BLUE}Those integrations are disabled: ${DISABLED_INTEGRATIONS}${COLOR_RESET}"
    echo
fi
