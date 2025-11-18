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

# We want to avoid misleading messages and perform only forward lookup of the service IP address.
# Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
# lookup name does not match the original name even if the host is reachable via IP. This happens
# randomly with docker-compose in GitHub Actions.
# Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
# And use the IP in NC and add '-n' switch to disable any DNS use.
# Even if this message might be harmless, it might hide the real reason for the problem
# Which is the long time needed to start some services, seeing this message might be totally misleading
# when you try to analyse the problem, that's why it's best to avoid it,

. "$( dirname "${BASH_SOURCE[0]}" )/check_connectivity.sh"

export COLOR_YELLOW=$'\e[33m'
export COLOR_RESET=$'\e[0m'

function check_service {
    local label=$1
    local call=$2
    local max_check=${3:=1}
    local initial_delay="${4:-0}"

    if [[ ${initial_delay} != 0 ]]; then
        echo "${COLOR_YELLOW}Adding initial delay. Waiting ${initial_delay} seconds before checking ${label}.${COLOR_RESET}"
        sleep "${initial_delay}"
    fi
    check_service_connection "${label}" "${call}" "${max_check}"
    EXIT_CODE=$?
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
    elif [[ ${BACKEND} == "none" ]]; then
        echo "${COLOR_YELLOW}WARNING: Using no database backend${COLOR_RESET}"

        if [[ ${START_AIRFLOW=} == "true" ]]; then
            echo "${COLOR_RED}ERROR: 'start-airflow' cannot be used with --backend=none${COLOR_RESET}"
            echo "${COLOR_RED}Supported values are: [postgres,mysql,sqlite]${COLOR_RESET}"
            echo "${COLOR_RED}Please specify one using '--backend'${COLOR_RESET}"
            exit 1
        fi
    else
        echo "${COLOR_RED}ERROR: Unknown backend. Supported values: [postgres,mysql,sqlite]. Current value: [${BACKEND}]${COLOR_RESET}"
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
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}

        if airflow db migrate
        then
            if [[ ${LOAD_DEFAULT_CONNECTIONS=} == "true" || ${LOAD_DEFAULT_CONNECTIONS=} == "True" ]]; then
                echo
                echo "${COLOR_BLUE}Creating default connections${COLOR_RESET}"
                echo
                airflow connections create-default-connections
            fi
        else
            echo "${COLOR_YELLOW}Failed to run 'airflow db migrate'.${COLOR_RESET}"
            echo "${COLOR_BLUE}This could be because you are installing old airflow version${COLOR_RESET}"
            echo "${COLOR_BLUE}Attempting to run deprecated 'airflow db init' instead.${COLOR_RESET}"
            # For Airflow versions that do not support db migrate, we should run airflow db init and
            # set the removed AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS
            AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=${LOAD_DEFAULT_CONNECTIONS} airflow db init
        fi

        if airflow config get-value core auth_manager | grep -q "FabAuthManager"; then
            airflow users create -u admin -p admin -f Thor -l Adminstra -r Admin -e admin@email.domain || true

            # Create all roles for testing if CREATE_ALL_ROLES is set
            if [[ "${CREATE_ALL_ROLES}" == "true" ]]; then
                echo "Creating all test roles for FabAuthManager..."
                airflow users create -u viewer -p viewer -f Test -l Viewer -r Viewer -e viewer@email.domain || true
                airflow users create -u user -p user -f Test -l User -r User -e user@email.domain || true
                airflow users create -u op -p op -f Test -l Op -r Op -e op@email.domain || true
                airflow users create -u testadmin -p testadmin -f Test -l TestAdmin -r Admin -e testadmin@email.domain || true
                echo "All test roles created successfully for FabAuthManager."
            fi
        else
            echo "SimpleAuthManager detected. All roles (admin, viewer, user, op) are always available via configuration in .dev/breeze/src/airflow_breeze/files/simple_auth_manager_passwords.json"
        fi
    fi
    return $?
}

echo
echo "${COLOR_BLUE}Checking backend and integrations.${COLOR_RESET}"
echo

if [[ -n ${BACKEND=} ]]; then
    check_db_backend 50
fi
echo

if [[ ${INTEGRATION_KERBEROS} == "true" ]]; then
    check_service "Kerberos" "run_nc kdc-server-example-com 88" 50
fi
if [[ ${INTEGRATION_MONGO} == "true" ]]; then
    check_service "MongoDB" "run_nc mongo 27017" 50
fi
if [[ ${INTEGRATION_REDIS} == "true" ]]; then
    check_service "Redis" "run_nc redis 6379" 50
fi
if [[ ${INTEGRATION_CELERY} == "true" ]]; then
    check_service "Redis" "run_nc redis 6379" 50
    check_service "RabbitMQ" "run_nc rabbitmq 5672" 50
fi
if [[ ${INTEGRATION_CASSANDRA} == "true" ]]; then
    check_service "Cassandra" "run_nc cassandra 9042" 50
fi
if [[ ${INTEGRATION_TRINO} == "true" ]]; then
    check_service "Trino (HTTP)" "run_nc trino 8080" 50
    check_service "Trino (HTTPS)" "run_nc trino 7778" 50
    check_service "Trino (API)" "curl --max-time 1 http://trino:8080/v1/info/ | grep '\"starting\":false'" 50
fi
if [[ ${INTEGRATION_PINOT} == "true" ]]; then
    check_service "Pinot (HTTP)" "run_nc pinot 9000" 50
    CMD="curl --max-time 1 -X GET 'http://pinot:9000/health' -H 'accept: text/plain' | grep OK"
    check_service "Pinot (Controller API)" "${CMD}" 50
    CMD="curl --max-time 1 -X GET 'http://pinot:9000/pinot-controller/admin' -H 'accept: text/plain' | grep GOOD"
    check_service "Pinot (Controller API)" "${CMD}" 50
    CMD="curl --max-time 1 -X GET 'http://pinot:8000/health' -H 'accept: text/plain' | grep OK"
    check_service "Pinot (Broker API)" "${CMD}" 50
fi

if [[ ${INTEGRATION_QDRANT} == "true" ]]; then
    check_service "Qdrant" "run_nc qdrant 6333" 50
    CMD="curl -f -X GET 'http://qdrant:6333/collections'"
    check_service "Qdrant (Collections API)" "${CMD}" 50
fi

if [[ ${INTEGRATION_KAFKA} == "true" ]]; then
    check_service "Kafka Cluster" "run_nc broker 9092" 50
fi

if [[ ${INTEGRATION_MSSQL} == "true" ]]; then
    check_service "mssql" "run_nc mssql 1433" 50
fi

if [[ ${INTEGRATION_DRILL} == "true" ]]; then
    check_service "drill" "run_nc drill 8047" 50
fi

if [[ ${INTEGRATION_YDB} == "true" ]]; then
    check_service "YDB Cluster" "run_nc ydb 2136" 50
fi

if [[ ${INTEGRATION_TINKERPOP} == "true" ]]; then
    check_service "gremlin" "run_nc gremlin 8182" 100 30
fi

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
