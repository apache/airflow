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

function check_service {
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

function check_integration {
    INTEGRATION_NAME=$1

    ENV_VAR_NAME=INTEGRATION_${INTEGRATION_NAME^^}
    if [[ ${!ENV_VAR_NAME:=} != "true" ]]; then
        DISABLED_INTEGRATIONS="${DISABLED_INTEGRATIONS} ${INTEGRATION_NAME}"
        return
    fi
    check_service "${@}"
}

function check_db_backend {
    MAX_CHECK=${1:=1}

    if [[ ${BACKEND} == "postgres" ]]; then
        check_service "postgres" "nc -zvv postgres 5432" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "mysql" ]]; then
        check_service "mysql" "nc -zvv mysql 3306" "${MAX_CHECK}"
    elif [[ ${BACKEND} == "sqlite" ]]; then
        return
    else
        echo "Unknown backend. Supported values: [postgres,mysql,sqlite]. Current value: [${BACKEND}]"
        exit 1
    fi
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

function startairflow_if_requested() {
    if [[ ${START_AIRFLOW:="false"} == "true" ]]; then

	. "$( dirname "${BASH_SOURCE[0]}" )/configure_environment.sh"

        # initialize db and create the admin user if it's a new run
        airflow db init
        airflow users create -u admin -p admin -f Thor -l Adminstra -r Admin -e dummy@dummy.email

        #this is because I run docker in WSL - Hi Bill!
        export TMUX_TMPDIR=~/.tmux/tmp
        mkdir -p ~/.tmux/tmp
        chmod 777 -R ~/.tmux/tmp

        # Set Session Name
        SESSION="Airflow"

        # Start New Session with our name
        tmux new-session -d -s $SESSION

        # Name first Pane and start bash
        tmux rename-window -t 0 'Main'
        tmux send-keys -t 'Main' 'bash' C-m 'clear' C-m

        tmux split-window -v
        tmux select-pane -t 1
        tmux send-keys 'airflow scheduler' C-m

        tmux split-window -h
        tmux select-pane -t 2
        tmux send-keys 'airflow webserver' C-m

        # Attach Session, on the Main window
        tmux select-pane -t 0
        tmux send-keys 'cd /opt/airflow/' C-m 'clear' C-m

        tmux attach-session -t $SESSION:0
    fi
    return $?
}

echo "==============================================================================================="
echo "             Checking integrations and backends"
echo "==============================================================================================="
if [[ -n ${BACKEND=} ]]; then
    check_db_backend 20
    echo "-----------------------------------------------------------------------------------------------"
fi
check_integration kerberos "nc -zvv kerberos 88" 30
check_integration mongo "nc -zvv mongo 27017" 20
check_integration redis "nc -zvv redis 6379" 20
check_integration rabbitmq "nc -zvv rabbitmq 5672" 20
check_integration cassandra "nc -zvv cassandra 9042" 20
check_integration openldap "nc -zvv openldap 389" 20
check_integration presto "nc -zvv presto 8080" 40
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
