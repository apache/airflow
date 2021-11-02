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

# Might be empty
AIRFLOW_COMMAND="${1}"

set -euo pipefail

# We want to avoid misleading messages and perform only forward lookup of the service IP address.
# Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
# lookup name does not match the original name even if the host is reachable via IP. This happens
# randomly with docker-compose in Github Actions.
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

function verify_db_connection {
    DB_URL="${1}"

    DB_CHECK_MAX_COUNT=${MAX_DB_CHECK_COUNT:=20}
    DB_CHECK_SLEEP_TIME=${DB_CHECK_SLEEP_TIME:=3}

    local DETECTED_DB_BACKEND=""
    local DETECTED_DB_HOST=""
    local DETECTED_DB_PORT=""


    if [[ ${DB_URL} != sqlite* ]]; then
        # Auto-detect DB parameters
        [[ ${DB_URL} =~ ([^:]*)://([^@/]*)@?([^/:]*):?([0-9]*)/([^\?]*)\??(.*) ]] && \
            DETECTED_DB_BACKEND=${BASH_REMATCH[1]} &&
            # Not used USER match
            DETECTED_DB_HOST=${BASH_REMATCH[3]} &&
            DETECTED_DB_PORT=${BASH_REMATCH[4]} &&
            # Not used SCHEMA match
            # Not used PARAMS match

        echo DB_BACKEND="${DB_BACKEND:=${DETECTED_DB_BACKEND}}"

        if [[ -z "${DETECTED_DB_PORT=}" ]]; then
            if [[ ${DB_BACKEND} == "postgres"* ]]; then
                DETECTED_DB_PORT=5432
            elif [[ ${DB_BACKEND} == "mysql"* ]]; then
                DETECTED_DB_PORT=3306
            fi
        fi

        DETECTED_DB_HOST=${DETECTED_DB_HOST:="localhost"}

        # Allow the DB parameters to be overridden by environment variable
        echo DB_HOST="${DB_HOST:=${DETECTED_DB_HOST}}"
        echo DB_PORT="${DB_PORT:=${DETECTED_DB_PORT}}"

        while true
        do
            set +e
            LAST_CHECK_RESULT=$(run_nc "${DB_HOST}" "${DB_PORT}" >/dev/null 2>&1)
            RES=$?
            set -e
            if [[ ${RES} == 0 ]]; then
                echo
                break
            else
                echo -n "."
                DB_CHECK_MAX_COUNT=$((DB_CHECK_MAX_COUNT-1))
            fi
            if [[ ${DB_CHECK_MAX_COUNT} == 0 ]]; then
                echo
                echo "ERROR! Maximum number of retries (${DB_CHECK_MAX_COUNT}) reached while checking ${DB_BACKEND} db. Exiting"
                echo
                break
            else
                sleep "${DB_CHECK_SLEEP_TIME}"
            fi
        done
        if [[ ${RES} != 0 ]]; then
            echo "        ERROR: ${DB_URL} db could not be reached!"
            echo
            echo "${LAST_CHECK_RESULT}"
            echo
            export EXIT_CODE=${RES}
        fi
    fi
}

if ! whoami &> /dev/null; then
  if [[ -w /etc/passwd ]]; then
    echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${AIRFLOW_USER_HOME_DIR}:/sbin/nologin" \
        >> /etc/passwd
  fi
  export HOME="${AIRFLOW_USER_HOME_DIR}"
fi

# Warning: command environment variables (*_CMD) have priority over usual configuration variables
# for configuration parameters that require sensitive information. This is the case for the SQL database
# and the broker backend in this entrypoint script.


if [[ -n "${AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD=}" ]]; then
    verify_db_connection "$(eval "$AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD")"
else
    # if no DB configured - use sqlite db by default
    AIRFLOW__CORE__SQL_ALCHEMY_CONN="${AIRFLOW__CORE__SQL_ALCHEMY_CONN:="sqlite:///${AIRFLOW_HOME}/airflow.db"}"
    verify_db_connection "${AIRFLOW__CORE__SQL_ALCHEMY_CONN}"
fi


# The Bash and python commands still should verify the basic connections so they are run after the
# DB check but before the broker check
if [[ ${AIRFLOW_COMMAND} == "bash" ]]; then
   shift
   exec "/bin/bash" "${@}"
elif [[ ${AIRFLOW_COMMAND} == "python" ]]; then
   shift
   exec "python" "${@}"
elif [[ ${AIRFLOW_COMMAND} == "airflow" ]]; then
   AIRFLOW_COMMAND="${2}"
   shift
fi

# Note: the broker backend configuration concerns only a subset of Airflow components
if [[ ${AIRFLOW_COMMAND} =~ ^(scheduler|celery|worker|flower)$ ]]; then
    if [[ -n "${AIRFLOW__CELERY__BROKER_URL_CMD=}" ]]; then
        verify_db_connection "$(eval "$AIRFLOW__CELERY__BROKER_URL_CMD")"
    else
        AIRFLOW__CELERY__BROKER_URL=${AIRFLOW__CELERY__BROKER_URL:=}
        if [[ -n ${AIRFLOW__CELERY__BROKER_URL=} ]]; then
            verify_db_connection "${AIRFLOW__CELERY__BROKER_URL}"
        fi
    fi
fi

exec "airflow" "${@}"
