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

        if [[ -z "${DETECTED_DB_PORT}" ]]; then
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
            LAST_CHECK_RESULT=$(nc -zvv "${DB_HOST}" "${DB_PORT}" >/dev/null 2>&1)
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
            echo "        ERROR: ${BACKEND} db could not be reached!"
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


# if no DB configured - use sqlite db by default
AIRFLOW__CORE__SQL_ALCHEMY_CONN="${AIRFLOW__CORE__SQL_ALCHEMY_CONN:="sqlite:///${AIRFLOW_HOME}/airflow.db"}"

verify_db_connection "${AIRFLOW__CORE__SQL_ALCHEMY_CONN}"

AIRFLOW__CELERY__BROKER_URL=${AIRFLOW__CELERY__BROKER_URL:=}

if [[ -n ${AIRFLOW__CELERY__BROKER_URL} ]] && \
        [[ ${AIRFLOW_COMMAND} =~ ^(scheduler|worker|flower)$ ]]; then
    verify_db_connection "${AIRFLOW__CELERY__BROKER_URL}"
fi

if [[ ${AIRFLOW_COMMAND} == "bash" ]]; then
   shift
   exec "/bin/bash" "${@}"
elif [[ ${AIRFLOW_COMMAND} == "python" ]]; then
   shift
   exec "python" "${@}"
fi

# Run the command
exec airflow "${@}"
