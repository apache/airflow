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
if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
    set -x
fi


# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "${AIRFLOW_SOURCES:-/opt/airflow}"/scripts/in_container/_in_container_script_init.sh

# This one is to workaround https://github.com/apache/airflow/issues/17546
# issue with /usr/lib/<MACHINE>-linux-gnu/libstdc++.so.6: cannot allocate memory in static TLS block
# We do not yet a more "correct" solution to the problem but in order to avoid raising new issues
# by users of the prod image, we implement the workaround now.
# The side effect of this is slightly (in the range of 100s of milliseconds) slower load for any
# binary started and a little memory used for Heap allocated by initialization of libstdc++
# This overhead is not happening for binaries that already link dynamically libstdc++
LD_PRELOAD="/usr/lib/$(uname -m)-linux-gnu/libstdc++.so.6"
export LD_PRELOAD

# Add "other" and "group" write permission to the tmp folder
# Note that it will also change permissions in the /tmp folder on the host
# but this is necessary to enable some of our CLI tools to work without errors
chmod 1777 /tmp

AIRFLOW_SOURCES=$(cd "${IN_CONTAINER_DIR}/../.." || exit 1; pwd)

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.9}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

# Create folder where sqlite database file will be stored if it does not exist (which happens when
# scripts are running rather than breeze shell)
mkdir "${AIRFLOW_HOME}/sqlite" -p || true

ASSET_COMPILATION_WAIT_MULTIPLIER=${ASSET_COMPILATION_WAIT_MULTIPLIER:=1}

# shellcheck disable=SC1091
. "${IN_CONTAINER_DIR}/check_connectivity.sh"

# Initialize environment variables to their default values that are used for running tests and
# interactive shell.
function environment_initialization() {
    if [[ ${SKIP_ENVIRONMENT_INITIALIZATION=} == "true" ]]; then
        return
    fi
    echo
    echo "${COLOR_BLUE}Running Initialization. Your basic configuration is:${COLOR_RESET}"
    echo
    echo "  * ${COLOR_BLUE}Airflow home:${COLOR_RESET} ${AIRFLOW_HOME}"
    echo "  * ${COLOR_BLUE}Airflow sources:${COLOR_RESET} ${AIRFLOW_SOURCES}"
    echo "  * ${COLOR_BLUE}Airflow core SQL connection:${COLOR_RESET} ${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:=}"
    if [[ ${BACKEND=} == "postgres" ]]; then
        echo "  * ${COLOR_BLUE}Airflow backend:${COLOR_RESET} Postgres: ${POSTGRES_VERSION}"
    elif [[ ${BACKEND=} == "mysql" ]]; then
        echo "  * ${COLOR_BLUE}Airflow backend:${COLOR_RESET} MySQL: ${MYSQL_VERSION}"
    elif [[ ${BACKEND=} == "sqlite" ]]; then
        echo "  * ${COLOR_BLUE}Airflow backend:${COLOR_RESET} Sqlite"
    fi
    echo

    if [[ ${STANDALONE_DAG_PROCESSOR=} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Running forcing scheduler/standalone_dag_processor to be True${COLOR_RESET}"
        echo
        export AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True
    fi

    RUN_TESTS=${RUN_TESTS:="false"}
    CI=${CI:="false"}

    # Added to have run-tests on path
    export PATH=${PATH}:${AIRFLOW_SOURCES}

    # Directory where simple auth manager store generated passwords
    export AIRFLOW_AUTH_MANAGER_CREDENTIAL_DIRECTORY="/files"

    mkdir -pv "${AIRFLOW_HOME}/logs/"

    # Change the default worker_concurrency for tests
    export AIRFLOW__CELERY__WORKER_CONCURRENCY=8

    set +e

    "${IN_CONTAINER_DIR}/check_environment.sh"
    ENVIRONMENT_EXIT_CODE=$?
    set -e
    if [[ ${ENVIRONMENT_EXIT_CODE} != 0 ]]; then
        echo
        echo "Error: check_environment returned ${ENVIRONMENT_EXIT_CODE}. Exiting."
        echo
        exit ${ENVIRONMENT_EXIT_CODE}
    fi
    mkdir -p /usr/lib/google-cloud-sdk/bin
    touch /usr/lib/google-cloud-sdk/bin/gcloud
    ln -s -f /usr/bin/gcloud /usr/lib/google-cloud-sdk/bin/gcloud

    if [[ ${SKIP_SSH_SETUP="false"} == "false" ]]; then
        # Set up ssh keys
        echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -m PEM -P '' -f ~/.ssh/id_rsa \
            >"${AIRFLOW_HOME}/logs/ssh-keygen.log" 2>&1

        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ln -s -f ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
        chmod 600 ~/.ssh/*

        # SSH Service
        sudo service ssh restart >/dev/null 2>&1

        # Sometimes the server is not quick enough to load the keys!
        while [[ $(ssh-keyscan -H localhost 2>/dev/null | wc -l) != "3" ]] ; do
            echo "Not all keys yet loaded by the server"
            sleep 0.05
        done

        ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null
    fi

    # shellcheck source=scripts/in_container/configure_environment.sh
    . "${IN_CONTAINER_DIR}/configure_environment.sh"

    # shellcheck source=scripts/in_container/run_init_script.sh
    . "${IN_CONTAINER_DIR}/run_init_script.sh"

    cd "${AIRFLOW_SOURCES}"

    if [[ ${START_AIRFLOW:="false"} == "true" || ${START_AIRFLOW} == "True" ]]; then
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}
        # shellcheck source=scripts/in_container/bin/run_tmux
        exec run_tmux
    fi
}

# Handle mount sources
function handle_mount_sources() {
    if [[ ${MOUNT_SOURCES=} == "remove" ]]; then
        echo
        echo "${COLOR_BLUE}Mounted sources are removed, cleaning up mounted dist-info files${COLOR_RESET}"
        echo
        rm -rf /usr/local/lib/python${PYTHON_MAJOR_MINOR_VERSION}/site-packages/apache_airflow*.dist-info/
    fi
}

# Determine which airflow version to use
function determine_airflow_to_use() {
    USE_AIRFLOW_VERSION="${USE_AIRFLOW_VERSION:=""}"
    if [[ ${USE_AIRFLOW_VERSION} == "" && ${USE_PACKAGES_FROM_DIST=} != "true" ]]; then
        export PYTHONPATH=${AIRFLOW_SOURCES}
        echo
        echo "${COLOR_BLUE}Using airflow version from current sources${COLOR_RESET}"
        echo
        # Cleanup the logs, tmp when entering the environment
        sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
        sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
        mkdir -p "${AIRFLOW_SOURCES}"/logs/
        mkdir -p "${AIRFLOW_SOURCES}"/tmp/
    else
        if [[ ${CLEAN_AIRFLOW_INSTALLATION=} == "true" ]]; then
            echo
            echo "${COLOR_BLUE}Uninstalling all packages first${COLOR_RESET}"
            echo
            # shellcheck disable=SC2086
            ${PACKAGING_TOOL_CMD} freeze | grep -ve "^-e" | grep -ve "^#" | grep -ve "^uv" | \
                xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS}
            # Now install rich ad click first to use the installation script
            # shellcheck disable=SC2086
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} rich rich-click click --python "/usr/local/bin/python" \
                --constraint https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt
        fi
        python "${IN_CONTAINER_DIR}/install_airflow_and_providers.py"
        echo
        echo "${COLOR_BLUE}Reinstalling all development dependencies${COLOR_RESET}"
        echo
        python "${IN_CONTAINER_DIR}/install_devel_deps.py" \
           --constraint https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt
        # Some packages might leave legacy typing module which causes test issues
        # shellcheck disable=SC2086
        ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} typing || true
    fi

    if [[ "${USE_AIRFLOW_VERSION}" =~ ^2\.2\..*|^2\.1\..*|^2\.0\..* && "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=}" != "" ]]; then
        # make sure old variable is used for older airflow versions
        export AIRFLOW__CORE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}"
    fi
}

# Upgrade boto3 and botocore to latest version to run Amazon tests with them
function check_boto_upgrade() {
    if [[ ${UPGRADE_BOTO=} != "true" ]]; then
        return
    fi
    echo
    echo "${COLOR_BLUE}Upgrading boto3, botocore to latest version to run Amazon tests with them${COLOR_RESET}"
    echo
    set -x
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} aiobotocore s3fs || true
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} --upgrade boto3 botocore
    set +x
}

# Download minimum supported version of sqlalchemy to run tests with it
function check_downgrade_sqlalchemy() {
    if [[ ${DOWNGRADE_SQLALCHEMY=} != "true" ]]; then
        return
    fi
    min_sqlalchemy_version=$(grep "\"sqlalchemy>=" hatch_build.py | sed "s/.*>=\([0-9\.]*\).*/\1/" | xargs)
    echo
    echo "${COLOR_BLUE}Downgrading sqlalchemy to minimum supported version: ${min_sqlalchemy_version}${COLOR_RESET}"
    echo
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} "sqlalchemy==${min_sqlalchemy_version}"
    pip check
}

# Download minimum supported version of pendulum to run tests with it
function check_downgrade_pendulum() {
    if [[ ${DOWNGRADE_PENDULUM=} != "true" || ${PYTHON_MAJOR_MINOR_VERSION} == "3.12" ]]; then
        return
    fi
    local MIN_PENDULUM_VERSION="2.1.2"
    echo
    echo "${COLOR_BLUE}Downgrading pendulum to minimum supported version: ${MIN_PENDULUM_VERSION}${COLOR_RESET}"
    echo
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} "pendulum==${MIN_PENDULUM_VERSION}"
    pip check
}

# Check if we should run tests and run them if needed
function check_run_tests() {
    if [[ ${RUN_TESTS=} != "true" ]]; then
        return
    fi

    if [[ ${REMOVE_ARM_PACKAGES:="false"} == "true" ]]; then
        # Test what happens if we do not have ARM packages installed.
        # This is useful to see if pytest collection works without ARM packages which is important
        # for the MacOS M1 users running tests in their ARM machines with `breeze testing *-tests` command
        python "${IN_CONTAINER_DIR}/remove_arm_packages.py"
    fi

    if [[ ${TEST_GROUP:=""} == "system" ]]; then
        exec "${IN_CONTAINER_DIR}/run_system_tests.sh" "${@}"
    else
        exec "${IN_CONTAINER_DIR}/run_ci_tests.sh" "${@}"
    fi
}

function check_force_lowest_dependencies() {
    if [[ ${FORCE_LOWEST_DEPENDENCIES=} != "true" ]]; then
        return
    fi
    export EXTRA=""
    if [[ ${TEST_TYPE=} =~ Providers\[.*\] ]]; then
        # shellcheck disable=SC2001
        EXTRA=$(echo "[${TEST_TYPE}]" | sed 's/Providers\[\(.*\)\]/\1/' | sed 's/\./-/')
        export EXTRA
        echo
        echo "${COLOR_BLUE}Forcing dependencies to lowest versions for provider: ${EXTRA}${COLOR_RESET}"
        echo
        if ! /opt/airflow/scripts/in_container/is_provider_excluded.py; then
            echo
            echo "Skipping ${EXTRA} provider check on Python ${PYTHON_MAJOR_MINOR_VERSION}!"
            echo
            exit 0
        fi
    else
        echo
        echo "${COLOR_BLUE}Forcing dependencies to lowest versions for Airflow.${COLOR_RESET}"
        echo
    fi
    set -x
    uv pip install --python "$(which python)" --resolution lowest-direct --upgrade --editable ".${EXTRA}" --editable "./task_sdk"
    set +x
}

function check_airflow_python_client_installation() {
    if [[ ${INSTALL_AIRFLOW_PYTHON_CLIENT=} != "true" ]]; then
        return
    fi
    python "${IN_CONTAINER_DIR}/install_airflow_python_client.py"
}

handle_mount_sources
determine_airflow_to_use
environment_initialization
check_boto_upgrade
check_downgrade_sqlalchemy
check_downgrade_pendulum
check_force_lowest_dependencies
check_airflow_python_client_installation
check_run_tests "${@}"

# If we are not running tests - just exec to bash shell
exec /bin/bash "${@}"
