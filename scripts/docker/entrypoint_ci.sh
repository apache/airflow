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
export SQLALCHEMY_ENGINE_DEBUG="true"

function set_verbose() {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    else
        set +x
    fi
}

set_verbose
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

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.10}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

# Create folder where sqlite database file will be stored if it does not exist (which happens when
# scripts are running rather than breeze shell)
mkdir "${AIRFLOW_HOME}/sqlite" -p || true

ASSET_COMPILATION_WAIT_MULTIPLIER=${ASSET_COMPILATION_WAIT_MULTIPLIER:=1}

if [[ "${CI=}" == "true" ]]; then
    export COLUMNS="202"
fi

# shellcheck disable=SC1091
. "${IN_CONTAINER_DIR}/check_connectivity.sh"

# Make sure that asset compilation is completed before we proceed
function wait_for_asset_compilation() {
    if [[ -f "${AIRFLOW_SOURCES}/.build/ui/.asset_compile.lock" ]]; then
        echo
        echo "${COLOR_YELLOW}Waiting for asset compilation to complete in the background.${COLOR_RESET}"
        echo
        local counter=0
        while [[ -f "${AIRFLOW_SOURCES}/.build/ui/.asset_compile.lock" ]]; do
            if (( counter % 5 == 2 )); then
                echo "${COLOR_BLUE}Still waiting .....${COLOR_RESET}"
            fi
            sleep 1
            ((counter=counter+1))
            if [[ ${counter} == 30*$ASSET_COMPILATION_WAIT_MULTIPLIER ]]; then
                echo
                echo "${COLOR_YELLOW}The asset compilation is taking too long.${COLOR_YELLOW}"
                echo """
If it does not complete soon, you might want to stop it and remove file lock:
   * press Ctrl-C
   * run 'rm ${AIRFLOW_SOURCES}/.build/ui/.asset_compile.lock'
"""
            fi
            if [[ ${counter} == 60*$ASSET_COMPILATION_WAIT_MULTIPLIER ]]; then
                echo
                echo "${COLOR_RED}The asset compilation is taking too long. Exiting.${COLOR_RED}"
                echo "${COLOR_RED}refer to dev/breeze/doc/04_troubleshooting.rst for resolution steps.${COLOR_RED}"
                echo
                exit 1
            fi
        done
    fi
    if [ -f "${AIRFLOW_SOURCES}/.build/ui/asset_compile.out" ]; then
        echo
        echo "${COLOR_RED}The asset compilation failed. Exiting.${COLOR_RESET}"
        echo
        cat "${AIRFLOW_SOURCES}/.build/ui/asset_compile.out"
        rm "${AIRFLOW_SOURCES}/.build/ui/asset_compile.out"
        echo
        exit 1
    fi
}

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
        echo "${COLOR_BLUE}Forcing scheduler/standalone_dag_processor to True${COLOR_RESET}"
        echo
        export AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True
    fi

    RUN_TESTS=${RUN_TESTS:="false"}
    CI=${CI:="false"}

    # Added to have run-tests on path
    export PATH=${PATH}:${AIRFLOW_SOURCES}:/usr/local/go/bin/

    mkdir -pv "${AIRFLOW_HOME}/logs/"

    # Change the default worker_concurrency for tests
    export AIRFLOW__CELERY__WORKER_CONCURRENCY=8

    set +e

    # shellcheck source=scripts/in_container/configure_environment.sh
    . "${IN_CONTAINER_DIR}/configure_environment.sh"
    # shellcheck source=scripts/in_container/run_init_script.sh
    . "${IN_CONTAINER_DIR}/run_init_script.sh"

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

    if [[ ${INTEGRATION_LOCALSTACK:-"false"} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Configuring LocalStack integration${COLOR_RESET}"
        echo

        # Define LocalStack AWS configuration
        declare -A localstack_config=(
            ["AWS_ENDPOINT_URL"]="http://localstack:4566"
            ["AWS_ACCESS_KEY_ID"]="test"
            ["AWS_SECRET_ACCESS_KEY"]="test"
            ["AWS_DEFAULT_REGION"]="us-east-1"
        )

        # Export each configuration variable and log it
        for key in "${!localstack_config[@]}"; do
            export "$key"="${localstack_config[$key]}"
            echo "  * ${COLOR_BLUE}${key}:${COLOR_RESET} ${localstack_config[$key]}"
        done
        echo
    fi

    cd "${AIRFLOW_SOURCES}"

    # Temporarily add /opt/airflow/providers/standard/tests to PYTHONPATH in order to see example dags
    # in the UI when testing in Breeze. This might be solved differently in the future
    if [[ -d /opt/airflow/providers/standard/tests  ]]; then
        export PYTHONPATH=${PYTHONPATH=}:/opt/airflow/providers/standard/tests
    fi

    if [[ ${START_AIRFLOW:="false"} == "true" || ${START_AIRFLOW} == "True" ]]; then
        if [[ ${BREEZE_DEBUG_CELERY_WORKER=} == "true" ]]; then
            export AIRFLOW__CELERY__POOL=${AIRFLOW__CELERY__POOL:-solo}
        fi
        export AIRFLOW__CORE__LOAD_EXAMPLES=${LOAD_EXAMPLES}
        wait_for_asset_compilation
        if [[ ${USE_MPROCS:="false"} == "true" || ${USE_MPROCS} == "True" ]]; then
            # shellcheck source=scripts/in_container/bin/run_mprocs
            exec run_mprocs
        else
            # shellcheck source=scripts/in_container/bin/run_tmux
            exec run_tmux
        fi
    fi
}

# Handle mount sources
function handle_mount_sources() {
    if [[ ${MOUNT_SOURCES=} == "remove" ]]; then
        echo
        echo "${COLOR_BLUE}Mounted sources are removed, cleaning up mounted dist-info files${COLOR_RESET}"
        echo
        rm -rf /usr/local/lib/python"${PYTHON_MAJOR_MINOR_VERSION}"/site-packages/apache_airflow*.dist-info/
    fi
}

# Determine which airflow version to use
function determine_airflow_to_use() {
    USE_AIRFLOW_VERSION="${USE_AIRFLOW_VERSION:=""}"
    if [[ "${USE_AIRFLOW_VERSION}" == "" && "${USE_DISTRIBUTIONS_FROM_DIST=}" != "true" ]]; then
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
            ${PACKAGING_TOOL_CMD} freeze | grep -ve "^-e" | grep -ve "^#" | grep -ve "^uv" | grep -v "@" | \
                xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS}
            # Now install rich ad click first to use the installation script
            # shellcheck disable=SC2086
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} rich rich-click click \
                --constraint https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt
        fi
        echo
        echo "${COLOR_BLUE}Reinstalling all development dependencies${COLOR_RESET}"
        echo
        # Use uv run to install necessary dependencies automatically
        # in the future we will be able to use uv sync when `uv.lock` is supported
        # for the use in parallel runs in docker containers--no-cache is needed - otherwise there is
        # possibility of overriding temporary environments by multiple parallel processes
        uv run --no-cache /opt/airflow/scripts/in_container/install_development_dependencies.py \
           --constraint https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-"${PYTHON_MAJOR_MINOR_VERSION}".txt
        # Some packages might leave legacy typing module which causes test issues
        # shellcheck disable=SC2086
        ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} typing || true
        echo
        echo "${COLOR_BLUE}Installing airflow and providers ${COLOR_RESET}"
        echo
        python "${IN_CONTAINER_DIR}/install_airflow_and_providers.py"
    fi
    if [[ "${USE_AIRFLOW_VERSION}" =~ ^2.* ]]; then
        # Remove auth manager setting
        unset AIRFLOW__CORE__AUTH_MANAGER
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
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} aiobotocore s3fs || true

    # Urllib 2.6.0 breaks kubernetes client because kubernetes client uses deprecated in 2.0.0 and
    # removed in 2.6.0 `getheaders()` call (instead of `headers` property.
    # Tracked in https://github.com/kubernetes-client/python/issues/2477
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} --upgrade "boto3<1.38.3" "botocore<1.38.3" "urllib3<2.6.0"
}

# Upgrade sqlalchemy to the latest version to run tests with it
function check_upgrade_sqlalchemy() {
    # The python version constraint is a TEMPORARY WORKAROUND to exclude all FAB tests. Is should be removed once we
    # upgrade FAB to v5 (PR #50960).
    if [[ "${UPGRADE_SQLALCHEMY=}" != "true" || ${PYTHON_MAJOR_MINOR_VERSION} != "3.13" ]]; then
        return
    fi
    echo
    echo "${COLOR_BLUE}Upgrading sqlalchemy to the latest version to run tests with it${COLOR_RESET}"
    echo
    uv sync --all-packages --no-install-package apache-airflow-providers-fab --resolution highest \
        --no-python-downloads --no-managed-python
}

# Download minimum supported version of sqlalchemy to run tests with it
function check_downgrade_sqlalchemy() {
    if [[ ${DOWNGRADE_SQLALCHEMY=} != "true" ]]; then
        return
    fi
    local min_sqlalchemy_version
    min_sqlalchemy_version=$(grep "sqlalchemy\[asyncio\]>=" airflow-core/pyproject.toml | sed "s/.*>=\([0-9\.]*\).*/\1/" | xargs)
    echo
    echo "${COLOR_BLUE}Downgrading sqlalchemy to minimum supported version: ${min_sqlalchemy_version}${COLOR_RESET}"
    echo
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} "sqlalchemy[asyncio]==${min_sqlalchemy_version}"
    echo
    echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
    echo
    # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
    # between `pip` and `uv` installations
    # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
    # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
    uv pip check
}

# Download minimum supported version of pendulum to run tests with it
function check_downgrade_pendulum() {
    if [[ ${DOWNGRADE_PENDULUM=} != "true" || ${PYTHON_MAJOR_MINOR_VERSION} == "3.12" ]]; then
        return
    fi
    local min_pendulum_version
    min_pendulum_version=$(grep "pendulum>=" airflow-core/pyproject.toml | head -1 | sed "s/.*>=\([0-9\.]*\).*/\1/" | xargs)
    echo
    echo "${COLOR_BLUE}Downgrading pendulum to minimum supported version: ${min_pendulum_version}${COLOR_RESET}"
    echo
    # shellcheck disable=SC2086
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} "pendulum==${min_pendulum_version}"
    echo
    echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
    echo
    # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
    # between `pip` and `uv` installations
    # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
    # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
    uv pip check
}

# Check if we should run tests and run them if needed
function check_run_tests() {
    if [[ ${RUN_TESTS=} != "true" ]]; then
        return
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
    if [[ ${TEST_TYPE=} =~ Providers\[.*\] ]]; then
        local provider_id
        # shellcheck disable=SC2001
        provider_id=$(echo "${TEST_TYPE}" | sed 's/Providers\[\(.*\)\]/\1/')
        echo
        echo "${COLOR_BLUE}Forcing dependencies to lowest versions for provider: ${provider_id}${COLOR_RESET}"
        echo
        if ! /opt/airflow/scripts/in_container/is_provider_excluded.py "${provider_id}"; then
            echo
            echo "S${COLOR_YELLOW}Skipping ${provider_id} provider check on Python ${PYTHON_MAJOR_MINOR_VERSION}!${COLOR_RESET}"
            echo
            exit 0
        fi
        cd "${AIRFLOW_SOURCES}/providers/${provider_id/.//}" || exit 1
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        uv sync --resolution lowest-direct --no-binary-package lxml --no-binary-package xmlsec --all-extras \
            --no-python-downloads --no-managed-python
    else
        echo
        echo "${COLOR_BLUE}Forcing dependencies to lowest versions for Airflow.${COLOR_RESET}"
        echo
        cd "${AIRFLOW_SOURCES}/airflow-core"
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        uv sync --resolution lowest-direct --no-binary-package lxml --no-binary-package xmlsec --all-extras \
            --no-python-downloads --no-managed-python
    fi
}

function check_airflow_python_client_installation() {
    if [[ ${INSTALL_AIRFLOW_PYTHON_CLIENT=} != "true" ]]; then
        return
    fi
    python "${IN_CONTAINER_DIR}/install_airflow_python_client.py"
}

function initialize_db() {
    # If we are going to start the api server OR we are a system test (which may or may not start the api server,
    # depending on the Airflow version being used to run the tests), then migrate the DB.
    if [[ ${START_API_SERVER_WITH_EXAMPLES=} == "true" || ${TEST_GROUP:=""} == "system" ]]; then
        echo
        echo "${COLOR_BLUE}Initializing database${COLOR_RESET}"
        echo
        airflow db migrate
        echo
        echo "${COLOR_BLUE}Database initialized${COLOR_RESET}"
    fi
}

function start_api_server_with_examples(){
    USE_AIRFLOW_VERSION="${USE_AIRFLOW_VERSION:=""}"
    # Do not start the api server if either START_API_SERVER_WITH_EXAMPLES is false or the TEST_GROUP env var is not
    # equal to "system".
    if [[ ${START_API_SERVER_WITH_EXAMPLES=} != "true" && ${TEST_GROUP:=""} != "system" ]]; then
        return
    fi
    # If the use Airflow version is set and it is <= 3.0.0 (which does not have the API server anyway) also return
    if [[ ${USE_AIRFLOW_VERSION} != "" && ${USE_AIRFLOW_VERSION} < "3.0.0" ]]; then
        return
    fi
    export AIRFLOW__CORE__LOAD_EXAMPLES=True
    export AIRFLOW__API__EXPOSE_CONFIG=True
    airflow dags reserialize
    echo "Example dags parsing finished"
    if airflow config get-value core auth_manager | grep -q "FabAuthManager"; then
        echo "Create admin user"
        airflow users create -u admin -p admin -f Thor -l Administrator -r Admin -e admin@email.domain || true
        echo "Admin user created"
    else
        echo "Skipping user creation as auth manager different from Fab is used"
    fi
    echo
    echo "${COLOR_BLUE}Starting airflow api server${COLOR_RESET}"
    echo
    airflow api-server --port 8080 --daemon
    echo
    echo "${COLOR_BLUE}Waiting for api-server to start${COLOR_RESET}"
    echo
    check_service_connection "Airflow api-server" "run_nc localhost 8080" 100
    EXIT_CODE=$?
    if [[ ${EXIT_CODE} != 0 ]]; then
        echo
        echo "${COLOR_RED}Api server did not start properly${COLOR_RESET}"
        echo
        exit ${EXIT_CODE}
    fi
    echo
    echo "${COLOR_BLUE}Airflow api-server started${COLOR_RESET}"
}

handle_mount_sources
determine_airflow_to_use
environment_initialization
check_boto_upgrade
check_upgrade_sqlalchemy
check_downgrade_sqlalchemy
check_downgrade_pendulum
check_force_lowest_dependencies
check_airflow_python_client_installation
initialize_db
start_api_server_with_examples
check_run_tests "${@}"

# If we are not running tests - just exec to bash shell
exec /bin/bash "${@}"
