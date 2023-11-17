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

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.8}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

: "${AIRFLOW_SOURCES:?"ERROR: AIRFLOW_SOURCES not set !!!!"}"

ASSET_COMPILATION_WAIT_MULTIPLIER=${ASSET_COMPILATION_WAIT_MULTIPLIER:=1}

function wait_for_asset_compilation() {
    if [[ -f "${AIRFLOW_SOURCES}/.build/www/.asset_compile.lock" ]]; then
        echo
        echo "${COLOR_YELLOW}Waiting for asset compilation to complete in the background.${COLOR_RESET}"
        echo
        local counter=0
        while [[ -f "${AIRFLOW_SOURCES}/.build/www/.asset_compile.lock" ]]; do
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
   * run 'rm ${AIRFLOW_SOURCES}/.build/www/.asset_compile.lock'
"""
            fi
            if [[ ${counter} == 60*$ASSET_COMPILATION_WAIT_MULTIPLIER ]]; then
                echo
                echo "${COLOR_RED}The asset compilation is taking too long. Exiting.${COLOR_RED}"
                echo "${COLOR_RED}refer to BREEZE.rst for resolution steps.${COLOR_RED}"
                echo
                exit 1
            fi
        done
    fi
    if [ -f "${AIRFLOW_SOURCES}/.build/www/asset_compile.out" ]; then
        echo
        echo "${COLOR_RED}The asset compilation failed. Exiting.${COLOR_RESET}"
        echo
        cat "${AIRFLOW_SOURCES}/.build/www/asset_compile.out"
        rm "${AIRFLOW_SOURCES}/.build/www/asset_compile.out"
        echo
        exit 1
    fi
}

if [[ ${SKIP_ENVIRONMENT_INITIALIZATION=} != "true" ]]; then

    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        if [[ ${BACKEND:=} == "mssql" ]]; then
            echo "${COLOR_RED}ARM platform is not supported for ${BACKEND} backend. Exiting.${COLOR_RESET}"
            exit 1
        fi
    fi

    echo
    echo "${COLOR_BLUE}Running Initialization. Your basic configuration is:${COLOR_RESET}"
    echo
    echo "  * ${COLOR_BLUE}Airflow home:${COLOR_RESET} ${AIRFLOW_HOME}"
    echo "  * ${COLOR_BLUE}Airflow sources:${COLOR_RESET} ${AIRFLOW_SOURCES}"
    echo "  * ${COLOR_BLUE}Airflow core SQL connection:${COLOR_RESET} ${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}"
    echo

    if [[ ${STANDALONE_DAG_PROCESSOR=} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Running forcing scheduler/standalone_dag_processor to be True${COLOR_RESET}"
        echo
        export AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True
    fi
    if [[ ${DATABASE_ISOLATION=} == "true" ]]; then
        echo "${COLOR_BLUE}Force database isolation configuration:${COLOR_RESET}"
        export AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION=True
        export AIRFLOW__CORE__INTERNAL_API_URL=http://localhost:8080
        export AIRFLOW__WEBSERVER_RUN_INTERNAL_API=True
    fi

    RUN_TESTS=${RUN_TESTS:="false"}
    CI=${CI:="false"}
    USE_AIRFLOW_VERSION="${USE_AIRFLOW_VERSION:=""}"

    if [[ ${USE_AIRFLOW_VERSION} == "" ]]; then
        export PYTHONPATH=${AIRFLOW_SOURCES}
        echo
        echo "${COLOR_BLUE}Using airflow version from current sources${COLOR_RESET}"
        echo
        # Cleanup the logs, tmp when entering the environment
        sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
        sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
        mkdir -p "${AIRFLOW_SOURCES}"/logs/
        mkdir -p "${AIRFLOW_SOURCES}"/tmp/
    elif [[ ${USE_AIRFLOW_VERSION} == "none"  ]]; then
        echo
        echo "${COLOR_BLUE}Skip installing airflow - only install wheel/tar.gz packages that are present locally.${COLOR_RESET}"
        echo
        echo
        echo "${COLOR_BLUE}Uninstalling airflow and providers"
        echo
        uninstall_airflow_and_providers
    elif [[ ${USE_AIRFLOW_VERSION} == "wheel"  ]]; then
        echo
        echo "${COLOR_BLUE}Uninstalling airflow and providers"
        echo
        uninstall_airflow_and_providers
        if [[ ${SKIP_CONSTRAINTS,,=} == "true" ]]; then
            echo "${COLOR_BLUE}Install airflow from wheel package with extras: '${AIRFLOW_EXTRAS}' with no constraints.${COLOR_RESET}"
            echo
            install_airflow_from_wheel "${AIRFLOW_EXTRAS}" "none"
        else
            echo "${COLOR_BLUE}Install airflow from wheel package with extras: '${AIRFLOW_EXTRAS}' and constraints reference ${AIRFLOW_CONSTRAINTS_REFERENCE}.${COLOR_RESET}"
            echo
            install_airflow_from_wheel "${AIRFLOW_EXTRAS}" "${AIRFLOW_CONSTRAINTS_REFERENCE}"
        fi
        uninstall_providers
    elif [[ ${USE_AIRFLOW_VERSION} == "sdist"  ]]; then
        echo
        echo "${COLOR_BLUE}Uninstalling airflow and providers"
        echo
        uninstall_airflow_and_providers
        echo
        if [[ ${SKIP_CONSTRAINTS,,=} == "true" ]]; then
            echo "${COLOR_BLUE}Install airflow from sdist package with extras: '${AIRFLOW_EXTRAS}' with no constraints.${COLOR_RESET}"
            echo
            install_airflow_from_sdist "${AIRFLOW_EXTRAS}" "none"
        else
            echo "${COLOR_BLUE}Install airflow from sdist package with extras: '${AIRFLOW_EXTRAS}' and constraints reference ${AIRFLOW_CONSTRAINTS_REFERENCE}.${COLOR_RESET}"
            echo
            install_airflow_from_sdist "${AIRFLOW_EXTRAS}" "${AIRFLOW_CONSTRAINTS_REFERENCE}"
        fi
        uninstall_providers
    else
        echo
        echo "${COLOR_BLUE}Uninstalling airflow and providers"
        echo
        uninstall_airflow_and_providers
        echo
        if [[ ${SKIP_CONSTRAINTS,,=} == "true" ]]; then
            echo "${COLOR_BLUE}Install released airflow from PyPI with extras: '${AIRFLOW_EXTRAS}' with no constraints.${COLOR_RESET}"
            echo
            install_released_airflow_version "${USE_AIRFLOW_VERSION}" "none"
        else
            echo "${COLOR_BLUE}Install released airflow from PyPI with extras: '${AIRFLOW_EXTRAS}' and constraints reference ${AIRFLOW_CONSTRAINTS_REFERENCE}.${COLOR_RESET}"
            echo
            install_released_airflow_version "${USE_AIRFLOW_VERSION}" "${AIRFLOW_CONSTRAINTS_REFERENCE}"
        fi
        if [[ "${USE_AIRFLOW_VERSION}" =~ ^2\.2\..*|^2\.1\..*|^2\.0\..* && "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=}" != "" ]]; then
            # make sure old variable is used for older airflow versions
            export AIRFLOW__CORE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}"
        fi
    fi
    if [[ ${USE_PACKAGES_FROM_DIST=} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Install all packages from dist folder${COLOR_RESET}"
        if [[ ${USE_AIRFLOW_VERSION} == "wheel" ]]; then
            echo "${COLOR_BLUE}(except apache-airflow)${COLOR_RESET}"
        fi
        if [[ ${PACKAGE_FORMAT} == "both" ]]; then
            echo
            echo "${COLOR_RED}ERROR:You can only specify 'wheel' or 'sdist' as PACKAGE_FORMAT not 'both'.${COLOR_RESET}"
            echo
            exit 1
        fi
        echo
        if [[ ${INSTALL_SELECTED_PROVIDERS=} != "" ]]; then
            IFS=\, read -ra selected_providers <<<"${INSTALL_SELECTED_PROVIDERS}"
            echo
            echo "${COLOR_BLUE}Selected providers to install: '${selected_providers[*]}'${COLOR_RESET}"
            echo
        else
            echo
            echo "${COLOR_BLUE}Installing all found providers${COLOR_RESET}"
            echo
            selected_providers=()
        fi
        installable_files=()
        for file in /dist/*.{whl,tar.gz}
        do
            if [[ ${file} == "/dist/apache?airflow-[0-9]"* ]]; then
                # Skip Apache Airflow package - it's just been installed above if
                # --use-airflow-version was set and should be skipped otherwise
                echo "${COLOR_BLUE}Skipping airflow core package ${file} from provider installation.${COLOR_RESET}"
                continue
            fi
            if [[ ${PACKAGE_FORMAT} == "wheel" && ${file} == *".whl" ]]; then
                provider_name=$(echo "${file}" | sed 's/\/dist\/apache_airflow_providers_//' | sed 's/-[0-9].*//' | sed 's/-/./g')
                if [[ ${INSTALL_SELECTED_PROVIDERS=} != "" ]]; then
                    # shellcheck disable=SC2076
                    if [[ " ${selected_providers[*]} " =~ " ${provider_name} " ]]; then
                        echo "${COLOR_BLUE}Adding ${provider_name} to install via ${file}${COLOR_RESET}"
                        installable_files+=( "${file}" )
                    else
                        echo "${COLOR_BLUE}Skipping ${provider_name} as it is not in the list of '${selected_providers[*]}'${COLOR_RESET}"
                    fi
                else
                    echo "${COLOR_BLUE}Adding ${provider_name} to install via ${file}${COLOR_RESET}"
                    installable_files+=( "${file}" )
                fi
            fi
            if [[ ${PACKAGE_FORMAT} == "sdist" && ${file} == *".tar.gz" ]]; then
                provider_name=$(echo "${file}" | sed 's/\/dist\/apache-airflow-providers-//' | sed 's/-[0-9].*//' | sed 's/-/./g')
                if [[ ${INSTALL_SELECTED_PROVIDERS=} != "" ]]; then
                    # shellcheck disable=SC2076
                    if [[ " ${selected_providers[*]} " =~ " ${provider_name} " ]]; then
                        echo "${COLOR_BLUE}Adding ${provider_name} to install via ${file}${COLOR_RESET}"
                        installable_files+=( "${file}" )
                    else
                        echo "${COLOR_BLUE}Skipping ${provider_name} as it is not in the list of '${selected_providers[*]}'${COLOR_RESET}"
                    fi
                else
                    echo "${COLOR_BLUE}Adding ${provider_name} to install via ${file}${COLOR_RESET}"
                    installable_files+=( "${file}" )
                fi
            fi
        done
        if [[ ${USE_AIRFLOW_VERSION} != "wheel" && ${USE_AIRFLOW_VERSION} != "sdist" && ${USE_AIRFLOW_VERSION} != "none" && ${USE_AIRFLOW_VERSION} != "" ]]; then
            echo
            echo "${COLOR_BLUE}Also adding airflow in specified version ${USE_AIRFLOW_VERSION} to make sure it is not upgraded by >= limits${COLOR_RESET}"
            echo
            installable_files+=( "apache-airflow==${USE_AIRFLOW_VERSION}" )
        fi
        echo
        echo "${COLOR_BLUE}Installing: ${installable_files[*]}${COLOR_RESET}"
        echo
        if (( ${#installable_files[@]} )); then
            pip install --root-user-action ignore "${installable_files[@]}"
        fi
    fi

    # Added to have run-tests on path
    export PATH=${PATH}:${AIRFLOW_SOURCES}

    # This is now set in conftest.py - only for pytest tests
    unset AIRFLOW__CORE__UNIT_TEST_MODE

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

    in_container_fix_ownership

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
        wait_for_asset_compilation
        # shellcheck source=scripts/in_container/bin/run_tmux
        exec run_tmux
    fi
fi

# Remove pytest.ini and .coveragerc from the current directory if it exists. It has been removed from the source tree
# but may still be present in the local directory if the user has old breeze image
rm -f "${AIRFLOW_SOURCES}/pytest.ini"
rm -f "${AIRFLOW_SOURCES}/.coveragerc"

if [[ ${UPGRADE_BOTO=} == "true" ]]; then
    echo
    echo "${COLOR_BLUE}Upgrading boto3, botocore to latest version to run Amazon tests with them${COLOR_RESET}"
    echo
    pip uninstall --root-user-action ignore aiobotocore s3fs -y || true
    pip install --root-user-action ignore --upgrade boto3 botocore
    pip check
fi
if [[ ${DOWNGRADE_SQLALCHEMY=} == "true" ]]; then
    min_sqlalchemy_version=$(grep "sqlalchemy>=" setup.cfg | sed "s/.*>=\([0-9\.]*\).*/\1/")
    echo
    echo "${COLOR_BLUE}Downgrading sqlalchemy to minimum supported version: ${min_sqlalchemy_version}${COLOR_RESET}"
    echo
    pip install --root-user-action ignore "sqlalchemy==${min_sqlalchemy_version}"
    pip check
fi

# Just in case uninstall the pytest-capture-warning which we vendored-in
pip uninstall --root-user-action ignore "pytest-capture-warnings" -y >/dev/null 2>&1 || true

set +u
# If we do not want to run tests, we simply drop into bash
if [[ "${RUN_TESTS}" != "true" ]]; then
    exec /bin/bash "${@}"
fi
set -u

if [[ ${REMOVE_ARM_PACKAGES:="false"} == "true" ]]; then
    # Test what happens if we do not have ARM packages installed.
    # This is useful to see if pytest collection works without ARM packages which is important
    # for the MacOS M1 users running tests in their ARM machines with `breeze testing tests` command
    python "${IN_CONTAINER_DIR}/remove_arm_packages.py"
fi

if [[ ${TEST_TYPE} == "PlainAsserts" ]]; then
   # Plain asserts should be converted to env variable to make sure they are taken into account
   # otherwise they will not be effective during test collection when plain assert is breaking collection
   export PYTEST_PLAIN_ASSERTS="true"
fi

if [[ ${RUN_SYSTEM_TESTS:="false"} == "true" ]]; then
    "${IN_CONTAINER_DIR}/run_system_tests.sh" "${@}"
else
    "${IN_CONTAINER_DIR}/run_ci_tests.sh" "${@}"
fi
