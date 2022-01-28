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

OPTIONAL_VERBOSE_FLAG=()
PROVIDER_PACKAGES_DIR="${AIRFLOW_SOURCES}/dev/provider_packages"

#######################################################################################################
#
# Adds trap to the traps already set.
#
# Arguments:
#      trap to set
#      .... list of signals to handle
#######################################################################################################
function add_trap() {
    trap="${1}"
    shift
    for signal in "${@}"; do
        # adding trap to exiting trap
        local handlers
        handlers="$(trap -p "${signal}" | cut -f2 -d \')"
        # shellcheck disable=SC2064
        trap "${trap};${handlers}" "${signal}"
    done
}

function assert_in_container() {
    export VERBOSE=${VERBOSE:="false"}
    if [[ ! -f /.dockerenv ]]; then
        echo
        echo "${COLOR_RED}ERROR: You are not inside the Airflow docker container!  ${COLOR_RESET}"
        echo
        echo "You should only run this script in the Airflow docker container as it may override your files."
        echo "Learn more about how we develop and test airflow in:"
        echo "https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst"
        echo
        exit 1
    fi
}

function in_container_script_start() {
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
        set -x
    fi
}

function in_container_script_end() {
    #shellcheck disable=2181
    EXIT_CODE=$?
    if [[ ${EXIT_CODE} != 0 ]]; then
        if [[ "${PRINT_INFO_FROM_SCRIPTS="true"}" == "true" ]]; then
            echo "########################################################################################################################"
            echo "${COLOR_BLUE} [IN CONTAINER]   EXITING ${0} WITH EXIT CODE ${EXIT_CODE}  ${COLOR_RESET}"
            echo "########################################################################################################################"
        fi
    fi

    if [[ ${VERBOSE_COMMANDS} == "true" ]]; then
        set +x
    fi
}

#
# Cleans up PYC files (in case they come in mounted folders)
#
function in_container_cleanup_pyc() {
    set +o pipefail
    sudo find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/ui/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "*.pyc" | grep ".pyc$" | sudo xargs rm -f
    set -o pipefail
}

#
# Cleans up __pycache__ directories (in case they come in mounted folders)
#
function in_container_cleanup_pycache() {
    set +o pipefail
    find . \
        -path "./airflow/www/node_modules" -prune -o \
        -path "./airflow/ui/node_modules" -prune -o \
        -path "./.eggs" -prune -o \
        -path "./docs/_build" -prune -o \
        -path "./build" -prune -o \
        -name "__pycache__" | grep "__pycache__" | sudo xargs rm -rf
    set -o pipefail
}

#
# Fixes ownership of files generated in container - if they are owned by root, they will be owned by
# The host user. Only needed if the host is Linux - on Mac, ownership of files is automatically
# changed to the Host user via osxfs filesystem
#
function in_container_fix_ownership() {
    if [[ ${HOST_OS:=} == "Linux" ]]; then
        DIRECTORIES_TO_FIX=(
            "/files"
            "/root/.aws"
            "/root/.azure"
            "/root/.config/gcloud"
            "/root/.docker"
            "/opt/airflow/logs"
            "/opt/airflow/docs"
            "/opt/airflow/dags"
            "${AIRFLOW_SOURCES}"
        )
        find "${DIRECTORIES_TO_FIX[@]}" -print0 -user root 2>/dev/null |
            xargs --null chown "${HOST_USER_ID}.${HOST_GROUP_ID}" --no-dereference || true >/dev/null 2>&1
    fi
}

function in_container_clear_tmp() {
    rm -rf /tmp/*
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}" >/dev/null 2>&1 || exit 1
}

function in_container_basic_sanity_check() {
    assert_in_container
    in_container_go_to_airflow_sources
    in_container_cleanup_pyc
    in_container_cleanup_pycache
}

export DISABLE_CHECKS_FOR_TESTS="missing-docstring,no-self-use,too-many-public-methods,protected-access,do-not-use-asserts"

function start_output_heartbeat() {
    MESSAGE=${1:-"Still working!"}
    INTERVAL=${2:=10}
    echo
    echo "Starting output heartbeat"
    echo

    bash 2>/dev/null <<EOF &
while true; do
  echo "\$(date): ${MESSAGE} "
  sleep ${INTERVAL}
done
EOF
    export HEARTBEAT_PID=$!
}

function stop_output_heartbeat() {
    kill "${HEARTBEAT_PID}" || true
    wait "${HEARTBEAT_PID}" || true 2>/dev/null
}

function dump_airflow_logs() {
    local dump_file
    dump_file=/files/airflow_logs_$(date "+%Y-%m-%d")_${CI_BUILD_ID}_${CI_JOB_ID}.log.tar.gz
    echo "###########################################################################################"
    echo "                   Dumping logs from all the airflow tasks"
    echo "###########################################################################################"
    pushd "${AIRFLOW_HOME}" >/dev/null 2>&1 || exit 1
    tar -czf "${dump_file}" logs
    echo "                   Logs dumped to ${dump_file}"
    popd >/dev/null 2>&1 || exit 1
    echo "###########################################################################################"
}

function install_airflow_from_wheel() {
    local extras
    extras="${1}"
    local airflow_package
    airflow_package=$(find /dist/ -maxdepth 1 -type f -name 'apache_airflow-[0-9]*.whl')
    echo
    echo "Found package: ${airflow_package}. Installing."
    echo
    if [[ -z "${airflow_package}" ]]; then
        >&2 echo
        >&2 echo "ERROR! Could not find airflow wheel package to install in dist"
        >&2 echo
        exit 4
    fi
    pip install "${airflow_package}${extras}"
}

function install_airflow_from_sdist() {
    local extras
    extras="${1}"
    local airflow_package
    airflow_package=$(find /dist/ -maxdepth 1 -type f -name 'apache-airflow-[0-9]*.tar.gz')
    echo
    echo "Found package: ${airflow_package}. Installing."
    echo
    if [[ -z "${airflow_package}" ]]; then
        >&2 echo
        >&2 echo "ERROR! Could not find airflow sdist package to install in dist"
        >&2 echo
        exit 4
    fi
    pip install "${airflow_package}${extras}"
}

function uninstall_airflow() {
    pip uninstall -y apache-airflow || true
    find /root/airflow/ -type f -print0 | xargs -0 rm -f --
}

function uninstall_all_pip_packages() {
    pip uninstall -y -r <(pip freeze)
}

function uninstall_providers() {
    local provider_packages_to_uninstall
    provider_packages_to_uninstall=$(pip freeze | grep apache-airflow-providers || true)
    if [[ -n ${provider_packages_to_uninstall} ]]; then
        echo "${provider_packages_to_uninstall}" | xargs pip uninstall -y || true 2>/dev/null
    fi
}

function uninstall_airflow_and_providers() {
    uninstall_providers
    uninstall_airflow
}

function install_released_airflow_version() {
    local version="${1}"
    echo
    echo "Installing released ${version} version of airflow without extras"
    echo

    rm -rf "${AIRFLOW_SOURCES}"/*.egg-info
    pip install "apache-airflow==${version}"
}

function install_local_airflow_with_eager_upgrade() {
    local extras
    extras="${1}"
    # we add eager requirements to make sure to take into account limitations that will allow us to
    # install all providers
    # shellcheck disable=SC2086
    pip install -e ".${extras}" ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS} \
        --upgrade --upgrade-strategy eager
}


function install_all_providers_from_pypi_with_eager_upgrade() {
    NO_PROVIDERS_EXTRAS=$(python -c 'import setup; print(",".join(setup.CORE_EXTRAS_REQUIREMENTS))')
    ALL_PROVIDERS_PACKAGES=$(python -c 'import setup; print(setup.get_all_provider_packages())')
    local packages_to_install=()
    local provider_package
    local res
    for provider_package in ${ALL_PROVIDERS_PACKAGES}
    do
        echo -n "Checking if ${provider_package} is available in PyPI: "
        res=$(curl --head -s -o /dev/null -w "%{http_code}" "https://pypi.org/project/${provider_package}/")
        if [[ ${res} == "200" ]]; then
            packages_to_install+=( "${provider_package}" )
            echo "${COLOR_GREEN}OK${COLOR_RESET}"
        else
            echo "${COLOR_YELLOW}Skipped${COLOR_RESET}"
        fi
    done
    echo "Installing provider packages: ${packages_to_install[*]}"
    # we add eager requirements to make sure to take into account limitations that will allow us to
    # install all providers. We install only those packages that are available in PyPI - we might
    # Have some new providers in the works and they might not yet be simply available in PyPI
    # Installing it with Airflow makes sure that the version of package that matches current
    # Airflow requirements will be used.
    # shellcheck disable=SC2086
    pip install -e ".[${NO_PROVIDERS_EXTRAS}]" "${packages_to_install[@]}" ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS} \
        --upgrade --upgrade-strategy eager

}

function install_all_provider_packages_from_wheels() {
    echo
    echo "Installing all provider packages from wheels"
    echo
    uninstall_providers
    pip install /dist/apache_airflow*providers_*.whl
}

function install_all_provider_packages_from_sdist() {
    echo
    echo "Installing all provider packages from .tar.gz"
    echo
    uninstall_providers
    pip install /dist/apache-airflow-*providers-*.tar.gz
}

function twine_check_provider_packages_from_wheels() {
    echo
    echo "Twine check of all provider packages from wheels"
    echo
    twine check /dist/apache_airflow*providers_*.whl
}

function twine_check_provider_packages_from_sdist() {
    echo
    echo "Twine check all provider packages from sdist"
    echo
    twine check /dist/apache-airflow-*providers-*.tar.gz
}

function setup_provider_packages() {
    export PACKAGE_TYPE="regular"
    export PACKAGE_PREFIX_UPPERCASE=""
    export PACKAGE_PREFIX_LOWERCASE=""
    export PACKAGE_PREFIX_HYPHEN=""
    if [[ ${VERBOSE} == "true" ]]; then
        OPTIONAL_VERBOSE_FLAG+=("--verbose")
    fi
    readonly PACKAGE_TYPE
    readonly PACKAGE_PREFIX_UPPERCASE
    readonly PACKAGE_PREFIX_LOWERCASE
    readonly PACKAGE_PREFIX_HYPHEN
}


function install_supported_pip_version() {
    group_start "Install supported PIP version ${AIRFLOW_PIP_VERSION}"
    pip install --disable-pip-version-check "pip==${AIRFLOW_PIP_VERSION}"
    group_end
}

function filename_to_python_module() {
    # Turn the file name into a python package name
    file="$1"
    no_leading_dotslash="${file#./}"
    no_py="${no_leading_dotslash/.py/}"
    no_init="${no_py/\/__init__/}"
    echo "${no_init//\//.}"
}

function import_all_provider_classes() {
    group_start "Import all Airflow classes"
    # We have to move to a directory where "airflow" is
    unset PYTHONPATH
    # We need to make sure we are not in the airflow checkout, otherwise it will automatically be added to the
    # import path
    cd /

    declare -a IMPORT_CLASS_PARAMETERS

    PROVIDER_PATHS=$(
        python3 <<EOF 2>/dev/null
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
    )
    export PROVIDER_PATHS

    echo "Searching for providers packages in:"
    echo "${PROVIDER_PATHS}"

    while read -r provider_path; do
        IMPORT_CLASS_PARAMETERS+=("--path" "${provider_path}")
    done < <(echo "${PROVIDER_PATHS}")

    python3 /opt/airflow/dev/import_all_classes.py "${IMPORT_CLASS_PARAMETERS[@]}"
    group_end
}

function in_container_set_colors() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
}


function check_missing_providers() {
    PACKAGE_ERROR="false"

    pushd "${AIRFLOW_SOURCES}/airflow/providers" >/dev/null 2>&1 || exit 1

    LIST_OF_DIRS_FILE=$(mktemp)
    find . -type d | sed 's!./!!; s!/!.!g' | grep -E 'hooks|operators|sensors|secrets|utils' \
        > "${LIST_OF_DIRS_FILE}"

    popd >/dev/null 2>&1 || exit 1

    # Check if all providers are included
    for PACKAGE in "${PROVIDER_PACKAGES[@]}"
    do
        if ! grep -E "^${PACKAGE}" <"${LIST_OF_DIRS_FILE}" >/dev/null; then
            echo "The package ${PACKAGE} is not available in providers dir"
            PACKAGE_ERROR="true"
        fi
        sed -i "/^${PACKAGE}.*/d" "${LIST_OF_DIRS_FILE}"
    done

    if [[ ${PACKAGE_ERROR} == "true" ]]; then
        echo
        echo "ERROR! Some packages from ${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py are missing in providers dir"
        exit 1
    fi

    if [[ $(wc -l < "${LIST_OF_DIRS_FILE}") != "0" ]]; then
        echo "ERROR! Some folders from providers package are not defined"
        echo "       Please add them to ${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py:"
        echo
        cat "${LIST_OF_DIRS_FILE}"
        echo

        rm "$LIST_OF_DIRS_FILE"
        exit 1
    fi
    rm "$LIST_OF_DIRS_FILE"
}

function get_providers_to_act_on() {
    group_start "Get all providers"
    if [[ -z "$*" ]]; then
        while IFS='' read -r line; do PROVIDER_PACKAGES+=("$line"); done < <(
            python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
                list-providers-packages
        )
    else
        if [[ "${1}" == "--help" ]]; then
            echo
            echo "Builds all provider packages."
            echo
            echo "You can provide list of packages to build out of:"
            echo
            python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
                list-providers-packages \
                | tr '\n ' ' ' | fold -w 100 -s
            echo
            echo
            exit
        fi
    fi
    group_end
}

# Starts group for GitHub Actions - makes logs much more readable
function group_start {
    if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
        echo "::group::${1}"
    else
        echo
        echo "${1}"
        echo
    fi
}

# Ends group for GitHub Actions
function group_end {
    if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
        echo -e "\033[0m"  # Disable any colors set in the group
        echo "::endgroup::"
    fi
}

export CI=${CI:="false"}
export GITHUB_ACTIONS=${GITHUB_ACTIONS:="false"}
