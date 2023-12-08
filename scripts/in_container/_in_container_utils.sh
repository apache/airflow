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
    if [[ ${VERBOSE_COMMANDS:="false"} == "true" || ${VERBOSE_COMMANDS} == "True" ]]; then
        set -x
    fi
}

function in_container_go_to_airflow_sources() {
    pushd "${AIRFLOW_SOURCES}" >/dev/null 2>&1 || exit 1
}

function in_container_basic_sanity_check() {
    assert_in_container
    in_container_go_to_airflow_sources
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


function uninstall_all_pip_packages() {
    pip uninstall -y -r <(pip freeze)
}

function install_local_airflow_with_eager_upgrade() {
    local extras
    extras="${1}"
    # we add eager requirements to make sure to take into account limitations that will allow us to
    # install all providers
    # shellcheck disable=SC2086
    pip install ".${extras}" ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS=} \
        --upgrade --upgrade-strategy eager
}


function install_all_providers_from_pypi_with_eager_upgrade() {
    NO_PROVIDERS_EXTRAS=$(python -c 'import setup; print(",".join(setup.CORE_EXTRAS_DEPENDENCIES))')
    ALL_PROVIDERS_PACKAGES=$(python -c 'import setup; print(setup.get_all_provider_packages())')
    local packages_to_install=()
    local provider_package
    local res
    local chicken_egg_prefixes
    chicken_egg_prefixes=""
    if [[ ${CHICKEN_EGG_PROVIDERS=} != "" ]]; then
        echo "${COLOR_BLUE}Finding providers to install from dist: ${CHICKEN_EGG_PROVIDERS}${COLOR_RESET}"
        for chicken_egg_provider in ${CHICKEN_EGG_PROVIDERS}
        do
            chicken_egg_prefixes="${chicken_egg_prefixes} apache-airflow-providers-${chicken_egg_provider//./-}"
        done
        echo "${COLOR_BLUE}Chicken egg prefixes: ${chicken_egg_prefixes}${COLOR_RESET}"
        ls /dist/
    fi
    for provider_package in ${ALL_PROVIDERS_PACKAGES}
    do
        if [[ "${chicken_egg_prefixes}" == *"${provider_package}"* ]]; then
            # add the provider prepared in dist folder where chicken - egg problem is mitigated
            for file in /dist/"${provider_package//-/_}"*.whl
            do
                packages_to_install+=( "${file}" )
                echo "Added ${file} from dist folder as this is a chicken-egg package ${COLOR_GREEN}OK${COLOR_RESET}"
            done
            continue
        fi
        # Remove fab provider in main branch until we cut-off v2-8-test branch and change
        # version in main to 2.9.0 - otherwise we won't be able to generate PyPI constraints as
        # released common-io provider has apache-airflow>2.8.0 as dependency and we cannot install
        # the provider from PyPI
        if [[ ${provider_package} == "apache-airflow-providers-fab" ]]; then
            continue
        fi
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
    set -x
    pip install ".[${NO_PROVIDERS_EXTRAS}]" "${packages_to_install[@]}" ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS=} \
        --upgrade --upgrade-strategy eager
    set +x
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


# Starts group for GitHub Actions - makes logs much more readable
function group_start {
    if [[ ${GITHUB_ACTIONS:="false"} == "true" ||  ${GITHUB_ACTIONS} == "True" ]]; then
        echo "::group::${1}"
    else
        echo
        echo "${1}"
        echo
    fi
}

# Ends group for GitHub Actions
function group_end {
    if [[ ${GITHUB_ACTIONS:="false"} == "true" ||  ${GITHUB_ACTIONS} == "True" ]]; then
        echo -e "\033[0m"  # Disable any colors set in the group
        echo "::endgroup::"
    fi
}

export CI=${CI:="false"}
export GITHUB_ACTIONS=${GITHUB_ACTIONS:="false"}
