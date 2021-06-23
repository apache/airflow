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
# shellcheck source=scripts/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

function import_all_provider_classes() {
    group_start "Importing all classes"
    python3 "${AIRFLOW_SOURCES}/dev/import_all_classes.py" --path "airflow/providers"
    group_end
}

function verify_provider_packages_named_properly() {
    python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
        verify-provider-classes
}

function run_prepare_documentation() {
    local prepared_documentation=()
    local skipped_documentation=()
    local error_documentation=()
    local doc_only_documentation=()

    # Delete the remote, so that we fetch it and update it once, not once per package we build!
    git remote rm apache-https-for-providers 2>/dev/null || :

    local provider_package
    for provider_package in "${PROVIDER_PACKAGES[@]}"
    do
        set +e
        local res
        # There is a separate group created in logs for each provider package
        python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
            update-package-documentation \
            --version-suffix "${VERSION_SUFFIX_FOR_PYPI}" \
            --no-git-update \
            "${OPTIONAL_VERBOSE_FLAG[@]}" \
            "${OPTIONAL_RELEASE_VERSION_ARGUMENT[@]}" \
            "${OPTIONAL_NON_INTERACTIVE_FLAG[@]}" \
            "${provider_package}"
        res=$?
        if [[ ${res} == "64" ]]; then
            skipped_documentation+=("${provider_package}")
            continue
            echo "${COLOR_YELLOW}Skipping provider package '${provider_package}'${COLOR_RESET}"
        fi
        if [[ ${res} == "65" ]]; then
            echo "${COLOR_RED}Exiting as the user chose to quit!${COLOR_RESET}"
            exit 1
        fi
        if [[ ${res} == "66" ]]; then
            echo "${COLOR_YELLOW}Provider package '${provider_package}' marked as documentation-only!${COLOR_RESET}"
            doc_only_documentation+=("${provider_package}")
            continue
        fi
        if [[ ${res} != "0" ]]; then
            echo "${COLOR_RED}Error when generating provider package '${provider_package}'${COLOR_RESET}"
            error_documentation+=("${provider_package}")
            continue
        fi
        # There is a separate group created in logs for each provider package
        python3 "${PROVIDER_PACKAGES_DIR}/prepare_provider_packages.py" \
            update-changelog \
            "${OPTIONAL_VERBOSE_FLAG[@]}" \
            "${provider_package}"
        res=$?
        if [[ ${res} == "64" ]]; then
            skipped_documentation+=("${provider_package}")
            continue
            echo "${COLOR_YELLOW}Skipping provider package '${provider_package}'${COLOR_RESET}"
        fi
        if [[ ${res} == "65" ]]; then
            echo "${COLOR_RED}Exiting as the user chose to quit!${COLOR_RESET}"
            exit 1
        fi
        prepared_documentation+=("${provider_package}")
        set -e
    done
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    echo
    echo "Summary of prepared documentations:"
    echo
    if [[ "${#prepared_documentation[@]}" != "0" ]]; then
        echo "${COLOR_GREEN}   Success:${COLOR_RESET}"
        echo "${prepared_documentation[@]}" | fold -sw 100
    fi
    if [[ "${#skipped_documentation[@]}" != "0" ]]; then
        echo "${COLOR_YELLOW}   Skipped:${COLOR_RESET}"
        echo "${skipped_documentation[@]}" | fold -sw 100
    fi
    if [[ "${#doc_only_documentation[@]}" != "0" ]]; then
        echo "${COLOR_YELLOW}   Marked as doc-only (please commit those!):${COLOR_RESET}"
        echo "${doc_only_documentation[@]}" | fold -sw 100
    fi
    if [[ "${#error_documentation[@]}" != "0" ]]; then
        echo "${COLOR_RED}   Errors:${COLOR_RESET}"
        echo "${error_documentation[@]}" | fold -sw 100 | sed "s/^/  /" | sed "s/$/\\/"
    fi
    echo
    echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
    if [[ ${#error_documentation[@]} != "0" ]]; then
        echo
        echo "${COLOR_RED}There were errors when preparing documentation. Exiting! ${COLOR_RESET}"
        exit 1
    else
        if [[ ${GENERATE_PROVIDERS_ISSUE=} == "true" ]]; then
            echo
            python3 dev/provider_packages/prepare_provider_packages.py generate-issue-content "${prepared_documentation[@]}"
            echo
        fi
        echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}You can separately generate content of the issue to create to track testing status by running this command${COLOR_RESET}"
        echo "${COLOR_YELLOW}You can optionally exclude some PRs via --excluded-pr-list option containing coma-separated list of pr numbers${COLOR_RESET}"
        echo
        echo "python3 dev/provider_packages/prepare_provider_packages.py generate-issue-content \\"
        echo "${prepared_documentation[@]}"  | fold -sw 100 | sed "s/^/  /" | sed "s/$/ \\\\/"
        echo "  [--excluded-pr-list=\"\" ]"
        echo
        echo "${COLOR_YELLOW}You can also run it here by rerunning the prepare-providers-documentation${COLOR_RESET}"
        echo "${COLOR_YELLOW}with --generate-providers-issue and --forward-credentials flag (you have to have GITHUB_TOKEN variable set)${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}===================================================================================${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}Please review the updated files, classify the changelog entries and commit the changes!${COLOR_RESET}"
        echo

    fi
}


setup_provider_packages

cd "${AIRFLOW_SOURCES}" || exit 1

export PYTHONPATH="${AIRFLOW_SOURCES}"

install_supported_pip_version
import_all_provider_classes
verify_provider_packages_named_properly

OPTIONAL_RELEASE_VERSION_ARGUMENT=()
if [[ $# != "0" && ${1} =~ ^[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]$ ]]; then
    OPTIONAL_RELEASE_VERSION_ARGUMENT+=("--release-version" "${1}")
    shift
fi

OPTIONAL_NON_INTERACTIVE_FLAG=()
if [[ ${NON_INTERACTIVE=} == "true" ]]; then
    OPTIONAL_NON_INTERACTIVE_FLAG+=("--non-interactive")
fi


PROVIDER_PACKAGES=("${@}")
get_providers_to_act_on "${@}"

run_prepare_documentation

echo
echo "${COLOR_GREEN}All good! Airflow Provider's documentation generated!${COLOR_RESET}"
echo
