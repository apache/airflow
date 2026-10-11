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

# Install airflow using regular 'pip install' command. This install airflow depending on the arguments:
#
# AIRFLOW_INSTALLATION_METHOD - determines where to install airflow form:
#             "." - installs airflow from local sources
#             "apache-airflow" - installs airflow from PyPI 'apache-airflow' package
#             "apache-airflow @ URL - installs from URL
#
# (example GitHub URL https://github.com/apache/airflow/archive/main.tar.gz)
#
# AIRFLOW_VERSION_SPECIFICATION - optional specification for Airflow version to install (
#                                 might be ==2.0.2 for example or <3.0.0
# UPGRADE_RANDOM_INDICATOR_STRING - if set with random value determines that dependency upgrade should be done
#                                   with highest resolution. It is random in order to trigger cache
#                                   invalidation when building images
#
# shellcheck shell=bash disable=SC2086
# shellcheck source=scripts/docker/common.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function has_providers_excluded_for_python() {
    local python_minor="${AIRFLOW_PYTHON_VERSION%.*}"
    local status
    python3 "$( dirname "${BASH_SOURCE[0]}" )/filter_ci_install_set.py" --root "${PWD}" \
        --python-minor "${python_minor}" --probe
    status=$?
    # 0 and 10 are the probe's two answers. Anything else is the probe itself failing, and must not
    # read as "nothing is excluded" - that would silently install the set this whole path avoids.
    case "${status}" in
        0) return 0 ;;
        10) return 1 ;;
        *)
            echo
            echo "${COLOR_RED}Could not determine excluded providers (exit ${status}).${COLOR_RESET}"
            echo
            exit "${status}"
            ;;
    esac
}

# uv sync --all-packages installs every workspace member, including providers excluded for this Python
# minor and the third-party packages only they reach. Exporting the lock for the members that are kept
# installs the same pins without them.
#
# Resolves the lock into the install set for this interpreter. This is the only step that can fail
# recoverably - a dependency missing from uv.lock stops the helper - so the caller tests it, and that
# suppresses errexit here: each command has to report its own failure.
function prepare_filtered_install_set() {
    local status
    local kept_package
    local -a kept_packages=()
    local -a package_args=()
    python3 "$( dirname "${BASH_SOURCE[0]}" )/filter_ci_install_set.py" --root "${PWD}" \
        --python-minor "${python_minor}" --packages "${work_dir}/packages.txt" \
        --editables "${work_dir}/editables.txt" || { status=$?; set +x; return "${status}"; }
    mapfile -t kept_packages < "${work_dir}/packages.txt"
    for kept_package in "${kept_packages[@]}"; do
        package_args+=(--package "${kept_package}")
    done
    # Exporting the lock for the kept members is the whole filter: uv emits their closure with the
    # hashes and the markers that already encode each provider's exclusions, so no second resolution
    # runs and the install cannot drift from uv.lock.
    uv export --frozen "${package_args[@]}" --group ci-image --no-emit-workspace \
        --output-file "${work_dir}/requirements.txt" || { status=$?; set +x; return "${status}"; }
}

# Pass "allow-relock" to let a stale lock be re-resolved instead of failing the build.
function install_workspace_without_excluded_providers() {
    local allow_relock="${1:-}"
    local python_minor="${AIRFLOW_PYTHON_VERSION%.*}"
    local work_dir="/tmp/ci-install-set"
    local python_bin
    local status=0
    # --no-config rides with --require-hashes: the install runs from the workspace root, and uv would
    # otherwise apply the root's constraint-dependencies, which --require-hashes rejects for not being
    # pinned with "==". It drops the rest of [tool.uv] too; of that, exclude-newer cannot matter for an
    # exact set installed with --no-deps, and no-build-isolation-package is passed back explicitly.
    local -a hash_args=(--require-hashes --no-config --no-build-isolation-package sphinx-redoc)
    local -a member_dirs=()
    local -a editable_args=()
    local member_dir
    mkdir -p "${work_dir}"
    # The PROD build stage puts the base interpreter (/usr/python/bin) ahead of the venv on PATH, so
    # `command -v python3` would point the install at the base interpreter instead of ${VIRTUAL_ENV}.
    # The CI image sets no VIRTUAL_ENV and keeps resolving python3 from PATH.
    python_bin="${VIRTUAL_ENV:+${VIRTUAL_ENV}/bin/python}"
    python_bin="${python_bin:-$(command -v python3)}"
    echo
    echo "${COLOR_BLUE}Installing the workspace without providers excluded for Python ${python_minor}.${COLOR_RESET}"
    echo
    set -x
    prepare_filtered_install_set || status=$?
    set +x
    if [[ ${status} -ne 0 ]]; then
        if [[ ${allow_relock} != "allow-relock" || ${AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION} != "true" ]]; then
            echo
            echo "${COLOR_RED}Failing because the filtered installation failed and fallback is disabled.${COLOR_RESET}"
            echo
            exit "${status}"
        fi
        echo
        echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies not reflected in uv.lock.${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}Falling back to re-resolving dependencies (uv lock, then filtered install).${COLOR_RESET}"
        echo
        set -x
        uv lock --no-python-downloads --no-managed-python
        prepare_filtered_install_set
        set +x
    fi
    # Nothing below may fail silently, and nothing has to say so: no caller tests this function, so
    # errexit is in force from here on.
    set -x
    # --exact removes what an earlier cached layer installed and the set no longer contains; plain
    # `uv pip install` would leave an excluded provider's dependencies behind in an incremental build.
    uv pip install --python "${python_bin}" --exact --no-deps --no-python-downloads --no-managed-python \
        "${hash_args[@]}" --no-binary lxml --no-binary xmlsec \
        --requirement "${work_dir}/requirements.txt"
    mapfile -t member_dirs < "${work_dir}/editables.txt"
    for member_dir in "${member_dirs[@]}"; do
        editable_args+=(--editable "${member_dir}")
    done
    uv pip install --python "${python_bin}" --no-deps --no-python-downloads --no-managed-python \
        --no-binary lxml --no-binary xmlsec "${editable_args[@]}"
    uv pip check --python "${python_bin}"
    set +x
}

function install_from_sources() {
    local extra_sync_flags
    extra_sync_flags=""
    if [[ ${VIRTUAL_ENV=} != "" ]]; then
        extra_sync_flags="--active"
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        if [[ ${PACKAGING_TOOL_CMD} == "pip" ]]; then
            set +x
            echo
            echo "${COLOR_RED}We only support uv not pip installation for upgrading dependencies!.${COLOR_RESET}"
            echo
            exit 1
        fi
        set +x
        echo
        echo "${COLOR_BLUE}Attempting to upgrade all packages to highest versions.${COLOR_RESET}"
        echo
        if has_providers_excluded_for_python; then
            # Re-lock at the highest resolution, then install that lock without the excluded providers.
            # uv sync would install every member, including the excluded ones and their dependencies.
            set -x
            uv lock --resolution highest --no-python-downloads --no-managed-python
            set +x
            install_workspace_without_excluded_providers
            return
        fi
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        set -x
        uv sync --all-packages --resolution highest --group ci-image \
            ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
            --no-python-downloads --no-managed-python
    else
        set +x
        if has_providers_excluded_for_python; then
            # Mirror the frozen path below: a lock that does not yet carry a new dependency makes the
            # helper stop with "<name> is not in the lock", so re-lock and filter again when allowed.
            install_workspace_without_excluded_providers "allow-relock"
            return
        fi
        echo
        echo "${COLOR_BLUE}Installing all packages from uv.lock (frozen).${COLOR_RESET}"
        echo
        # Use uv sync --frozen to install exactly what is pinned in uv.lock without re-resolving.
        # --no-binary-package is needed in order to avoid libxml and xmlsec using different version of
        # libxml2 (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        set -x
        if ! uv sync --all-packages --frozen --group ci-image \
            ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
            --no-python-downloads --no-managed-python; then
            set +x
            if [[ ${AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION} != "true" ]]; then
                echo
                echo "${COLOR_RED}Failing because frozen uv.lock installation failed and fallback is disabled.${COLOR_RESET}"
                echo
                exit 1
            fi
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies not reflected in uv.lock.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to re-resolving dependencies (uv sync without --frozen).${COLOR_RESET}"
            echo
            set -x
            uv sync --all-packages --group ci-image \
                ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
                --no-python-downloads --no-managed-python
            set +x
        fi
    fi
}

function install_from_external_spec() {
     local installation_command_flags
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "apache-airflow" ]]; then
        installation_command_flags="apache-airflow[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
    else
        echo
        echo "${COLOR_RED}The '${AIRFLOW_INSTALLATION_METHOD}' installation method is not supported${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}Supported methods are ('.', 'apache-airflow')${COLOR_RESET}"
        echo
        exit 1
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Remove airflow and all provider distributions installed before potentially${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} freeze | grep apache-airflow | xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} 2>/dev/null || true
        set +x
        echo
        echo "${COLOR_BLUE}Installing all packages with highest resolutions. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_TO_HIGHEST_RESOLUTION} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
        set +x
    else
        echo
        echo "${COLOR_BLUE}Installing all packages with constraints. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        if ! ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags} --constraint "${HOME}/constraints.txt"; then
            set +x
            if [[ ${AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION} != "true" ]]; then
                echo
                echo "${COLOR_RED}Failing because constraints installation failed and fallback is disabled.${COLOR_RESET}"
                echo
                exit 1
            fi
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies conflicting with constraints.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to no-constraints installation.${COLOR_RESET}"
            echo
            set -x
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_IF_NEEDED} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
            set +x
        fi
    fi
}


function install_airflow_when_building_images() {
    # Remove mysql from extras if client is not going to be installed
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
        echo "${COLOR_YELLOW}MYSQL client installation is disabled. Extra 'mysql' installations were therefore omitted.${COLOR_RESET}"
    fi
    # Remove postgres from extras if client is not going to be installed
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
        echo "${COLOR_YELLOW}Postgres client installation is disabled. Extra 'postgres' installations were therefore omitted.${COLOR_RESET}"
    fi
    # Determine the installation_command_flags based on AIRFLOW_INSTALLATION_METHOD method
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "." ]]; then
        install_from_sources
    else
        install_from_external_spec
    fi
    set +x
    common::install_packaging_tools
    echo
    echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
    echo
    # We use pip check here to make sure that whatever `uv` installs, is also "correct" according to `pip`
    pip check
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow_when_building_images
