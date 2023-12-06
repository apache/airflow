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

CONSTRAINTS_DIR="/files/constraints-${PYTHON_MAJOR_MINOR_VERSION}"

LATEST_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/original-${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
CONSTRAINTS_MARKDOWN_DIFF="${CONSTRAINTS_DIR}/diff-${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.md"
mkdir -pv "${CONSTRAINTS_DIR}"


if [[ ${AIRFLOW_CONSTRAINTS_MODE} == "constraints-no-providers" ]]; then
    NO_PROVIDERS_EXTRAS=$(python -c 'import setup; print(",".join(setup.CORE_EXTRAS_DEPENDENCIES.keys()))')
    CURRENT_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
    echo
    echo "UnInstall All PIP packages."
    echo
    uninstall_all_pip_packages
    echo
    echo "Install airflow with [${NO_PROVIDERS_EXTRAS}] extras only (uninstall all packages first)."
    echo
    install_local_airflow_with_eager_upgrade "[${NO_PROVIDERS_EXTRAS}]"
    cat <<EOF >"${CURRENT_CONSTRAINT_FILE}"
#
# This constraints file was automatically generated on $(date -u +'%Y-%m-%dT%H:%M:%SZ')
# via "eager-upgrade" mechanism of PIP. For the "${DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install just the 'bare' 'apache-airflow' package build from the HEAD of
# the branch, without installing any of the providers.
#
# Those constraints represent the "newest" dependencies airflow could use, if providers did not limit
# Airflow in any way.
#
EOF
elif [[ ${AIRFLOW_CONSTRAINTS_MODE} == "constraints-source-providers" ]]; then
    CURRENT_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
    echo
    echo "Providers are already installed from sources."
    echo
    cat <<EOF >"${CURRENT_CONSTRAINT_FILE}"
#
# This constraints file was automatically generated on $(date -u +'%Y-%m-%dT%H:%M:%SZ')
# via "eager-upgrade" mechanism of PIP. For the "${DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version of both
# 'apache-airflow' package and all available community provider packages.
#
# Those constraints represent the dependencies that are used by all pull requests when they are build in CI.
# They represent "latest" and greatest set of constraints that HEAD of the "apache-airflow" package should
# Install with "HEAD" of providers. Those are the only constraints that are used by our CI builds.
#
EOF
elif [[ ${AIRFLOW_CONSTRAINTS_MODE} == "constraints" ]]; then
    CURRENT_CONSTRAINT_FILE="${CONSTRAINTS_DIR}/${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
    echo
    echo "Install all providers from PyPI so that they are included in the constraints."
    echo
    install_all_providers_from_pypi_with_eager_upgrade
    cat <<EOF >"${CURRENT_CONSTRAINT_FILE}"
#
# This constraints file was automatically generated on $(date -u +'%Y-%m-%dT%H:%M:%SZ')
# via "eager-upgrade" mechanism of PIP. For the "${DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version for 'apache-airflow' but installs
# the providers from PIP-released packages at the moment of the constraint generation.
#
# Those constraints are actually those that regular users use to install released version of Airflow.
# We also use those constraints after "apache-airflow" is released and the constraints are tagged with
# "constraints-X.Y.Z" tag to build the production image for that version.
#
# This constraints file is meant to be used only in the "apache-airflow" installation command and not
# in all subsequent pip commands. By using a constraints.txt file, we ensure that solely the Airflow
# installation step is reproducible. Subsequent pip commands may install packages that would have
# been incompatible with the constraints used in Airflow reproducible installation step. Finally, pip
# commands that might change the installed version of apache-airflow should include "apache-airflow==X.Y.Z"
# in the list of install targets to prevent Airflow accidental upgrade or downgrade.
#
# Typical installation process of airflow for Python 3.8 is (with random selection of extras and custom
# dependencies added), usually consists of two steps:
#
# 1. Reproducible installation of airflow with selected providers (note constraints are used):
#
# pip install "apache-airflow[celery,cncf.kubernetes,google,amazon,snowflake]==X.Y.Z" \\
#     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-X.Y.Z/constraints-3.8.txt"
#
# 2. Installing own dependencies that are potentially not matching the constraints (note constraints are not
#    used, and apache-airflow==X.Y.Z is used to make sure there is no accidental airflow upgrade/downgrade.
#
# pip install "apache-airflow==X.Y.Z" "snowflake-connector-python[pandas]==2.9.0"
#
EOF
else
    echo
    echo "${COLOR_RED}Error! AIRFLOW_CONSTRAINTS_MODE has wrong value: '${AIRFLOW_CONSTRAINTS_MODE}' ${COLOR_RESET}"
    echo
    exit 1
fi

readonly AIRFLOW_CONSTRAINTS_MODE

CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${DEFAULT_CONSTRAINTS_BRANCH}/${AIRFLOW_CONSTRAINTS_MODE}-${PYTHON_MAJOR_MINOR_VERSION}.txt"
readonly CONSTRAINTS_LOCATION

touch "${LATEST_CONSTRAINT_FILE}"
curl --connect-timeout 60  --max-time 60 "${CONSTRAINTS_LOCATION}" --output "${LATEST_CONSTRAINT_FILE}" || true

echo
echo "Freezing constraints to ${CURRENT_CONSTRAINT_FILE}"
echo

pip freeze | sort | \
    grep -v "apache_airflow" | \
    grep -v "apache-airflow==" | \
    grep -v "@" | \
    grep -v "/opt/airflow" >>"${CURRENT_CONSTRAINT_FILE}"

echo
echo "Constraints generated in ${CURRENT_CONSTRAINT_FILE}"
echo

set +e
if diff "--ignore-matching-lines=#" --color=always "${LATEST_CONSTRAINT_FILE}" "${CURRENT_CONSTRAINT_FILE}"; then
    echo
    echo "${COLOR_GREEN}No changes in constraints - exiting${COLOR_RESET}"
    echo
    rm -f "${CONSTRAINTS_MARKDOWN_DIFF}"
    exit 0
fi

cat <<EOF >"${CONSTRAINTS_MARKDOWN_DIFF}"
# Dependencies updated for Python ${PYTHON_MAJOR_MINOR_VERSION}

\`\`\`diff
$(diff --unified=0 --ignore-matching-lines=# "${LATEST_CONSTRAINT_FILE}" "${CURRENT_CONSTRAINT_FILE}")
\`\`\`
EOF

echo
echo "Constraints error markdown generated in ${CONSTRAINTS_MARKDOWN_DIFF}"
echo

ls "${CONSTRAINTS_MARKDOWN_DIFF}"

exit 0
