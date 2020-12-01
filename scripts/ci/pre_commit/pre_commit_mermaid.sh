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
set -euo pipefail
export PRINT_INFO_FROM_SCRIPTS="false"

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

if ! command -v npm; then
    echo 'You need to have npm installed in order to generate .mermaid graphs automatically.'
    echo
    # Do not fail. This is no problem if those images are not regenerated.
    exit 0
fi

TMP_FILE="${CACHE_TMP_FILE_DIR}/tmp.mermaid"
readonly TMP_FILE

cd "${AIRFLOW_SOURCES}"

MERMAID_INSTALLATION_DIR="${AIRFLOW_SOURCES}/.build/mermaid/"
readonly MERMAID_INSTALLATION_DIR

MERMAID_CONFIG_FILE="${MERMAID_INSTALLATION_DIR}/mermaid-config.json"
readonly MERMAID_CONFIG_FILE

MERMAID_CLI="${MERMAID_INSTALLATION_DIR}/node_modules/.bin/mmdc"
readonly  MERMAID_CLI

export NODE_VIRTUAL_ENV="${MERMAID_INSTALLATION_DIR}"
readonly NODE_VIRTUAL_ENV

if [[ -f "${MERMAID_CLI}" ]]; then
    mermaid_installed="true"
else
    mermaid_installed="false"
fi

# shellcheck disable=SC2064
traps::add_trap "rm -rf '${TMP_FILE}'" EXIT HUP INT TERM

for file in "${@}"
do
    basename_file=${AIRFLOW_SOURCES}/"$(dirname "${file}")/$(basename "${file}" .mermaid)"
    md5sum_file="${basename_file}.md5"
    if ! diff "${md5sum_file}" <(md5sum "${file}"); then
        if [[ ${mermaid_installed} != "true" ]]; then
            echo "Installing mermaid"
            mkdir -p "${MERMAID_INSTALLATION_DIR}/node_modules"
            pushd "${MERMAID_INSTALLATION_DIR}"
            npm install mermaid.cli
            cat >"${MERMAID_CONFIG_FILE}" <<EOF
{
  "theme": "default",
  "themeCSS": ".label foreignObject { overflow: visible; }"
}
EOF
            mermaid_installed="true"
            popd
        fi
        echo "Running generation for ${file}"
        rm -f "${basename_file}.png"
        rm -f "${basename_file}.md5"
        # unfortunately mermaid does not handle well multiline comments and we need licence comment
        # Stripping them manually :(. Multiline comments are coming in the future
        # https://github.com/mermaid-js/mermaid/issues/1249
        grep -v "^%%" <"${file}" > "${TMP_FILE}"
        mkdir -p "${MERMAID_INSTALLATION_DIR}"

        "${MERMAID_CLI}" \
            -i "${TMP_FILE}" \
            -w 2048 \
            -o "${basename_file}.png" \
            -c "${MERMAID_CONFIG_FILE}"
        if [ -f "${basename_file}.png" ]; then
            md5sum "${file}" >"${md5sum_file}"
            echo
            echo "Successfully generated: ${basename_file}.png"
            echo "Successfully updated: ${basename_file}.md5"
            echo
            echo "Please add both files and commit them to repository"
            echo
        else
            echo
            echo "\e[31mERROR: Could not generate ${basename_file}.png  ${COLOR_RESET}"
            echo
            exit 1
        fi
    else
        echo "Skip regenerating file ${file} -> it's hash did not change in ${md5sum_file}"
    fi
done
