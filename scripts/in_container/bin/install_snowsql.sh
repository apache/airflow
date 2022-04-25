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

INSTALL_DIR="/files/opt/snowsql"
BIN_PATH="/files/bin/snowsql"

if [[ $# != "0" && ${1} == "--reinstall" ]]; then
    rm -rf "${INSTALL_DIR}"
    rm -f "${BIN_PATH}"
fi

hash -r

if command -v snowsql; then
    echo 'The "snowsql" command found. Installation not needed. Run with --reinstall to reinstall'
    exit 1
fi

SNOWSQL_VERSION=1.2.21
DOWNLOAD_URL="https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-${SNOWSQL_VERSION}-linux_x86_64.bash"

if [[ -e ${BIN_PATH} ]]; then
    echo "The binary file (${BIN_PATH}) already exists. This may mean snowsql is already installed."
    echo "Run with --reinstall to reinstall."
    exit 1
fi

TMP_DIR="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf ${TMP_DIR}" EXIT

mkdir -p "${INSTALL_DIR}"
echo "Downloading from ${DOWNLOAD_URL}"
curl -# --fail "${DOWNLOAD_URL}" --output "${TMP_DIR}/snowsql.bash"

mkdir -p "${TMP_DIR}/home"

# By default, SnowSQL installs to ~/.snowsql.
# This directory can be changed with SNOWSQL_DOWNLOAD_DIR environment variable at runtime,
# but the installer does not support this yet, so we need to change HOME environment variable and
# copy necessary files
HOME="${TMP_DIR}/home" \
    SNOWSQL_LOGIN_SHELL=/dev/null \
    SNOWSQL_DEST="${INSTALL_DIR}/bin" \
    bash "${TMP_DIR}/snowsql.bash"
mkdir -p "${INSTALL_DIR}/download/"
mv "${TMP_DIR}/home/.snowsql/${SNOWSQL_VERSION}" "${INSTALL_DIR}/download/${SNOWSQL_VERSION}"

# Create wrapper to change default installation directory.
cat > "${INSTALL_DIR}/snowsql" <<EOF
#!/usr/bin/env bash

export SNOWSQL_DOWNLOAD_DIR="${INSTALL_DIR}/download/"
exec /files/opt/snowsql/bin/snowsql "\${@}"
EOF

chmod a+x "${INSTALL_DIR}/snowsql"

ln -s "${INSTALL_DIR}/snowsql" "${BIN_PATH}"

# Coherence check
if ! command -v snowsql > /dev/null; then
    echo 'Installation failed. The command "snowsql" was not found.'
    exit 1
fi

echo 'Installation complete.'
