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

INSTALL_DIR="/files/opt/az"
BIN_PATH="/files/bin/az"

if [[ $# != "0" && ${1} == "--reinstall" ]]; then
    rm -rf "${INSTALL_DIR}"
    rm -f "${BIN_PATH}"
fi

hash -r

if command -v az; then
    echo 'The "az" command found. Installation not needed. Run with --reinstall to reinstall'
    exit 1
fi


if [[ -e ${INSTALL_DIR} ]]; then
    echo "The install directory (${INSTALL_DIR}) already exists. This may mean az CLI is already installed."
    echo "Run with --reinstall to reinstall."
    exit 1
fi

virtualenv /files/opt/az

# ignore the source
# shellcheck source=/dev/null
source /files/opt/az/bin/activate

pip install azure-cli

cat >/files/opt/az/az <<EOF
#!/usr/bin/env bash

source /files/opt/az/bin/activate

az "\${@}"
EOF

chmod a+x /files/opt/az/az

ln -s /files/opt/az/az "${BIN_PATH}"

# Sanity check
if ! command -v az > /dev/null; then
    echo 'Installation failed. The command "az" was not found.'
    exit 1
fi

echo 'Installation complete.'
