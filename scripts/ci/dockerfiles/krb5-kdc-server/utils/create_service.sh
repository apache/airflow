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
set -x

function usage() {
    CMDNAME="$(basename -- "$0")"

      echo """
Usage: ${CMDNAME} <service_name> <service_type> <keytab_file>

Creates an account for the service.

The service name is combined with the domain to create an principal name. If your service is named
\"trino\" a principal \"trino.example.com\" will be created.

The protocol can have any value, but it must be identical in the server and client configuration.
For example: HTTP.
"""
}

if [[ ! "$#" -eq 3 ]]; then
    echo "You must provide exactly three arguments."
    usage
    exit 1
fi

SERVICE_NAME=$1
SERVICE_TYPE=$2
KEYTAB_FILE=$3

DOMAIN_NAME=example.com
REALM_NAME=EXAMPLE.COM

cat << EOF | kadmin.local &>/dev/null
add_principal -randkey "${SERVICE_NAME}.${DOMAIN_NAME}@${REALM_NAME}"
add_principal -randkey "${SERVICE_TYPE}/${SERVICE_NAME}.${DOMAIN_NAME}@${REALM_NAME}"
ktadd -k ${KEYTAB_FILE} -norandkey "${SERVICE_NAME}.${DOMAIN_NAME}@${REALM_NAME}"
ktadd -k ${KEYTAB_FILE} -norandkey "${SERVICE_TYPE}/${SERVICE_NAME}.${DOMAIN_NAME}@${REALM_NAME}"
quit
EOF

chmod 777 "${KEYTAB_FILE}" &>/dev/null

echo "Created service: ${SERVICE_NAME}"
