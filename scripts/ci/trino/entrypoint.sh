#!/bin/bash

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

COLOR_RED="\u001b[31m"
COLOR_GREEN="\u001b[32m"
COLOR_BLUE="\u001b[34m"
COLOR_RESET="\u001b[0m"

function check_service {
    INTEGRATION_NAME=$1
    CALL=$2
    MAX_CHECK=${3}

    echo -n "${INTEGRATION_NAME}: "
    while true
    do
        set +e
        LAST_CHECK_RESULT=$(eval "${CALL}" 2>&1)
        RES=$?
        set -e
        if [[ ${RES} == 0 ]]; then
            echo -e "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo -e "${COLOR_RED}ERROR: Maximum number of retries while checking service. Exiting.${COLOR_RESET}"
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo -e "${COLOR_BLUE}Service could not be started!${COLOR_RESET}"
        echo
        echo -e "${COLOR_BLUE}${LAST_CHECK_RESULT}${COLOR_RESET}"
        echo
        return ${RES}
    fi
}

function log() {
  echo -e "${COLOR_GREEN}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*${COLOR_RESET}"
}

if [ -f /tmp/trino-initiaalized ]; then
  exec "$@"
fi

TRINO_CONFIG_FILE="/etc/trino/config.properties"
JVM_CONFIG_FILE="/etc/trino/jvm.config"

log "Generate self-signed SSL certificate"
JKS_KEYSTORE_FILE=/tmp/ssl_keystore.jks
JKS_KEYSTORE_PASS=trinodb
keytool \
    -genkeypair \
    -alias "trino-ssl" \
    -keyalg RSA \
    -keystore "${JKS_KEYSTORE_FILE}" \
    -validity 10000 \
    -dname "cn=Unknown, ou=Unknown, o=Unknown, c=Unknown"\
    -storepass "${JKS_KEYSTORE_PASS}"

log "Set up SSL in ${TRINO_CONFIG_FILE}"
cat << EOF >> "${TRINO_CONFIG_FILE}"
http-server.https.enabled=true
http-server.https.port=7778
http-server.https.keystore.path=${JKS_KEYSTORE_FILE}
http-server.https.keystore.key=${JKS_KEYSTORE_PASS}
node.internal-address-source=FQDN
EOF

if [[ -n "${KRB5_CONFIG=}" ]]; then
    log "Set up Kerberos in ${TRINO_CONFIG_FILE}"
    cat << EOF >> "${TRINO_CONFIG_FILE}"
http-server.https.enabled=true
http-server.https.port=7778
http-server.https.keystore.path=${JKS_KEYSTORE_FILE}
http-server.https.keystore.key=${JKS_KEYSTORE_PASS}
node.internal-address-source=FQDN
EOF

    log "Add debug Kerberos options to ${JVM_CONFIG_FILE}"
    cat <<"EOF" >> "${JVM_CONFIG_FILE}"
-Dsun.security.krb5.debug=true
-Dlog.enable-console=true
EOF
fi

log "Waiting for keytab:${KRB5_KTNAME}"
check_service "Keytab" "test -f ${KRB5_KTNAME}" 30

touch /tmp/trino-initiaalized

echo "Config: ${JVM_CONFIG_FILE}"
cat "${JVM_CONFIG_FILE}"

echo "Config: ${TRINO_CONFIG_FILE}"
cat "${TRINO_CONFIG_FILE}"

log "Executing cmd: ${*}"
exec "$@"
