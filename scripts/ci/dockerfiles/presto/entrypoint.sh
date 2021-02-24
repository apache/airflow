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
            echo  "${COLOR_GREEN}OK.  ${COLOR_RESET}"
            break
        else
            echo -n "."
            MAX_CHECK=$((MAX_CHECK-1))
        fi
        if [[ ${MAX_CHECK} == 0 ]]; then
            echo "${COLOR_RED}ERROR: Maximum number of retries while checking service. Exiting.${COLOR_RESET}"
            break
        else
            sleep 1
        fi
    done
    if [[ ${RES} != 0 ]]; then
        echo "Service could not be started!"
        echo
        echo "${LAST_CHECK_RESULT}"
        echo
        return ${RES}
    fi
}

function log() {
  echo -e "\u001b[32m[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*\u001b[0m"
}

if [ -f /tmp/presto-initiaalized ]; then
  exec /bin/sh -c "$@"
fi

PRESTO_CONFIG_FILE="/usr/lib/presto/default/etc/config.properties"
JVM_CONFIG_FILE="/usr/lib/presto/default/etc/jvm.config"

log "Generate self-signed SSL certificate"
JKS_KEYSTORE_FILE=/tmp/ssl_keystore.jks
JKS_KEYSTORE_PASS=presto
keytool \
    -genkeypair \
    -alias "presto-ssl" \
    -keyalg RSA \
    -keystore "${JKS_KEYSTORE_FILE}" \
    -validity 10000 \
    -dname "cn=Unknown, ou=Unknown, o=Unknown, c=Unknown"\
    -storepass "${JKS_KEYSTORE_PASS}"

log "Set up SSL in ${PRESTO_CONFIG_FILE}"
cat << EOF >> "${PRESTO_CONFIG_FILE}"
http-server.https.enabled=true
http-server.https.port=7778
http-server.https.keystore.path=${JKS_KEYSTORE_FILE}
http-server.https.keystore.key=${JKS_KEYSTORE_PASS}
node.internal-address-source=FQDN
EOF

if [[ -n "${KRB5_CONFIG=}" ]]; then
    log "Set up Kerberos in ${PRESTO_CONFIG_FILE}"
    cat << EOF >> "${PRESTO_CONFIG_FILE}"
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

touch /tmp/presto-initiaalized

echo "Config: ${JVM_CONFIG_FILE}"
cat "${JVM_CONFIG_FILE}"

echo "Config: ${PRESTO_CONFIG_FILE}"
cat "${PRESTO_CONFIG_FILE}"

log "Executing cmd: ${*}"
exec /bin/sh -c "${@}"
