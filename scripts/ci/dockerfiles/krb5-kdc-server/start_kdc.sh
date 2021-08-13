#!/usr/bin/env bash

#
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

set -exuo pipefail

FQDN=$(hostname)
readonly FQDN
ADMIN="admin"
readonly ADMIN
PASS="airflow"
readonly PASS

KRB5_KTNAME=/etc/airflow.keytab
readonly KRB5_KTNAME

cat /etc/hosts
echo "hostname: ${FQDN}"
# create kerberos database
echo -e "${PASS}\n${PASS}" | kdb5_util create -s
# create admin
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc ${ADMIN}/admin"
# create airflow
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc -randkey airflow"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc -randkey airflow/${FQDN}"
kadmin.local -q "ktadd -k ${KRB5_KTNAME} airflow"
kadmin.local -q "ktadd -k ${KRB5_KTNAME} airflow/${FQDN}"

# Start services
/usr/local/bin/supervisord -n -c /etc/supervisord.conf
