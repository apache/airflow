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

DIRNAME=$(cd "$(dirname "$0")"; pwd)

FQDN=`hostname`
ADMIN="admin"
PASS="airflow"
KRB5_KTNAME=/etc/airflow.keytab

cat /etc/hosts
echo "hostname: ${FQDN}"

sudo cp $DIRNAME/krb5/krb-conf/client/krb5.conf /etc/krb5.conf

echo -e "${PASS}\n${PASS}" | sudo kadmin -p ${ADMIN}/admin -w ${PASS} -q "addprinc -randkey airflow/${FQDN}"
sudo kadmin -p ${ADMIN}/admin -w ${PASS} -q "ktadd -k ${KRB5_KTNAME} airflow"
sudo kadmin -p ${ADMIN}/admin -w ${PASS} -q "ktadd -k ${KRB5_KTNAME} airflow/${FQDN}"
sudo chmod 0644 ${KRB5_KTNAME}
