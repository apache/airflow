#!/usr/bin/env bash

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cat /etc/hosts

FQDN=`hostname`

echo "hostname: ${FQDN}"

ADMIN="admin"
PASS="airflow"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cp ${DIR}/kdc.conf /etc/krb5kdc/kdc.conf

ln -sf /dev/urandom /dev/random

cp ${DIR}/kadm5.acl /etc/krb5kdc/kadm5.acl

cp ${DIR}/krb5.conf /etc/krb5.conf

# create admin
echo -e "${PASS}\n${PASS}" | kdb5_util create -s

echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc ${ADMIN}/admin"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc -randkey airflow"
echo -e "${PASS}\n${PASS}" | kadmin.local -q "addprinc -randkey airflow/${FQDN}"
kadmin.local -q "ktadd -k ${KRB5_KTNAME} airflow"
kadmin.local -q "ktadd -k ${KRB5_KTNAME} airflow/${FQDN}"

service krb5-kdc restart

# make sure the keytab is readable to anyone
chmod 664 ${KRB5_KTNAME}

# don't do a kinit here as this happens under super user privileges
# on travis
# kinit -kt ${KRB5_KTNAME} airflow

