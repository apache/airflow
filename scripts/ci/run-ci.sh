#!/usr/bin/env bash

#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -xe

sudo chmod -R g+w /home/airflow/

#####################################################################
### Configure environment

DIRNAME=$(cd "$(dirname "$0")"; pwd)
AIRFLOW_ROOT="$DIRNAME/../.."

export PATH=$PATH:/home/airflow/.local/bin

# Check if variables are set, otherwise set some defaults
cd ${AIRFLOW_ROOT} && ${PYTHON} -m pip --version
cp -f ${DIRNAME}/airflow_travis.cfg ${AIRFLOW_HOME}/unittests.cfg

echo Python version: ${PYTHON}
echo Backend: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN}

#####################################################################
### Start MiniCluster
java -cp "/tmp/minicluster-1.1-SNAPSHOT/*" com.ing.minicluster.MiniCluster > /tmp/minicluster.txt &

# Set up ssh keys
echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
#ln -s ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
chmod 600 ~/.ssh/*

# SSH Service
sudo service ssh restart


#####################################################################
### Configure Kerberos

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

kinit -kt ${KRB5_KTNAME} airflow

#####################################################################
### Prepare MySQL Database

MYSQL_HOST=mysql
mysql -h ${MYSQL_HOST} -u root -e 'DROP DATABASE IF EXISTS airflow; CREATE DATABASE airflow'

#####################################################################
### Insert some data into MySQL

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="${DIR}/data"
DATA_FILE="${DATA_DIR}/baby_names.csv"
DATABASE=airflow_ci
HOST=mysql

mysqladmin -h ${HOST} -u root create ${DATABASE} || echo "Database already exists"
mysql -h ${HOST} -u root < ${DATA_DIR}/mysql_schema.sql
mysqlimport --local -h ${HOST} -u root --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --ignore-lines=1 ${DATABASE} ${DATA_FILE}

#####################################################################
### Prepare Python

# Generate the `airflow` executable
${PYTHON} setup.py develop -q

${PYTHON} -m pip wheel --progress-bar off -w /home/airflow/.wheelhouse -f /home/airflow/.wheelhouse -e .[devel_ci]
${PYTHON} -m pip install --progress-bar off --find-links=/home/airflow/.wheelhouse --no-index -e .[devel_ci]

echo "Initializing the DB"
yes | airflow initdb
yes | airflow resetdb

# add test/contrib to PYTHONPATH
export PYTHONPATH=${PYTHONPATH:-$AIRFLOW_ROOT/tests/test_utils}

# For impersonation tests running on SQLite on Travis, make the database world readable so other
# users can update it
AIRFLOW_DB="$HOME/airflow.db"

if [ -f "${AIRFLOW_DB}" ]; then
  chmod a+rw "${AIRFLOW_DB}"
  chmod g+rwx "${AIRFLOW_HOME}"
fi

# any argument received is overriding the default nose execution arguments:
nose_args=$@

if [ -z "$nose_args" ]; then
  nose_args="--with-coverage \
  --cover-erase \
  --cover-html \
  --cover-package=airflow \
  --cover-html-dir=airflow/www/static/coverage \
  --with-ignore-docstrings \
  --rednose \
  --with-timer \
  -v \
  --logging-level=DEBUG"
fi


if [ -z "$KUBERNETES_VERSION" ];
then
    nosetests ${nose_args}
else
  KUBERNETES_VERSION=${KUBERNETES_VERSION} $DIRNAME/kubernetes/setup_kubernetes.sh && \
  nosetests -- tests.contrib.minikube \
        --with-coverage \
        --cover-erase \
        --cover-html \
        --cover-package=airflow \
        --cover-html-dir=airflow/www/static/coverage \
        --with-ignore-docstrings \
        --rednose \
        --with-timer \
        -v \
        --logging-level=DEBUG
fi

scripts/ci/6-check-license.sh
