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

# Bash sanity settings (error on exit, complain for undefined vars, error when pipe fails)
set -euxo pipefail

# TODO: We should think about removing those and moving them into docker-compose dependencies if possible.
# TODO: We might come up with just one airflow CI image not the SLIM/CI versions. That would simplify a lot.
# TODO: We could likely get rid of the multi-staging approach. It introduces a number of limitations
# TODO: As long as we decrease the size of the CI image, we should be fine with using single CI image for
# TODO: everything. Currently the CI image is about 1GB where CI_SLIM is around 0.5 GB.

export HADOOP_DISTRO="${HADOOP_DISTRO:="cdh"}"
export HADOOP_MAJOR="${HADOOP_MAJOR:="5"}"
export HADOOP_DISTRO_VERSION="${HADOOP_DISTRO_VERSION:="5.11.0"}"
export HADOOP_VERSION="${HADOOP_VERSION:="2.6.0"}"
export HIVE_VERSION="${HIVE_VERSION:="1.1.0"}"
export HADOOP_URL="${HADOOP_URL:="https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/"}"
export HADOOP_HOME="${HADOOP_HOME:="/tmp/hadoop-cdh"}"
export HIVE_HOME="${HIVE_HOME:="/tmp/hive"}"
export MINICLUSTER_BASE="${MINICLUSTER_BASE:="https://github.com/bolkedebruin/minicluster/releases/download/"}"
export MINICLUSTER_HOME="${MINICLUSTER_HOME:="/tmp/minicluster"}"
export MINICLUSTER_VER="${MINICLUSTER_VER:="1.1"}"
export KUBERNETES_VERSION=${KUBERNETES_VERSION:="v1.15.0"}
export KIND_VERSION=${KIND_VERSION:="v0.5.0"}

mkdir -pv "${HADOOP_HOME}"
mkdir -pv "${HIVE_HOME}"
mkdir -pv "${MINICLUSTER_HOME}"
mkdir -pv "/user/hive/warehouse"
chmod -R 777 "${HIVE_HOME}"
chmod -R 777 "/user/"

# Install Hadoop
export HADOOP_DOWNLOAD_URL="${HADOOP_URL}hadoop-${HADOOP_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz"
export HADOOP_TMP_FILE="/tmp/hadoop.tar.gz"

curl -sL "${HADOOP_DOWNLOAD_URL}" >"${HADOOP_TMP_FILE}"

tar xzf "${HADOOP_TMP_FILE}" --absolute-names --strip-components 1 -C "${HADOOP_HOME}"

rm "${HADOOP_TMP_FILE}"

# Install Hive
export HIVE_URL="${HADOOP_URL}hive-${HIVE_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz"
export HIVE_TMP_FILE="/tmp/hive.tar.gz"

curl -sL "${HIVE_URL}" >"${HIVE_TMP_FILE}"
tar xzf "${HIVE_TMP_FILE}" --strip-components 1 -C "${HIVE_HOME}"
rm "${HIVE_TMP_FILE}"

# Install minicluster
MINICLUSTER_URL="${MINICLUSTER_BASE}${MINICLUSTER_VER}/minicluster-${MINICLUSTER_VER}-SNAPSHOT-bin.zip"
MINICLUSTER_TMP_FILE="/tmp/minicluster.zip"

curl -sL "${MINICLUSTER_URL}" > "${MINICLUSTER_TMP_FILE}"
unzip "${MINICLUSTER_TMP_FILE}" -d "/tmp"
rm "${MINICLUSTER_TMP_FILE}"

# Install docker-ce
curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian stretch stable"
apt-get update
apt-get -y install --no-install-recommends docker-ce
apt-get autoremove -yqq --purge
apt-get clean && rm -rf /var/lib/apt/lists/*

# Install kubectl
curl -Lo kubectl \
  "https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/amd64/kubectl"
chmod +x kubectl
mv kubectl /usr/local/bin/kubectl

# Install kind
curl -Lo kind \
   "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
chmod +x kind
mv kind /usr/local/bin/kind
