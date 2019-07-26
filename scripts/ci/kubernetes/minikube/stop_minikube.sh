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

# This script was based on one made by @kimoonkim for kubernetes-hdfs

set -ex

if [[ ! -x /usr/local/bin/minikube ]]; then
  exit 0
fi

# Fix file permissions
# TODO: Change this - this should be Travis independent
if [[ "${TRAVIS}" == true ]]; then
  sudo chown -R travis.travis "${HOME}/.kube" "${HOME}/.minikube" 2>/dev/null || true
fi
set +e

if sudo minikube status; then
  sudo minikube delete
  sudo rm -rf "${HOME}/.kube" "${HOME}/.minikube"
  if [[ "${TRAVIS}" == true ]]; then
    sudo rm -rf /etc/kubernetes/*.conf
  fi
fi
set -e

sudo chown -R travis.travis . || true
