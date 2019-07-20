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

# Helper bash functions.

# Wait for Kubernetes resources to be up and ready.
function _wait_for_ready () {
  local COUNT="$1"
  shift
  local EVIDENCE="$1"
  shift
  local ATTEMPTS=40
  echo "Waiting till ready (count: ${COUNT}): $*"
  while [[ "${COUNT}" < $("$@" 2>&1 | tail -n +2 | awk '{print $2}' | grep -c "${EVIDENCE}") ]];
  do
    if [[ "${ATTEMPTS}" = "1" ]]; then
      echo "Last run: $*"
      "$@" || true
      local command="$*"
      command="${command/get/describe}"
      ${command} || true
    fi
    (( ATTEMPTS-- )) || return 1
    sleep 5
  done
  "$@" || true
}

# Wait for all expected number of nodes to be ready
function k8s_all_nodes_ready () {
  local count="$1"
  shift
  _wait_for_ready "$count" "-v NotReady" kubectl get nodes
  _wait_for_ready "$count" Ready kubectl get nodes
}

function k8s_single_node_ready () {
  k8s_all_nodes_ready 1
}

# Wait for at least expected number of pods to be ready.
function k8s_at_least_n_pods_ready () {
  local COUNT="$1"
  shift
  local EVIDENCE="-E '([0-9])\/(\1)'"
  _wait_for_ready "${COUNT}" "{EVIDENCE}" kubectl get pods "$@"
}

function k8s_single_pod_ready () {
  k8s_at_least_n_pods_ready 1 "$@"
}
