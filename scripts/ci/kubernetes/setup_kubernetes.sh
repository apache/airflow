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

echo "This script downloads minikube, starts a driver=None minikube cluster, builds the airflow source\
 and docker image, and then deploys airflow onto kubernetes"
echo "For development, start minikube yourself (ie: minikube start) then run this script as you probably\
 do not want a driver=None minikube cluster"

DIRNAME=$(cd "$(dirname "$0")" && pwd)

# Fix file permissions
# TODO: change this - it should be Travis independent
if [[ "${TRAVIS:=}" == true ]]; then
  sudo chown -R travis.travis .
fi

"${DIRNAME}/minikube/start_minikube.sh"
"${DIRNAME}/docker/build.sh"

echo "Airflow environment on kubernetes is good to go!"
