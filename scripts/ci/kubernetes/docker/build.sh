#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

IMAGE=${1:-airflow}
TAG=${2:-latest}
DIRNAME=$(cd "$(dirname "$0")"; pwd)
AIRFLOW_ROOT="$DIRNAME/../../../.."

set -e

if [ "${VM_DRIVER:-none}" != "none" ]; then
    ENVCONFIG=$(minikube docker-env)
    if [ $? -eq 0 ]; then
      eval $ENVCONFIG
    fi
fi

echo "Airflow directory $AIRFLOW_ROOT"
echo "Airflow Docker directory $DIRNAME"

if [[ ${PYTHON_VERSION} == '3' ]]; then
  PYTHON_DOCKER_IMAGE=python:3.6-slim
else
  PYTHON_DOCKER_IMAGE=python:2.7-slim
fi

cd $AIRFLOW_ROOT
docker run -ti --rm -e SLUGIFY_USES_TEXT_UNIDECODE -v ${AIRFLOW_ROOT}:/airflow \
    -w /airflow ${PYTHON_DOCKER_IMAGE} ./scripts/ci/kubernetes/docker/compile.sh \

sudo rm -rf ${AIRFLOW_ROOT}/airflow/www_rbac/node_modules

echo "Copy distro $AIRFLOW_ROOT/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz"
cp $AIRFLOW_ROOT/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz
cd $DIRNAME && docker build --pull $DIRNAME --tag=${IMAGE}:${TAG}
rm $DIRNAME/airflow.tar.gz
