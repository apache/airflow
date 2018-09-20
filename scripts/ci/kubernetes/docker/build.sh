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

#ENVCONFIG=$(minikube docker-env)
#if [ $? -eq 0 ]; then
#  eval $ENVCONFIG
#fi
_UNAME_OUT=$(uname -s)
case "${_UNAME_OUT}" in
    Linux*)     _MY_OS=linux;;
    Darwin*)    _MY_OS=darwin;;
    *)          echo "${_UNAME_OUT} is not unsupported."
                exit 1;;
esac
echo "Local OS is ${_MY_OS}"

if [ "$_MY_OS" = "linux" ]; then
    export _REGISTRY_IP=10.192.0.1
else
    export _REGISTRY_IP=`ipconfig getifaddr en0`
fi
echo "Airflow directory $AIRFLOW_ROOT"
echo "Airflow Docker directory $DIRNAME"

cd $AIRFLOW_ROOT
python setup.py sdist -q
echo "Copy distro $AIRFLOW_ROOT/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz"
cp $AIRFLOW_ROOT/dist/*.tar.gz ${DIRNAME}/airflow.tar.gz
cd $DIRNAME && docker build --pull $DIRNAME --tag=${_REGISTRY_IP}:5000/${IMAGE}:${TAG}
docker push ${_REGISTRY_IP}:5000/${IMAGE}:${TAG}
docker exec kube-node-1 docker pull ${_REGISTRY_IP}:5000/${IMAGE}:${TAG}
rm $DIRNAME/airflow.tar.gz
