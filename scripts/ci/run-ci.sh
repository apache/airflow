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

set -x

DIRNAME=$(cd "$(dirname "$0")"; pwd)
AIRFLOW_ROOT="$DIRNAME/../.."

# Fix file permissions
sudo chown -R airflow.airflow . $HOME/.wheelhouse/ $HOME/.cache/pip

if [[ $PYTHON_VERSION == '3' ]]; then
  PIP=pip3
else
  PIP=pip
fi

sudo $PIP install --upgrade pip
sudo $PIP install tox

cd $AIRFLOW_ROOT && $PIP --version && tox --version

if [ -z "$KUBERNETES_VERSION" ];
then
  tox -e $TOX_ENV
else
  KUBERNETES_VERSION=${KUBERNETES_VERSION} $DIRNAME/kubernetes/setup_kubernetes.sh && \
  tox -e $TOX_ENV -- tests.contrib.minikube \
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
