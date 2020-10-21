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

echo "Running helm tests"

chart_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../../chart/"

cat chart/files/pod-template-file.kubernetes-helm-yaml > chart/templates/pod-template-file.yaml

docker run -w /airflow-chart -v "$chart_directory":/airflow-chart \
  --entrypoint /bin/sh \
  aneeshkj/helm-unittest \
  -c "helm repo add stable https://kubernetes-charts.storage.googleapis.com; helm dependency update ; helm unittest ." \
  && rm chart/templates/pod-template-file.yaml
