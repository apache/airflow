#!/usr/bin/env bash

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

set -o xtrace
set -e

echo "This script downloads builds the airflow source and docker image, uploads to repository, and then deploys airflow onto existing kubernetes cluster"

DIRNAME=$(cd "$(dirname "$0")"; pwd)

#$DIRNAME/docker/build.sh $1 $2
#docker push $1:$2
IMAGE_STRING=$1:$2
echo $IMAGE_STRING
cat $DIRNAME/kube/airflow_testing.yaml.template | sed -e "s|{IMAGE_STRING}|image: $IMAGE_STRING|g" > $DIRNAME/kube/airflow_testing.yaml
cat $DIRNAME/kube/configmaps.yaml | sed -e "s|worker_container_repository.*|worker_container_repository = $1|g" |\
sed -e "s|worker_container_tag.*|worker_container_tag = $2|g" |\
sed -e "s|worker_container_image_pull_policy.*|worker_container_image_pull_policy = Always|g" \
> $DIRNAME/kube/configmaps_testing.yaml
$DIRNAME/kube/deploy.sh local_testing

echo "Airflow environment on kubernetes is good to go!"
