#!/bin/bash
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
rm -rf docker-context-files/*.whl
rm -rf docker-context-files/*.tar.gz
export ANSWER="yes"
export CI="true"
export GITHUB_TOKEN=""
export PLATFORM=${PLATFORM:="linux/amd64,linux/arm64"}

breeze setup self-upgrade --use-current-airflow-sources

for PYTHON in 3.9 3.10 3.11 3.12
do
    breeze ci-image build \
         --builder airflow_cache \
         --run-in-parallel \
         --prepare-buildx-cache \
         --platform "${PLATFORM}" \
         --python ${PYTHON} \
         --verbose
done

rm -fv ./dist/* ./docker-context-files/*

breeze release-management prepare-provider-packages \
    --package-list-file ./prod_image_installed_providers.txt \
    --package-format wheel \
    --version-suffix-for-pypi dev0

breeze release-management prepare-airflow-package --package-format wheel --version-suffix-for-pypi dev0

mv -v ./dist/*.whl ./docker-context-files && chmod a+r ./docker-context-files/*

for PYTHON in 3.9 3.10 3.11 3.12
do
    breeze prod-image build \
         --builder airflow_cache \
         --run-in-parallel \
         --install-packages-from-context \
         --prepare-buildx-cache \
         --platform "${PLATFORM}" \
         --python ${PYTHON} \
         --verbose
done
