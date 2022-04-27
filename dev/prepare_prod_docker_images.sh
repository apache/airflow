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
AIRFLOW_SOURCES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
export AIRFLOW_SOURCES_DIR

set -e

CURRENT_PYTHON_MAJOR_MINOR_VERSIONS=("3.7" "3.8" "3.9" "3.10")

usage() {
    local cmdname
    cmdname="$(basename -- "$0")"

    cat << EOF
Usage: ${cmdname} <AIRFLOW_VERSION>

Prepares prod docker images for the version specified.

EOF
}

if [[ "$#" -ne 1 ]]; then
    >&2 echo "You must provide Airflow version."
    usage
    exit 1
fi

airflow_version="${1}"

for python_version in "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"
do
  image_name="apache/airflow:${airflow_version}-python${python_version}"
  docker buildx build --builder "airflow_cache" \
     --build-arg PYTHON_BASE_IMAGE="python:${python_version}-slim-bullseye" \
     --build-arg AIRFLOW_VERSION="${airflow_version}" \
     --platform linux/amd64,linux/arm64 . -t "${image_name}" --push
  docker pull "${image_name}"
  breeze verify-prod-image --image-name "${image_name}"
  if [[ ${python_version} == "3.7" ]]; then
      docker tag "${image_name}" "apache/airflow:${airflow_version}"
      docker push "apache/airflow:${airflow_version}"
  fi
done

if [[ ${INSTALL_AIRFLOW_VERSION} =~ .*rc.* ]]; then
    echo
    echo "Skipping tagging latest as this is an rc version"
    echo
    exit
fi

echo "Should we tag version ${1} with latest tag [y/N]"
read -r RESPONSE

if [[ ${RESPONSE} == 'n' || ${RESPONSE} = 'N' ]]; then
    echo
    echo "Skip tagging the image with latest tag."
    echo
    exit
fi

for python_version in "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"
do
    docker tag "apache/airflow:${airflow_version}-python${python_version}" \
        "apache/airflow:latest-python${python_version}"
    docker push "apache/airflow:latest-python${python_version}"
done

docker tag "apache/airflow:${airflow_version}" "apache/airflow:latest"
docker push "apache/airflow:latest"
