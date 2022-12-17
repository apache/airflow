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

# This is an example docker build script. It is not intended for PRODUCTION use
set -euo pipefail
AIRFLOW_SOURCES="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../" && pwd)"
TEMP_DOCKER_DIR=$(mktemp -d)
pushd "${TEMP_DOCKER_DIR}"

cp "${AIRFLOW_SOURCES}/Dockerfile" "${TEMP_DOCKER_DIR}"
# [START build]
mkdir -p docker-context-files

# TODO: figure out how to add fake private repo
# TODO: confirm constraints url is used?

cat <<EOF >./docker-context-files/requirements.txt
# Redundant to include index url here, but it will make the build fail
# if pip fails to reach out and touch the url passed into the docker build
# via --secret as seen shortly below
--extra-index-url=https://pypi.my-company.net/repository/pypi/simple
my-company-package
EOF

export DOCKER_BUILDKIT=1

# Certainly, there are other ways to provide extra index url.  In this example
# we assume it's in the environment already picked up locally by pip and dump
# into a temp file descriptor
# https://wiki.bash-hackers.org/syntax/expansion/proc_subst#examples
#
# (insert Clue movie "...That's one way it could have happened..." meme here)
docker build . \
    --secret id=pipextraindexurl,src=<(echo -n "${PIP_EXTRA_INDEX_URL:?}") \
    --build-arg DOCKER_CONTEXT_FILES=./docker-context-files \
    --tag "my-company-airflow:0.0.1"
docker run -it my-company-airflow:0.0.1 python -c 'import my_company_package; import sys; sys.exit(0)' && \
    echo "Success! Private repo package installed" && echo
# [END build]
docker rmi --force "my-company-airflow:0.0.1"
popd
rm -rf "${TEMP_DOCKER_DIR}"
