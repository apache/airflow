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

# This autodetection of PYTHON_VERSION works in this sequence:
#
#    1) When running builds on DockerHub we cannot pass different environment
#       variables there, so we detect python version based on the image
#       name being build (airflow:master-python3.7 -> PYTHON_VERSION=3.7).
#       DockerHub adds index.docker.io in front of the image name.
#
#    2) When running builds on Travis CI. We use python version determined
#       from default python3 version available on the path. This way we
#       do not have to specify PYTHON_VERSION separately in each job.
#       We just specify which host python version is used for that job.
#       This makes a nice UI experience where you see python version in
#       Travis UI.
#
#    3) Running builds locally via scripts where we can pass PYTHON_VERSION
#       as environment variable.
#
#    4) Running builds locally for the first time with Breeze. By default
#       we determine the version based on default python3 version we have
#       in the host system (3.6 or 3.7) and we use this one.
#
#    5) Selecting python version with Breeze's --python switch. This will
#       override python version but it will also store the last used version
#       of python in .build directory so that it is automatically used next
#       time.

# shellcheck source=common/_default_branch.sh
. "${AIRFLOW_SOURCES}/common/_default_branch.sh"

# You can override DOCKERHUB_USER to use your own DockerHub account and play with your
# own docker images. In this case you can build images locally and push them
export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}

# You can override DOCKERHUB_REPO to use your own DockerHub repository and play with your
# own docker images. In this case you can build images locally and push them
export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}

# if BRANCH_NAME is not set it will be set to either SOURCE_BRANCH
# (if overridden by configuration) or default branch configured in /common/_default_branch.sh
export SOURCE_BRANCH=${SOURCE_BRANCH:=${DEFAULT_BRANCH}}

export BRANCH_NAME=${BRANCH_NAME:=${SOURCE_BRANCH}}

# IMAGE_NAME might come from DockerHub and we decide which PYTHON_VERSION to use based on it (see below)
# In case IMAGE_NAME is not set we determine PYTHON_VERSION based on the default python on the path
# So in virtualenvs it will use the virtualenv's PYTHON_VERSION and in DockerHub it will determine
# PYTHON_VERSION from the IMAGE_NAME set in the DockerHub build.
# See comment above in PYTHON_VERSION - we will be able to get rid of this cumbersomness when we switch
# to a different CI system and start pushing images to DockerHub rather than build it there.
export BASE_IMAGE_NAME=${IMAGE_NAME:=${DOCKERHUB_USER}/${DOCKERHUB_REPO}:${BRANCH_NAME}-python${PYTHON_VERSION:-3.6}}

# Remove index.docker.io/ prefix as it is added by default by DockerHub
export LOCAL_BASE_IMAGE_NAME=${BASE_IMAGE_NAME#index.docker.io/}

if [[ ! ${LOCAL_BASE_IMAGE_NAME} == ${DOCKERHUB_USER}/${DOCKERHUB_REPO}* ]]; then
    echo >&2
    echo >&2 "ERROR:  The ${LOCAL_BASE_IMAGE_NAME} does not start with ${DOCKERHUB_USER}/${DOCKERHUB_REPO}"
    echo >&2
    exit 1
fi

# Extract IMAGE_TAG from LOCAL_BASE_IMAGE_NAME (apache/airflow:master-python3.6 -> master-python3.6)
export IMAGE_TAG=${LOCAL_BASE_IMAGE_NAME##${DOCKERHUB_USER}/${DOCKERHUB_REPO}:}

# Extract TAG_PREFIX from IMAGE_TAG (master-python3.6 -> master)
export TAG_PREFIX=${IMAGE_TAG%-*}

# By that time no matter if we got the version from the IMAGE_NAME or we built
# IMAGE_NAME with PYTHON_VERSION - the LOCAL_BASE_IMAGE_NAME should end with the correct pythonX.Y
# So we Extract python version from image name. The bash construct below extracts last
# field after '-' delimiter, so for example 'airflow:master-python3.6'
# will produce LONG_PYTHON_VERSION_FROM_IMAGE=python3.6
export LONG_PYTHON_VERSION_FROM_IMAGE="${LOCAL_BASE_IMAGE_NAME##*-}"

# Verify that python version retrieved from image follows the expected pattern of python3.Y
if [[ ! ${LONG_PYTHON_VERSION_FROM_IMAGE} =~ python[23]\.[0-9]+ ]]; then
    echo >&2
    echo >&2 "ERROR:  Python version extracted from IMAGE_NAME does not match the python3.Y format"
    echo >&2
    echo >&2 "The IMAGE_NAME format should be '<BRANCH>-python[23].Y'"
    echo >&2
    exit 1
fi

# Unless python_version is forced, read actual python version from IMAGE NAME
export PYTHON_VERSION=${LONG_PYTHON_VERSION_FROM_IMAGE#python}
