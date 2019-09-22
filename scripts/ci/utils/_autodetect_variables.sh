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

#
# Autodetects common variables depending on the environment it is run in
#
# This autodetection of PYTHON_VERSION works in this sequence:
##
#    1) When running builds on Travis CI we use python version determined
#       from default python3 available on the path. This way we
#       do not have to specify PYTHON_VERSION separately in each job.
#       (We do not need it it in Travis). We just specify which host python
#       version is used for that job. This makes a nice UI experience where
#       you see python version in Travis UI.
#
#    2) Running builds locally via scripts where we can pass PYTHON_VERSION
#       as environment variable. If variable is not set we use the same detection
#       as in Travis CI.
#

# Assume AIRFLOW_SOURCES are set to point to sources of Airflow

# shellcheck source=scripts/ci/utils/_default_branch.sh
. "${AIRFLOW_SOURCES}/scripts/ci/utils/_default_branch.sh"

#  Output:
#
#  * DEFAULT_BRANCH                - default branch used (master/v1-10-test)
#  * DOCKERHUB_USER/DOCKERHUB_REPO - DockerHub user/repo  (<USER>/<REPO>)
#  * BRANCH_NAME                   - branch for the build
#  * PYTHON_VERSION                - actually used PYTHON_VERSION (might be overwritten)
#  * CI/NON_CI                     - ("true"/"false") to distinguish CI vs. NON_CI build
#  * AIRFLOW_VERSION               - version of Airflow from sources
#  * USE_DOCKER_CACHE              - set to false if CRON job detected
#
function autodetect_variables() {
    # You can override DOCKERHUB_USER to use your own DockerHub account and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_USER=${DOCKERHUB_USER:="apache"}

    # You can override DOCKERHUB_REPO to use your own DockerHub repository and play with your
    # own docker images. In this case you can build images locally and push them
    export DOCKERHUB_REPO=${DOCKERHUB_REPO:="airflow"}

    # if BRANCH_NAME is not set it will be set to either SOURCE_BRANCH
    # (if overridden by configuration) or default branch configured in /common/_default_branch.sh
    SOURCE_BRANCH=${SOURCE_BRANCH:=${DEFAULT_BRANCH}}

    BRANCH_NAME=${BRANCH_NAME:=${SOURCE_BRANCH}}

    echo
    echo "Branch: ${BRANCH_NAME}"
    echo

    # You can override AUTODETECT_PYTHON_BINARY if you want to use different version but for now we assume
    # that the binary used will be the default python 3 version available on the path
    # unless it is overridden with PYTHON_VERSION variable in the next step
    if ! AUTODETECT_PYTHON_BINARY=${AUTODETECT_PYTHON_BINARY:=$(command -v python3)}; then
        echo >&2
        echo >&2 "Error: You must have python3 in your PATH"
        echo >&2
        exit 1
    fi

    # Determine python version. This can be either specified by PYTHON_VERSION variable or it is
    # auto-detected from the version of the python3 binary (or AUTODETECT_PYTHON_BINARY you specify as
    # environment variable).
    export PYTHON_VERSION=${PYTHON_VERSION:=$(${AUTODETECT_PYTHON_BINARY} -c \
     'import sys;print("%s.%s" % (sys.version_info.major, sys.version_info.minor))')}

    IMAGE_TAG=${BRANCH_NAME}-python${PYTHON_VERSION}

    # Extract TAG_PREFIX from IMAGE_TAG (master-python3.6 -> master)
    TAG_PREFIX=${IMAGE_TAG%-*}
    export TAG_PREFIX

    # Disable writing .pyc files - slightly slower imports but not messing around when switching
    # Python version and avoids problems with root-owned .pyc files in host
    export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:="true"}

    # Check if we are running in the CI environment
    CI=${CI:="false"}

    if [[ "${CI}" == "true" ]]; then
        NON_CI="false"
    else
        NON_CI="true"
    fi
    export CI
    export NON_CI

    # Determine version of the Airflow from version.py
    AIRFLOW_VERSION=$(cat airflow/version.py - << EOF | python
print(version.replace("+",""))
EOF
)
    export AIRFLOW_VERSION

    # Should dependencies of docker compose be started - default is true
    DEPS=${DEPS:="true"}
    export DEPS


    if [[ -n ${TRAVIS_EVENT_TYPE:=} ]]; then
        echo
        echo "Travis event type: ${TRAVIS_EVENT_TYPE}"
        echo
    fi

    # In case of CRON jobs on Travis we run builds without cache
    if [[ "${TRAVIS_EVENT_TYPE}" == "cron" ]]; then
        echo
        echo "Disabling cache for CRON jobs"
        echo
        export USE_DOCKER_CACHE="false"
        echo
        echo "Enabling PROD image building for CRON jobs"
        echo
        export BUILD_PROD_IMAGE="true"
    fi
}
