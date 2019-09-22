# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# WARNING: THIS DOCKERFILE IS NOT INTENDED FOR PRODUCTION USE OR DEPLOYMENT.
#
# Base image for the whole Docker file
ARG PYTHON_BASE_IMAGE="python:3.6-slim-buster"
ARG NODE_BASE_IMAGE="node:12.11.1-buster"

############################################################################################################
# Base image for Airflow - contains dependencies used by both - Production and CI images
############################################################################################################
FROM ${PYTHON_BASE_IMAGE} as airflow-base

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG PYTHON_BASE_IMAGE="python:3.6-slim-buster"
ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}

ARG AIRFLOW_VERSION="2.0.0.dev0"
ENV AIRFLOW_VERSION=$AIRFLOW_VERSION

# Print versions
RUN echo "Base image: ${PYTHON_BASE_IMAGE}"
RUN echo "Airflow version: ${AIRFLOW_VERSION}"

# Make sure noninteractive debian install is used and language variables set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

# By increasing this number we can do force build of all dependencies
# It can also be overwritten manually by setting the build variable.
ARG APT_DEPENDENCIES_EPOCH_NUMBER="1"
ENV APT_DEPENDENCIES_EPOCH_NUMBER=${APT_DEPENDENCIES_EPOCH_NUMBER}

# PIP version used to install dependencies
ARG PIP_VERSION="19.0.2"
ENV PIP_VERSION=${PIP_VERSION}

RUN pip install --upgrade pip==${PIP_VERSION}

# By default PIP install run without cache to make image smaller
ARG PIP_NO_CACHE_DIR="true"
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}

# Install basic apt dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           apt-utils \
           build-essential \
           curl \
           dirmngr \
           freetds-bin \
           freetds-dev \
           git \
           gosu \
           libffi-dev \
           libkrb5-dev \
           libpq-dev \
           libsasl2-2 \
           libsasl2-dev \
           libsasl2-modules \
           libssl-dev \
           locales  \
           netcat \
           rsync \
           sasl2-bin \
           sudo \
           libmariadb-dev-compat \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN adduser airflow \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow

############################################################################################################
# CI airflow image
############################################################################################################
FROM airflow-base as airflow-ci

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Setting to 1 speeds up building the image. Cassandra driver without CYTHON saves around 10 minutes
# But might not be suitable for production image
ENV CASS_DRIVER_NO_CYTHON="1"
ENV CASS_DRIVER_BUILD_CONCURRENCY=8

ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64/

# By changing the CI build epoch we can force reinstalling apt dependenecies for CI
# It can also be overwritten manually by setting the build variable.
ARG CI_APT_DEPENDENCIES_EPOCH_NUMBER="1"
ENV CI_APT_DEPENDENCIES_EPOCH_NUMBER=${CI_APT_DEPENDENCIES_EPOCH_NUMBER}

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
         apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common curl gnupg2 \
    && export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1 \
    && curl -sL https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
      gnupg \
      graphviz \
      krb5-user \
      ldap-utils \
      less \
      lsb-release \
      nodejs \
      net-tools \
      adoptopenjdk-8-hotspot \
      openssh-client \
      openssh-server \
      postgresql-client \
      python-selinux \
      sqlite3 \
      tmux \
      unzip \
      vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    ;

ENV HADOOP_DISTRO="cdh" HADOOP_MAJOR="5" HADOOP_DISTRO_VERSION="5.11.0" HADOOP_VERSION="2.6.0" \
    HADOOP_HOME="/tmp/hadoop-cdh"
ENV HIVE_VERSION="1.1.0" HIVE_HOME="/tmp/hive"
ENV HADOOP_URL="https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/"
ENV MINICLUSTER_BASE="https://github.com/bolkedebruin/minicluster/releases/download/" \
    MINICLUSTER_HOME="/tmp/minicluster" \
    MINICLUSTER_VER="1.1"

RUN mkdir -pv "${HADOOP_HOME}" \
    && mkdir -pv "${HIVE_HOME}" \
    && mkdir -pv "${MINICLUSTER_HOME}" \
    && mkdir -pv "/user/hive/warehouse" \
    && chmod -R 777 "${HIVE_HOME}" \
    &&chmod -R 777 "/user/"

ENV HADOOP_DOWNLOAD_URL="${HADOOP_URL}hadoop-${HADOOP_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz" \
    HADOOP_TMP_FILE="/tmp/hadoop.tar.gz"

RUN curl -sL "${HADOOP_DOWNLOAD_URL}" >"${HADOOP_TMP_FILE}" \
    && tar xzf "${HADOOP_TMP_FILE}" --absolute-names --strip-components 1 -C "${HADOOP_HOME}" \
    && rm "${HADOOP_TMP_FILE}"

ENV HIVE_URL="${HADOOP_URL}hive-${HIVE_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz" \
    HIVE_TMP_FILE="/tmp/hive.tar.gz"

RUN curl -sL "${HIVE_URL}" >"${HIVE_TMP_FILE}" \
    && tar xzf "${HIVE_TMP_FILE}" --strip-components 1 -C "${HIVE_HOME}" \
    && rm "${HIVE_TMP_FILE}"

ENV MINICLUSTER_URL="${MINICLUSTER_BASE}${MINICLUSTER_VER}/minicluster-${MINICLUSTER_VER}-SNAPSHOT-bin.zip" \
    MINICLUSTER_TMP_FILE="/tmp/minicluster.zip"

RUN curl -sL "${MINICLUSTER_URL}" > "${MINICLUSTER_TMP_FILE}" \
    && unzip "${MINICLUSTER_TMP_FILE}" -d "/tmp" \
    && rm "${MINICLUSTER_TMP_FILE}"

ENV PATH "${PATH}:/tmp/hive/bin"

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian stretch stable" \
    && apt-get update \
    && apt-get -y install --no-install-recommends docker-ce \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG KUBERNETES_VERSION="v1.15.0"
ENV KUBERNETES_VERSION=${KUBERNETES_VERSION}
ARG KIND_VERSION="v0.5.0"
ENV KIND_VERSION=${KIND_VERSION}

RUN curl -Lo kubectl \
  "https://storage.googleapis.com/kubernetes-release/release/${KUBERNETES_VERSION}/bin/linux/amd64/kubectl" \
  && chmod +x kubectl \
  && mv kubectl /usr/local/bin/kubectl

RUN curl -Lo kind \
   "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64" \
   && chmod +x kind \
   && mv kind /usr/local/bin/kind

ARG AIRFLOW_REPO=apache/airflow
ENV AIRFLOW_REPO=${AIRFLOW_REPO}

ARG AIRFLOW_BRANCH=master
ENV AIRFLOW_BRANCH=${AIRFLOW_BRANCH}

# Airflow Extras installed
ARG AIRFLOW_CI_EXTRAS="all,devel"
ENV AIRFLOW_CI_EXTRAS=${AIRFLOW_CI_EXTRAS}

RUN echo "Installing with extras: ${AIRFLOW_CI_EXTRAS}."

# Increase the value here to force reinstalling pip dependencies from the scratch for CI build
# It can also be overwritten manually by setting the build variable.
ARG PIP_DEPENDENCIES_EPOCH_NUMBER="1"
ENV PIP_DEPENDENCIES_EPOCH_NUMBER=${PIP_DEPENDENCIES_EPOCH_NUMBER}

ENV PATH="/root/.local/bin:/root:${PATH}"

# In case of CI builds we want to pre-install master version of airflow dependencies so that
# We do not have to always reinstall it from the scratch and loose time for that.
# CI build is optimised for build speed
RUN pip install --user \
        "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_CI_EXTRAS}]" \
        && pip uninstall --yes apache-airflow snakebite

ARG AIRFLOW_SOURCES=/opt/airflow
ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

# Copy all www files here so that we can run npm building for production
COPY airflow/www/ ${AIRFLOW_SOURCES}/airflow/www/

WORKDIR ${AIRFLOW_SOURCES}/airflow/www

ARG DEBUG_FIXING_PERMISSIONS=false

RUN  \
    if [[ ${DEBUG_FIXING_PERMISSIONS:=} == "true" ]]; then \
       find . -type d -exec ls -la {} +; \
    fi

RUN mkdir -p "${AIRFLOW_SOURCES}/airflow/www/static" \
    && mkdir -p "${AIRFLOW_SOURCES}/docs/build/_html" \
    && pushd "${AIRFLOW_SOURCES}/airflow/www/static" || exit \
    && ln -sf ../../../docs/_build/html docs \
    && popd || exit

RUN npm ci

RUN npm run prod

WORKDIR ${AIRFLOW_SOURCES}

# Airflow sources change frequently but dependency configuration won't change that often
# We copy setup.py and other files needed to perform setup of dependencies
# So in case setup.py changes we can install latest dependencies required.
COPY setup.py ${AIRFLOW_SOURCES}/setup.py
COPY setup.cfg ${AIRFLOW_SOURCES}/setup.cfg

COPY airflow/version.py ${AIRFLOW_SOURCES}/airflow/version.py
COPY airflow/__init__.py ${AIRFLOW_SOURCES}/airflow/__init__.py
COPY airflow/bin/airflow ${AIRFLOW_SOURCES}/airflow/bin/airflow

# The goal of this line is to install the dependencies from the most current setup.py from sources
# This will be usually incremental small set of packages in CI optimized build, so it will be very fast
# For production optimised build it is the first time dependencies are installed so it will be slower
RUN pip install --user -e ".[${AIRFLOW_CI_EXTRAS}]" \
    && pip uninstall --yes apache-airflow

# Cache for this line will be automatically invalidated if any
# of airflow sources change
COPY . ${AIRFLOW_SOURCES}/

# Reinstall airflow again - this time with sources and remove the sources after installation
# It is not perfect because the sources are added as layer but it is still better
RUN pip install --user -e ".[${AIRFLOW_CI_EXTRAS}]"

# Additional python deps to install
ARG ADDITIONAL_PYTHON_DEPS=""

RUN if [[ -n "${ADDITIONAL_PYTHON_DEPS}" ]]; then \
        pip install --user ${ADDITIONAL_PYTHON_DEPS}; \
    fi

COPY ./scripts/docker/entrypoint.sh /entrypoint.sh

ARG AIRFLOW_HOME=/root/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

RUN mkdir -pv "${AIRFLOW_HOME}" \
    && mkdir -pv "${AIRFLOW_HOME}/dags" \
    && mkdir -pv "${AIRFLOW_HOME}/logs"

WORKDIR ${AIRFLOW_SOURCES}

COPY .bash_completion run-tests-complete run-tests /root/
COPY .bash_completion.d/run-tests-complete /root/.bash_completion.d/run-tests-complete

ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_SOURCES}:${AIRFLOW_SOURCES}/tests/test_utils

RUN "${AIRFLOW_SOURCES}/scripts/ci/docker_build/extract_tests.sh"

ENV AIRFLOW_USER=root
ENV HOME=/root

EXPOSE 8080

ENTRYPOINT ["/root/.local/bin/dumb-init", "--", "/entrypoint.sh"]

CMD ["--help"]

############################################################################################################
# This is separate stage for packaging. WWW files with npm so that no node is needed for production image
############################################################################################################
FROM ${NODE_BASE_IMAGE} as airflow-www

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG AIRFLOW_SOURCES=/opt/airflow
ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

COPY airflow/www/ ${AIRFLOW_SOURCES}/airflow/www/

WORKDIR ${AIRFLOW_SOURCES}/airflow/www

RUN npm ci

RUN mkdir -p "${AIRFLOW_SOURCES}/airflow/www/static" \
    && mkdir -p "${AIRFLOW_SOURCES}/docs/build/_html" \
    && pushd "${AIRFLOW_SOURCES}/airflow/www/static" || exit \
    && ln -sf ../../../docs/_build/html docs \
    && popd || exit

# Package NPM for production
RUN npm run prod

# Remove node modules
RUN rm -rf ${AIRFLOW_SOURCES}/airflow/www/node_modules

############################################################################################################
# Generated documentation
############################################################################################################
FROM airflow-ci as airflow-docs

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

RUN "${AIRFLOW_SOURCES}/docs/build.sh"

############################################################################################################
# Production-ready Airflow image
############################################################################################################
FROM airflow-base as airflow-prod

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Airflow Extras installed
ARG AIRFLOW_PROD_EXTRAS="all"
ENV AIRFLOW_PROD_EXTRAS=${AIRFLOW_PROD_EXTRAS}

RUN echo "Installing with extras: ${AIRFLOW_PROD_EXTRAS}."

ARG AIRFLOW_HOME=/home/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

RUN chown airflow.airflow ${AIRFLOW_HOME}

USER airflow

ARG AIRFLOW_SOURCES=/opt/airflow
ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

WORKDIR ${AIRFLOW_SOURCES}

# Airflow sources change frequently but dependency configuration won't change that often
# We copy setup.py and other files needed to perform setup of dependencies
# So in case setup.py changes we can install latest dependencies required.
COPY --chown=airflow:airflow setup.py ${AIRFLOW_SOURCES}/setup.py
COPY --chown=airflow:airflow setup.cfg ${AIRFLOW_SOURCES}/setup.cfg

COPY --chown=airflow:airflow airflow/version.py ${AIRFLOW_SOURCES}/airflow/version.py
COPY --chown=airflow:airflow airflow/__init__.py ${AIRFLOW_SOURCES}/airflow/__init__.py
COPY --chown=airflow:airflow airflow/bin/airflow ${AIRFLOW_SOURCES}/airflow/bin/airflow

# Setting to 1 speeds up building the image. Cassandra driver without CYTHON saves around 10 minutes
# But might not be suitable for production image
ENV CASS_DRIVER_NO_CYTHON=""
ENV CASS_DRIVER_BUILD_CONCURRENCY="8"

ENV PATH="/home/airflow/.local/bin:/home/airflow:${PATH}"

# The goal of this line is to install the dependencies from the most current setup.py from sources
# This will be usually incremental small set of packages in CI optimized build, so it will be very fast
# For production optimised build it is the first time dependencies are installed so it will be slower
RUN pip install --user ".[${AIRFLOW_PROD_EXTRAS}]" \
    && pip uninstall --yes apache-airflow snakebite

# Cache for this line will be automatically invalidated if any
# of airflow sources change
COPY --chown=airflow:airflow . ${AIRFLOW_SOURCES}/

# Reinstall airflow again - this time with sources and remove the sources after installation
RUN pip install --user ".[${AIRFLOW_PROD_EXTRAS}]"

# Additional python deps to install
ARG ADDITIONAL_PYTHON_DEPS=""

RUN if [[ -n "${ADDITIONAL_PYTHON_DEPS}" ]]; then \
        pip install --user ${ADDITIONAL_PYTHON_DEPS}; \
    fi

COPY --chown=airflow:airflow ./scripts/docker/entrypoint.sh /entrypoint.sh

# Copy Airflow www packages
COPY --chown=airflow:airflow --from=airflow-www /opt/airflow/airflow/www ${HOME}/.local/airflow/www

COPY --chown=airflow:airflow --from=airflow-docs /opt/airflow/docs/_build/html \
    ${HOME}/.local/airflow/www/static/docs

RUN mkdir -pv "${AIRFLOW_HOME}" \
    && mkdir -pv "${AIRFLOW_HOME}/dags" \
    && mkdir -pv "${AIRFLOW_HOME}/logs"

ENV AIRFLOW_USER=airflow
ENV HOME=/home/airflow

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

ENTRYPOINT ["/home/airflow/.local/bin/dumb-init", "--", "/entrypoint.sh"]

CMD ["--help"]
