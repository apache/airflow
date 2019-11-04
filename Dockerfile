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
# Base image for Airflow - contains dependencies used by the main CI image
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

RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && apt-get update \
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
    HADOOP_HOME="/opt/hadoop-cdh"
ENV HIVE_VERSION="1.1.0" HIVE_HOME="/opt/hive"
ENV HADOOP_URL="https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/"
ENV MINICLUSTER_BASE="https://github.com/bolkedebruin/minicluster/releases/download/" \
    MINICLUSTER_HOME="/opt/minicluster" \
    MINICLUSTER_VER="1.1"

RUN mkdir -pv "${HADOOP_HOME}" \
    && mkdir -pv "${HIVE_HOME}" \
    && mkdir -pv "${MINICLUSTER_HOME}" \
    && mkdir -pv "/user/hive/warehouse" \
    && chmod -R 777 "${HIVE_HOME}" \
    && chmod -R 777 "/user/"

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
    && unzip "${MINICLUSTER_TMP_FILE}" -d "/opt" \
    && rm "${MINICLUSTER_TMP_FILE}"

ENV PATH "${PATH}:/opt/hive/bin"

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian stretch stable" \
    && apt-get update \
    && apt-get -y install --no-install-recommends docker-ce \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG KUBECTL_VERSION="v1.15.3"
ENV KUBECTL_VERSION=${KUBECTL_VERSION}
ARG KIND_VERSION="v0.5.0"
ENV KIND_VERSION=${KIND_VERSION}

RUN curl -Lo kubectl \
  "https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  && chmod +x kubectl \
  && mv kubectl /usr/local/bin/kubectl

RUN curl -Lo kind \
   "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64" \
   && chmod +x kind \
   && mv kind /usr/local/bin/kind

ARG RAT_VERSION="0.13"

ENV RAT_VERSION="${RAT_VERSION}" \
    RAT_JAR="/opt/apache-rat-${RAT_VERSION}.jar" \
    RAT_URL="https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"
ENV RAT_JAR_MD5="${RAT_JAR}.md5" \
    RAT_URL_MD5="${RAT_URL}.md5"

RUN echo "Downloading RAT from ${RAT_URL} to ${RAT_JAR}" \
    && curl -sL "${RAT_URL}" > "${RAT_JAR}" \
    && curl -sL "${RAT_URL_MD5}" > "${RAT_JAR_MD5}" \
    && jar -tf "${RAT_JAR}" >/dev/null \
    && md5sum -c <<<"$(cat "${RAT_JAR_MD5}") ${RAT_JAR}"

ARG HOME=/root
ENV HOME=${HOME}

ARG AIRFLOW_HOME=/root/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

ARG AIRFLOW_SOURCES=/opt/airflow
ENV AIRFLOW_SOURCES=${AIRFLOW_SOURCES}

WORKDIR ${AIRFLOW_SOURCES}

RUN mkdir -pv ${AIRFLOW_HOME} \
    mkdir -pv ${AIRFLOW_HOME}/dags \
    mkdir -pv ${AIRFLOW_HOME}/logs

# Optimizing installation of Cassandra driver
# Speeds up building the image - cassandra driver without CYTHON saves around 10 minutes
ARG CASS_DRIVER_NO_CYTHON="1"
# Build cassandra driver on multiple CPUs
ARG CASS_DRIVER_BUILD_CONCURRENCY="8"

ENV CASS_DRIVER_BUILD_CONCURRENCY=${CASS_DRIVER_BUILD_CONCURRENCY}
ENV CASS_DRIVER_NO_CYTHON=${CASS_DRIVER_NO_CYTHON}

# By default PIP install run without cache to make image smaller
ARG PIP_NO_CACHE_DIR="true"
ENV PIP_NO_CACHE_DIR=${PIP_NO_CACHE_DIR}
RUN echo "Pip no cache dir: ${PIP_NO_CACHE_DIR}"

# PIP version used to install dependencies
ARG PIP_VERSION="19.0.2"
ENV PIP_VERSION=${PIP_VERSION}
RUN echo "Pip version: ${PIP_VERSION}"

RUN pip install --upgrade pip==${PIP_VERSION}

ARG AIRFLOW_REPO=apache/airflow
ENV AIRFLOW_REPO=${AIRFLOW_REPO}

ARG AIRFLOW_BRANCH=master
ENV AIRFLOW_BRANCH=${AIRFLOW_BRANCH}

# Airflow Extras installed
ARG AIRFLOW_CI_EXTRAS="devel_ci"
ENV AIRFLOW_CI_EXTRAS=${AIRFLOW_CI_EXTRAS}

RUN echo "Installing with extras: ${AIRFLOW_CI_EXTRAS}."

ENV PATH="${HOME}/.local/bin:${PATH}"

# Increase the value here to force reinstalling Apache Airflow pip dependencies
ARG PIP_DEPENDENCIES_EPOCH_NUMBER="1"
ENV PIP_DEPENDENCIES_EPOCH_NUMBER=${PIP_DEPENDENCIES_EPOCH_NUMBER}

# In case of CI builds we want to pre-install master version of airflow dependencies so that
# We do not have to always reinstall it from the scratch and loose time for that.
# CI build is optimised for build speed
RUN pip install --user \
        "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_CI_EXTRAS}]" \
        && pip uninstall --yes apache-airflow snakebite

# Copy all www files here so that we can run npm building
COPY airflow/www/ ${AIRFLOW_SOURCES}/airflow/www/

WORKDIR ${AIRFLOW_SOURCES}/airflow/www

RUN mkdir -p "${AIRFLOW_SOURCES}/airflow/www/static" \
    && mkdir -p "${AIRFLOW_SOURCES}/docs/build/_html" \
    && pushd "${AIRFLOW_SOURCES}/airflow/www/static" || exit \
    && ln -sf ../../../docs/_build/html docs \
    && popd || exit

RUN npm ci

RUN npm run prod

COPY ./scripts/docker/entrypoint.sh /entrypoint.sh

COPY .bash_completion run-tests-complete run-tests /root/
COPY .bash_completion.d/run-tests-complete /root/.bash_completion.d/run-tests-complete

RUN echo ". ${HOME}/.bash_completion" >> "${HOME}/.bashrc"

RUN chmod +x "${HOME}/run-tests-complete"

RUN chmod +x "${HOME}/run-tests"

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
# This will be usually incremental small set of packages in the CI optimized build, so it will be very fast
RUN pip install --user -e ".[${AIRFLOW_CI_EXTRAS}]" \
    && pip uninstall --yes apache-airflow

# Copy selected subdirectories only
COPY .github/ ${AIRFLOW_SOURCES}/.github/
COPY dags/ ${AIRFLOW_SOURCES}/dags/
COPY common/ ${AIRFLOW_SOURCES}/common/
COPY licenses/ ${AIRFLOW_SOURCES}/licenses/
COPY scripts/ci/ ${AIRFLOW_SOURCES}/scripts/ci/
COPY docs/ ${AIRFLOW_SOURCES}/docs/
COPY tests/ ${AIRFLOW_SOURCES}/tests/
COPY airflow/ ${AIRFLOW_SOURCES}/airflow/
COPY .coveragerc .rat-excludes .flake8 pylintrc LICENSE MANIFEST.in NOTICE CHANGELOG.txt \
     .github .bash_completion .bash_completion.d run-tests run-tests-complete \
     setup.cfg setup.py \
     ${AIRFLOW_SOURCES}/

# Reinstall airflow again - this time with sources and remove the sources after installation
# It is not perfect because the sources are added as layer but it is still better
RUN pip install --user -e ".[${AIRFLOW_CI_EXTRAS}]"

# Additional python deps to install
ARG ADDITIONAL_PYTHON_DEPS=""

RUN if [[ -n "${ADDITIONAL_PYTHON_DEPS}" ]]; then \
        pip install --user ${ADDITIONAL_PYTHON_DEPS}; \
    fi


WORKDIR ${AIRFLOW_SOURCES}

ENV PATH="${HOME}:${PATH}"

EXPOSE 8080

ENTRYPOINT ["/root/.local/bin/dumb-init", "--", "/entrypoint.sh"]

CMD ["--help"]
