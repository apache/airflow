#
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
# Arguments of the build
ARG PYTHON_BASE_IMAGE="python:3.6-slim"
ARG AIRFLOW_VERSION="2.0.0.dev0"
# Which image is used as dependency for the main image
ARG APT_DEPS_IMAGE="airflow-apt-deps"
# Speeds up building the image - cassandra driver without CYTHON saves around 10 minutes
ARG CASS_DRIVER_NO_CYTHON="1"
# Build cassandra driver on multiple CPUs
ARG CASS_DRIVER_BUILD_CONCURRENCY="8"
# By default PIP install is run without cache to make image smaller
ARG PIP_CACHE_DIRECTIVE="--no-cache-dir"
# Additional python deps to install
ARG ADDITIONAL_PYTHON_DEPS=""
# PIP version used to install dependencies
ARG PIP_VERSION="19.0.1"
# By increasing this number we can do force build of all dependencies
ARG FORCE_REINSTALL_ALL_DEPENDENCIES="1"
############################################################################################################
# This is base image with APT dependencies needed by Airflow. It is based on a python slim image
# Parameters:
#    PYTHON_BASE_IMAGE - base python image (python:x.y-slim)
############################################################################################################
FROM ${PYTHON_BASE_IMAGE} as airflow-apt-deps

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG PYTHON_BASE_IMAGE
ARG AIRFLOW_VERSION
ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}
ENV AIRFLOW_VERSION=$AIRFLOW_VERSION

# Print versions
RUN echo "Python version: ${PYTHON_VERSION}"
RUN echo "Base image: ${PYTHON_BASE_IMAGE}"
RUN echo "Airflow version: ${AIRFLOW_VERSION}"

# Make sure noninteractie debian install is used and language variab1les set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

# Increase the value below to force renstalling of all dependencies
ENV FORCE_REINSTALL_ALL_DEPENDENCIES=${FORCE_REINSTALL_ALL_DEPENDENCIES}

# Install curl and gnupg2 - needed to download nodejs in next step
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           curl gnupg2 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Install basic apt dependencies
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           # Packages to install \
           libsasl2-dev freetds-bin build-essential \
           default-libmysqlclient-dev apt-utils curl rsync netcat locales  \
           freetds-dev libkrb5-dev libssl-dev libffi-dev libpq-dev git \
           nodejs gosu sudo \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN adduser airflow \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow

############################################################################################################
# This is an image with all APT dependencies needed by CI. It is built on top of the airlfow APT image
# Parameters:
#     airflow-apt-deps - this is the base image for CI deps image.
############################################################################################################
FROM airflow-apt-deps as airflow-ci-apt-deps

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

ARG APT_DEPS_IMAGE
ENV APT_DEPS_IMAGE=${APT_DEPS_IMAGE}

RUN echo "${APT_DEPS_IMAGE}"

# Note the ifs below might be removed if Buildkit will become usable. It should skip building this
# image automatically if it is not used. For now we still go through all layers below but they are empty
# Note missing directories on debian-stretch https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
RUN if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
        mkdir -pv /usr/share/man/man1 \
        && mkdir -pv /usr/share/man/man7 \
        && apt-get update \
        && apt-get install --no-install-recommends -y \
          lsb-release \
          gnupg \
          dirmngr \
          openjdk-8-jdk \
          vim \
          tmux \
          less \
          unzip \
          ldap-utils \
          postgresql-client \
          sqlite3 \
          krb5-user \
          openssh-client \
          openssh-server \
          python-selinux \
          sasl2-bin \
        && apt-get autoremove -yqq --purge \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/* \
        ;\
    fi

RUN if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
        KEY="A4A9406876FCBD3C456770C88C718D3B5072E1F5" \
        && GNUPGHOME="$(mktemp -d)" \
        && export GNUPGHOME \
        && for KEYSERVER in $(shuf -e \
                ha.pool.sks-keyservers.net \
                hkp://p80.pool.sks-keyservers.net:80 \
                keyserver.ubuntu.com \
                hkp://keyserver.ubuntu.com:80 \
                pgp.mit.edu) ; do \
              gpg --keyserver "${KEYSERVER}" --recv-keys "${KEY}" && break || true ; \
           done \
        && gpg --export "${KEY}" > /etc/apt/trusted.gpg.d/mysql.gpg \
        && gpgconf --kill all \
        rm -rf "${GNUPGHOME}"; \
        apt-key list > /dev/null \
        && echo "deb http://repo.mysql.com/apt/ubuntu/ trusty mysql-5.7" | \
            tee -a /etc/apt/sources.list.d/mysql.list \
        && apt-get update \
        && MYSQL_PASS="secret" \
        && debconf-set-selections <<< \
            "mysql-community-server mysql-community-server/data-dir select ''" \
        && debconf-set-selections <<< \
            "mysql-community-server mysql-community-server/root-pass password ${MYSQL_PASS}" \
        && debconf-set-selections <<< \
            "mysql-community-server mysql-community-server/re-root-pass password ${MYSQL_PASS}" \
        && apt-get install --no-install-recommends -y mysql-client libmysqlclient-dev \
        && apt-get autoremove -yqq --purge \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/* \
        ;\
    fi

ENV HADOOP_DISTRO=cdh HADOOP_MAJOR=5 HADOOP_DISTRO_VERSION=5.11.0 HADOOP_VERSION=2.6.0 HIVE_VERSION=1.1.0
ENV HADOOP_URL=https://archive.cloudera.com/${HADOOP_DISTRO}${HADOOP_MAJOR}/${HADOOP_DISTRO}/${HADOOP_MAJOR}/
ENV HADOOP_HOME=/tmp/hadoop-cdh HIVE_HOME=/tmp/hive

RUN \
if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
    mkdir -pv ${HADOOP_HOME} \
    && mkdir -pv ${HIVE_HOME} \
    && mkdir /tmp/minicluster \
    && mkdir -pv /user/hive/warehouse \
    && chmod -R 777 ${HIVE_HOME} \
    && chmod -R 777 /user/ \
    ;\
fi
# Install Hadoop
# --absolute-names is a work around to avoid this issue https://github.com/docker/hub-feedback/issues/727
RUN \
if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
    HADOOP_URL=${HADOOP_URL}hadoop-${HADOOP_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz \
    && HADOOP_TMP_FILE=/tmp/hadoop.tar.gz \
    && curl -sL ${HADOOP_URL} > ${HADOOP_TMP_FILE} \
    && tar xzf ${HADOOP_TMP_FILE} --absolute-names --strip-components 1 -C ${HADOOP_HOME} \
    && rm ${HADOOP_TMP_FILE} \
    ;\
fi

# Install Hive
RUN \
if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
    HIVE_URL=${HADOOP_URL}hive-${HIVE_VERSION}-${HADOOP_DISTRO}${HADOOP_DISTRO_VERSION}.tar.gz \
    && HIVE_TMP_FILE=/tmp/hive.tar.gz \
    && curl -sL ${HIVE_URL} > ${HIVE_TMP_FILE} \
    && tar xzf ${HIVE_TMP_FILE} --strip-components 1 -C ${HIVE_HOME} \
    && rm ${HIVE_TMP_FILE} \
    ;\
fi

ENV MINICLUSTER_URL=https://github.com/bolkedebruin/minicluster/releases/download/
ENV MINICLUSTER_VER=1.1
# Install MiniCluster TODO: install it differently. Installing to /tmp is probably a bad idea
RUN \
if [[ "${APT_DEPS_IMAGE}" == "airflow-ci-apt-deps" ]]; then \
    MINICLUSTER_URL=${MINICLUSTER_URL}${MINICLUSTER_VER}/minicluster-${MINICLUSTER_VER}-SNAPSHOT-bin.zip \
    && MINICLUSTER_TMP_FILE=/tmp/minicluster.zip \
    && curl -sL ${MINICLUSTER_URL} > ${MINICLUSTER_TMP_FILE} \
    && unzip ${MINICLUSTER_TMP_FILE} -d /tmp \
    && rm ${MINICLUSTER_TMP_FILE} \
    ;\
fi

ENV PATH "$PATH:/tmp/hive/bin:$ADDITIONAL_PATH"

############################################################################################################
# This is the target image - it installs PIP and NPN dependencies including efficient caching
# mechanisms - it might be used to build the bare airflow build or CI build
# Parameters:
#    APT_DEPS_IMAGE - image with APT dependencies. It might either be base deps image with airflow
#                     dependencies or CI deps image that contains also CI-required dependencies
############################################################################################################
FROM ${APT_DEPS_IMAGE} as main

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR /opt/airflow

RUN echo "Airflow version: ${AIRFLOW_VERSION}"

ARG AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

RUN mkdir -pv ${AIRFLOW_HOME} \
    && chown -R airflow.airflow ${AIRFLOW_HOME}

# Increase the value here to force reinstalling Apache Airflow pip dependencies
ENV FORCE_REINSTALL_ALL_PIP_DEPENDENCIES=1

ARG CASS_DRIVER_BUILD_CONCURRENCY
ARG CASS_DRIVER_NO_CYTHON
ENV CASS_DRIVER_BUILD_CONCURRENCY=${CASS_DRIVER_BUILD_CONCURRENCY}
ENV CASS_DRIVER_NO_CYTHON=${CASS_DRIVER_NO_CYTHON}

# Airflow sources change frequently but dependency onfiguration won't change that often
# We copy setup.py and other files needed to perform setup of dependencies
# This way cache here will only be invalidated if any of the
# version/setup configuration change but not when airflow sources change
COPY --chown=airflow:airflow setup.py /opt/airflow/setup.py
COPY --chown=airflow:airflow setup.cfg /opt/airflow/setup.cfg

COPY --chown=airflow:airflow airflow/version.py /opt/airflow/airflow/version.py
COPY --chown=airflow:airflow airflow/__init__.py /opt/airflow/airflow/__init__.py
COPY --chown=airflow:airflow airflow/bin/airflow /opt/airflow/airflow/bin/airflow


ARG AIRFLOW_EXTRAS="all"
ENV AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS}
RUN echo "Installing with extras: ${AIRFLOW_EXTRAS}."

ARG PIP_CACHE_DIRECTIVE
ENV PIP_CACHE_DIRECTIVE=${PIP_CACHE_DIRECTIVE}
RUN echo "Pip cache directive: ${PIP_CACHE_DIRECTIVE}."

ARG PIP_VERSION
ENV PIP_VERSION=${PIP_VERSION}
RUN echo "Pip version: ${PIP_VERSION}"

# First install only dependencies but no Apache Airflow itself
# This way regular changes in sources of Airflow will not trigger reinstallation of all dependencies
# And this Docker layer will be reused between builds.
RUN pip install ${PIP_CACHE_DIRECTIVE} --upgrade pip==${PIP_VERSION} \
    && pip install ${PIP_CACHE_DIRECTIVE} --no-use-pep517 -e ".[${AIRFLOW_EXTRAS}]"

COPY --chown=airflow:airflow airflow/www/package.json /opt/airflow/airflow/www/package.json
COPY --chown=airflow:airflow airflow/www/package-lock.json /opt/airflow/airflow/www/package-lock.json

WORKDIR /opt/airflow/airflow/www

# Install necessary NPM dependencies (triggered by changes in package-lock.json)
RUN gosu airflow npm ci

COPY --chown=airflow:airflow airflow/www/ /opt/airflow/airflow/www/

# Package NPM for production
RUN gosu airflow npm run prod

WORKDIR /opt/airflow

# Cache for this line will be automatically invalidated if any
# of airflow sources change
COPY --chown=airflow:airflow . /opt/airflow/

# Always add-get update/upgrade here to get latest dependencies before
# we redo pip install
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Additional python dependencies
ARG ADDITIONAL_PYTHON_DEPS

RUN if [ -n "${ADDITIONAL_PYTHON_DEPS}" ]; then \
        pip install ${PIP_CACHE_DIRECTIVE} ${ADDITIONAL_PYTHON_DEPS}; \
    fi

USER airflow

WORKDIR ${AIRFLOW_HOME}

COPY --chown=airflow:airflow ./scripts/docker/entrypoint.sh /entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dumb-init", "--", "/entrypoint.sh"]
CMD ["--help"]
