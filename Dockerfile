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
# THIS DOCKERFILE IS INTENDED FOR PRODUCTION USE AND DEPLOYMENT.
# NOTE! IT IS ALPHA-QUALITY FOR NOW - WE ARE IN A PROCESS OF TESTING IT
#
#
# This is a multi-segmented image. It actually contains two images:
#
# airflow-build-image  - there all airflow dependencies can be installed (and
#                        built - for those dependencies that require
#                        build essentials). Airflow is installed there with
#                        --user switch so that all the dependencies are
#                        installed to ${HOME}/.local
#
# main                 - this is the actual production image that is much
#                        smaller because it does not contain all the build
#                        essentials. Instead the ${HOME}/.local folder
#                        is copied from the build-image - this way we have
#                        only result of installation and we do not need
#                        all the build essentials. This makes the image
#                        much smaller.
#
ARG AIRFLOW_VERSION="2.0.0.dev0"
ARG AIRFLOW_EXTRAS="async,aws,azure,celery,dask,elasticsearch,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv"
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
ARG ADDITIONAL_PYTHON_DEPS=""

ARG AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_UID="50000"
ARG AIRFLOW_GID="50000"

ARG PIP_VERSION="19.0.2"
ARG CASS_DRIVER_BUILD_CONCURRENCY="8"

ARG PYTHON_BASE_IMAGE="python:3.6-slim-buster"
ARG PYTHON_MAJOR_MINOR_VERSION="3.6"

##############################################################################################
# This is the build image where we build all dependencies
##############################################################################################
FROM ${PYTHON_BASE_IMAGE} as airflow-build-image
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

LABEL org.apache.airflow.distro="debian"
LABEL org.apache.airflow.distro.version="buster"
LABEL org.apache.airflow.module="airflow"
LABEL org.apache.airflow.component="airflow"
LABEL org.apache.airflow.image="airflow-build-image"

ARG PYTHON_BASE_IMAGE
ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}

ARG PYTHON_MAJOR_MINOR_VERSION
ENV PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION}

# Make sure noninteractive debian install is used and language variables set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

# Install curl and gnupg2 - needed to download nodejs in the next step
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           curl \
           gnupg2 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG ADDITIONAL_DEV_DEPS=""
ENV ADDITIONAL_DEV_DEPS=${ADDITIONAL_DEV_DEPS}

# Install basic and additional apt dependencies
RUN curl --fail --location https://deb.nodesource.com/setup_10.x | bash - \
    && curl https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - > /dev/null \
    && echo "deb https://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list \
    # Note missing man directories on debian-buster
    # https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
    && mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           apt-transport-https \
           apt-utils \
           build-essential \
           ca-certificates \
           gnupg \
           dirmngr \
           freetds-bin \
           freetds-dev \
           gosu \
           krb5-user \
           ldap-utils \
           libffi-dev \
           libkrb5-dev \
           libpq-dev \
           libsasl2-2 \
           libsasl2-dev \
           libsasl2-modules \
           libssl-dev \
           locales  \
           lsb-release \
           nodejs \
           openssh-client \
           postgresql-client \
           python-selinux \
           sasl2-bin \
           software-properties-common \
           sqlite3 \
           sudo \
           unixodbc \
           unixodbc-dev \
           yarn \
           ${ADDITIONAL_DEV_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install MySQL client from Oracle repositories (Debian installs mariadb)
RUN KEY="A4A9406876FCBD3C456770C88C718D3B5072E1F5" \
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
    && gpg --export "${KEY}" | apt-key add - \
    && gpgconf --kill all \
    rm -rf "${GNUPGHOME}"; \
    apt-key list > /dev/null \
    && echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7" | tee -a /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
        libmysqlclient-dev \
        mysql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG PIP_VERSION
ENV PIP_VERSION=${PIP_VERSION}

RUN pip install --upgrade pip==${PIP_VERSION}

ARG AIRFLOW_REPO=apache/airflow
ENV AIRFLOW_REPO=${AIRFLOW_REPO}

ARG AIRFLOW_BRANCH=master
ENV AIRFLOW_BRANCH=${AIRFLOW_BRANCH}

ARG AIRFLOW_EXTRAS
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
ENV AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS}${ADDITIONAL_AIRFLOW_EXTRAS:+,}${ADDITIONAL_AIRFLOW_EXTRAS}

# In case of Production build image segment we want to pre-install master version of airflow
# dependencies from github so that we do not have to always reinstall it from the scratch.
RUN pip install --user \
    "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_EXTRAS}]" \
        --constraint "https://raw.githubusercontent.com/${AIRFLOW_REPO}/${AIRFLOW_BRANCH}/requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt" \
    && pip uninstall --yes apache-airflow;

ARG AIRFLOW_SOURCES_FROM="."
ENV AIRFLOW_SOURCES_FROM=${AIRFLOW_SOURCES_FROM}

ARG AIRFLOW_SOURCES_TO="/opt/airflow"
ENV AIRFLOW_SOURCES_TO=${AIRFLOW_SOURCES_TO}

COPY ${AIRFLOW_SOURCES_FROM} ${AIRFLOW_SOURCES_TO}

ARG CASS_DRIVER_BUILD_CONCURRENCY
ENV CASS_DRIVER_BUILD_CONCURRENCY=${CASS_DRIVER_BUILD_CONCURRENCY}

ARG AIRFLOW_VERSION
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}

ARG ADDITIONAL_PYTHON_DEPS=""
ENV ADDITIONAL_PYTHON_DEPS=${ADDITIONAL_PYTHON_DEPS}

ARG AIRFLOW_INSTALL_SOURCES="."
ENV AIRFLOW_INSTALL_SOURCES=${AIRFLOW_INSTALL_SOURCES}

ARG AIRFLOW_INSTALL_VERSION=""
ENV AIRFLOW_INSTALL_VERSION=${AIRFLOW_INSTALL_VERSION}

ARG CONSTRAINT_REQUIREMENTS="requirements/requirements-python${PYTHON_MAJOR_MINOR_VERSION}.txt"
ENV CONSTRAINT_REQUIREMENTS=${CONSTRAINT_REQUIREMENTS}

WORKDIR /opt/airflow

# hadolint ignore=DL3020
ADD "${CONSTRAINT_REQUIREMENTS}" /requirements.txt

ENV PATH=${PATH}:/root/.local/bin

RUN pip install --user "${AIRFLOW_INSTALL_SOURCES}[${AIRFLOW_EXTRAS}]${AIRFLOW_INSTALL_VERSION}" \
    --constraint /requirements.txt && \
    if [ -n "${ADDITIONAL_PYTHON_DEPS}" ]; then pip install --user ${ADDITIONAL_PYTHON_DEPS} \
    --constraint /requirements.txt; fi && \
    find /root/.local/ -name '*.pyc' -print0 | xargs -0 rm -r && \
    find /root/.local/ -type d -name '__pycache__' -print0 | xargs -0 rm -r

RUN AIRFLOW_SITE_PACKAGE="/root/.local/lib/python${PYTHON_MAJOR_MINOR_VERSION}/site-packages/airflow"; \
    if [[ -f "${AIRFLOW_SITE_PACKAGE}/www_rbac/package.json" ]]; then \
        WWW_DIR="${AIRFLOW_SITE_PACKAGE}/www_rbac"; \
    elif [[ -f "${AIRFLOW_SITE_PACKAGE}/www/package.json" ]]; then \
        WWW_DIR="${AIRFLOW_SITE_PACKAGE}/www"; \
    fi; \
    if [[ ${WWW_DIR:=} != "" ]]; then \
        yarn --cwd "${WWW_DIR}" install --frozen-lockfile --no-cache; \
        yarn --cwd "${WWW_DIR}" run prod; \
        rm -rf "${WWW_DIR}/node_modules"; \
    fi

# make sure that all directories and files in .local are also group accessible
RUN find /root/.local -executable -print0 | xargs --null chmod g+x && \
    find /root/.local -print0 | xargs --null chmod g+rw

##############################################################################################
# This is the actual Airflow image - much smaller than the build one. We copy
# installed Airflow and all it's dependencies from the build image to make it smaller.
##############################################################################################
FROM ${PYTHON_BASE_IMAGE} as main
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG AIRFLOW_UID
ARG AIRFLOW_GID

LABEL org.apache.airflow.distro="debian"
LABEL org.apache.airflow.distro.version="buster"
LABEL org.apache.airflow.module="airflow"
LABEL org.apache.airflow.component="airflow"
LABEL org.apache.airflow.image="airflow"
LABEL org.apache.airflow.uid="${AIRFLOW_UID}"
LABEL org.apache.airflow.gid="${AIRFLOW_GID}"

ARG PYTHON_BASE_IMAGE
ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}

ARG AIRFLOW_VERSION
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}

ARG ADDITIONAL_RUNTIME_DEPS=""
ENV ADDITIONAL_RUNTIME_DEPS=${ADDITIONAL_RUNTIME_DEPS}

# Make sure noninteractive debian install is used and language variables set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

# Note missing man directories on debian-buster
# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
# Install basic and additional apt dependencies
RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           apt-transport-https \
           apt-utils \
           ca-certificates \
           curl \
           dumb-init \
           freetds-bin \
           gnupg \
           gosu \
           krb5-user \
           ldap-utils \
           libffi6 \
           libsasl2-2 \
           libsasl2-modules \
           libssl1.1 \
           locales  \
           lsb-release \
           netcat \
           openssh-client \
           postgresql-client \
           rsync \
           sasl2-bin \
           sqlite3 \
           sudo \
           unixodbc \
           ${ADDITIONAL_RUNTIME_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install MySQL client from Oracle repositories (Debian installs mariadb)
RUN KEY="A4A9406876FCBD3C456770C88C718D3B5072E1F5" \
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
    && gpg --export "${KEY}" | apt-key add - \
    && gpgconf --kill all \
    rm -rf "${GNUPGHOME}"; \
    apt-key list > /dev/null \
    && echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7" | tee -a /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
        libmysqlclient20 \
        mysql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ARG PIP_VERSION
ENV PIP_VERSION=${PIP_VERSION}
RUN pip install --upgrade pip==${PIP_VERSION}

ENV AIRFLOW_UID=${AIRFLOW_UID}
ENV AIRFLOW_GID=${AIRFLOW_GID}

ENV AIRFLOW__CORE__LOAD_EXAMPLES="false"

ARG AIRFLOW_USER_HOME_DIR=/home/airflow
ENV AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR}

RUN addgroup --gid "${AIRFLOW_GID}" "airflow" && \
    adduser --quiet "airflow" --uid "${AIRFLOW_UID}" \
        --gid "${AIRFLOW_GID}" \
        --home "${AIRFLOW_USER_HOME_DIR}"

ARG AIRFLOW_HOME
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Make Airflow files belong to the root group and are accessible. This is to accomodate the guidelines from
# OpenShift https://docs.openshift.com/enterprise/3.0/creating_images/guidelines.html
RUN mkdir -pv "${AIRFLOW_HOME}"; \
    mkdir -pv "${AIRFLOW_HOME}/dags"; \
    mkdir -pv "${AIRFLOW_HOME}/logs"; \
    chown -R "airflow:root" "${AIRFLOW_USER_HOME_DIR}" "${AIRFLOW_HOME}"; \
    find "${AIRFLOW_HOME}" -executable -print0 | xargs --null chmod g+x && \
        find "${AIRFLOW_HOME}" -print0 | xargs --null chmod g+rw

COPY --chown=airflow:root --from=airflow-build-image /root/.local "${AIRFLOW_USER_HOME_DIR}/.local"

COPY scripts/prod/entrypoint_prod.sh /entrypoint
COPY scripts/prod/clean-logs.sh /clean-logs

RUN chmod a+x /entrypoint /clean-logs

# Make /etc/passwd root-group-writeable so that user can be dynamically added by OpenShift
# See https://github.com/apache/airflow/issues/9248
RUN chmod g=u /etc/passwd

ENV PATH="${AIRFLOW_USER_HOME_DIR}/.local/bin:${PATH}"
ENV GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm"

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

USER ${AIRFLOW_UID}

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
