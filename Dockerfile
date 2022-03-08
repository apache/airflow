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
# Use the same builder frontend version for everyone
# syntax=docker/dockerfile:1.3
ARG AIRFLOW_EXTRAS="amazon,async,celery,cncf.kubernetes,dask,docker,elasticsearch,ftp,google,google_auth,grpc,hashicorp,http,ldap,microsoft.azure,mysql,odbc,pandas,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv"
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
ARG ADDITIONAL_PYTHON_DEPS=""

ARG AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_UID="50000"
ARG AIRFLOW_USER_HOME_DIR=/home/airflow

# latest released version here
ARG AIRFLOW_VERSION="2.2.4"

ARG PYTHON_BASE_IMAGE="python:3.7-slim-bullseye"

ARG AIRFLOW_PIP_VERSION=22.0.4
ARG AIRFLOW_IMAGE_REPOSITORY="https://github.com/apache/airflow"
ARG AIRFLOW_IMAGE_README_URL="https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"

# By default latest released version of airflow is installed (when empty) but this value can be overridden
# and we can install version according to specification (For example ==2.0.2 or <3.0.0).
ARG AIRFLOW_VERSION_SPECIFICATION=""

# By default PIP has progress bar but you can disable it.
ARG PIP_PROGRESS_BAR="on"
##############################################################################################
# This is the build image where we build all dependencies
##############################################################################################
FROM ${PYTHON_BASE_IMAGE} as airflow-build-image

# Nolog bash flag is currently ignored - but you can replace it with
# xtrace - to show commands executed)
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "nolog", "-c"]

ARG PYTHON_BASE_IMAGE

ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE} \
    DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

ARG DEV_APT_DEPS="\
     apt-transport-https \
     apt-utils \
     build-essential \
     ca-certificates \
     dirmngr \
     freetds-bin \
     freetds-dev \
     gosu \
     krb5-user \
     ldap-utils \
     libffi-dev \
     libkrb5-dev \
     libldap2-dev \
     libsasl2-2 \
     libsasl2-dev \
     libsasl2-modules \
     libssl-dev \
     locales  \
     lsb-release \
     nodejs \
     openssh-client \
     sasl2-bin \
     software-properties-common \
     sqlite3 \
     sudo \
     unixodbc \
     unixodbc-dev \
     yarn"

ARG ADDITIONAL_DEV_APT_DEPS=""
ARG DEV_APT_COMMAND="\
    curl --silent --fail --location https://deb.nodesource.com/setup_14.x | \
        bash -o pipefail -o errexit -o nolog - \
    && curl --silent https://dl.yarnpkg.com/debian/pubkey.gpg | \
    apt-key add - >/dev/null 2>&1\
    && echo 'deb https://dl.yarnpkg.com/debian/ stable main' > /etc/apt/sources.list.d/yarn.list"
ARG ADDITIONAL_DEV_APT_COMMAND="echo"
ARG ADDITIONAL_DEV_APT_ENV=""

ENV DEV_APT_DEPS=${DEV_APT_DEPS} \
    ADDITIONAL_DEV_APT_DEPS=${ADDITIONAL_DEV_APT_DEPS} \
    DEV_APT_COMMAND=${DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_COMMAND=${ADDITIONAL_DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_ENV=${ADDITIONAL_DEV_APT_ENV}
COPY scripts/docker/determine_debian_version_specific_variables.sh /scripts/docker/

# Install basic and additional apt dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1 \
    && apt-get install -y --no-install-recommends curl gnupg2 lsb-release \
    && export ${ADDITIONAL_DEV_APT_ENV?} \
    && source /scripts/docker/determine_debian_version_specific_variables.sh \
    && bash -o pipefail -o errexit -o nounset -o nolog -c "${DEV_APT_COMMAND}" \
    && bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_DEV_APT_COMMAND}" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           ${DEV_APT_DEPS} \
           "${DISTRO_SELINUX}" \
           ${ADDITIONAL_DEV_APT_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG INSTALL_MYSQL_CLIENT="true"
ARG INSTALL_MSSQL_CLIENT="true"
ARG INSTALL_POSTGRES_CLIENT="true"
ARG AIRFLOW_REPO=apache/airflow
ARG AIRFLOW_BRANCH=main
ARG AIRFLOW_EXTRAS
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
# Allows to override constraints source
ARG CONSTRAINTS_GITHUB_REPOSITORY="apache/airflow"
ARG AIRFLOW_CONSTRAINTS="constraints"
ARG AIRFLOW_CONSTRAINTS_REFERENCE=""
ARG AIRFLOW_CONSTRAINTS_LOCATION=""
ARG DEFAULT_CONSTRAINTS_BRANCH="constraints-main"
ARG AIRFLOW_PIP_VERSION
# By default PIP has progress bar but you can disable it.
ARG PIP_PROGRESS_BAR
# By default we do not use pre-cached packages, but in CI/Breeze environment we override this to speed up
# builds in case setup.py/setup.cfg changed. This is pure optimisation of CI/Breeze builds.
ARG AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"
# This is airflow version that is put in the label of the image build
ARG AIRFLOW_VERSION
# By default latest released version of airflow is installed (when empty) but this value can be overridden
# and we can install version according to specification (For example ==2.0.2 or <3.0.0).
ARG AIRFLOW_VERSION_SPECIFICATION
# By default we install providers from PyPI but in case of Breeze build we want to install providers
# from local sources without the need of preparing provider packages upfront. This value is
# automatically overridden by Breeze scripts.
ARG INSTALL_PROVIDERS_FROM_SOURCES="false"
# Determines the way airflow is installed. By default we install airflow from PyPI `apache-airflow` package
# But it also can be `.` from local installation or GitHub URL pointing to specific branch or tag
# Of Airflow. Note That for local source installation you need to have local sources of
# Airflow checked out together with the Dockerfile and AIRFLOW_SOURCES_FROM and AIRFLOW_SOURCES_TO
# set to "." and "/opt/airflow" respectively. Similarly AIRFLOW_SOURCES_WWW_FROM/TO are set to right source
# and destination
ARG AIRFLOW_INSTALLATION_METHOD="apache-airflow"
# By default we do not upgrade to latest dependencies
ARG UPGRADE_TO_NEWER_DEPENDENCIES="false"
# By default we install latest airflow from PyPI so we do not need to copy sources of Airflow
# www to compile the assets but in case of breeze/CI builds we use latest sources and we override those
# those SOURCES_FROM/TO with "airflow/www" and "/opt/airflow/airflow/www" respectively.
# This is to rebuild the assets only when any of the www sources change
ARG AIRFLOW_SOURCES_WWW_FROM="empty"
ARG AIRFLOW_SOURCES_WWW_TO="/empty"

# By default we install latest airflow from PyPI so we do not need to copy sources of Airflow
# but in case of breeze/CI builds we use latest sources and we override those
# those SOURCES_FROM/TO with "." and "/opt/airflow" respectively
ARG AIRFLOW_SOURCES_FROM="empty"
ARG AIRFLOW_SOURCES_TO="/empty"

ARG AIRFLOW_HOME
ARG AIRFLOW_USER_HOME_DIR
ARG AIRFLOW_UID

ENV INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT} \
    INSTALL_MSSQL_CLIENT=${INSTALL_MSSQL_CLIENT} \
    INSTALL_POSTGRES_CLIENT=${INSTALL_POSTGRES_CLIENT}

# Only copy mysql/mssql installation scripts for now - so that changing the other
# scripts which are needed much later will not invalidate the docker layer here
COPY scripts/docker/install_mysql.sh scripts/docker/install_mssql.sh scripts/docker/install_postgres.sh /scripts/docker/

RUN bash /scripts/docker/install_mysql.sh dev && \
    bash /scripts/docker/install_mssql.sh && \
    bash /scripts/docker/install_postgres.sh dev
ENV PATH=${PATH}:/opt/mssql-tools/bin

COPY docker-context-files /docker-context-files

RUN adduser --gecos "First Last,RoomNumber,WorkPhone,HomePhone" --disabled-password \
       --quiet "airflow" --uid "${AIRFLOW_UID}" --gid "0" --home "${AIRFLOW_USER_HOME_DIR}" && \
    mkdir -p ${AIRFLOW_HOME} && chown -R "airflow:0" "${AIRFLOW_USER_HOME_DIR}" ${AIRFLOW_HOME}

USER airflow

RUN if [[ -f /docker-context-files/pip.conf ]]; then \
        mkdir -p ${AIRFLOW_USER_HOME_DIR}/.config/pip; \
        cp /docker-context-files/pip.conf "${AIRFLOW_USER_HOME_DIR}/.config/pip/pip.conf"; \
    fi; \
    if [[ -f /docker-context-files/.piprc ]]; then \
        cp /docker-context-files/.piprc "${AIRFLOW_USER_HOME_DIR}/.piprc"; \
    fi

ENV AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION} \
    AIRFLOW_PRE_CACHED_PIP_PACKAGES=${AIRFLOW_PRE_CACHED_PIP_PACKAGES} \
    INSTALL_PROVIDERS_FROM_SOURCES=${INSTALL_PROVIDERS_FROM_SOURCES} \
    AIRFLOW_VERSION=${AIRFLOW_VERSION} \
    AIRFLOW_INSTALLATION_METHOD=${AIRFLOW_INSTALLATION_METHOD} \
    AIRFLOW_VERSION_SPECIFICATION=${AIRFLOW_VERSION_SPECIFICATION} \
    AIRFLOW_SOURCES_FROM=${AIRFLOW_SOURCES_FROM} \
    AIRFLOW_SOURCES_TO=${AIRFLOW_SOURCES_TO} \
    AIRFLOW_REPO=${AIRFLOW_REPO} \
    AIRFLOW_BRANCH=${AIRFLOW_BRANCH} \
    AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS}${ADDITIONAL_AIRFLOW_EXTRAS:+,}${ADDITIONAL_AIRFLOW_EXTRAS} \
    CONSTRAINTS_GITHUB_REPOSITORY=${CONSTRAINTS_GITHUB_REPOSITORY} \
    AIRFLOW_CONSTRAINTS=${AIRFLOW_CONSTRAINTS} \
    AIRFLOW_CONSTRAINTS_REFERENCE=${AIRFLOW_CONSTRAINTS_REFERENCE} \
    AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION} \
    DEFAULT_CONSTRAINTS_BRANCH=${DEFAULT_CONSTRAINTS_BRANCH} \
    PATH=${PATH}:${AIRFLOW_USER_HOME_DIR}/.local/bin \
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION} \
    PIP_PROGRESS_BAR=${PIP_PROGRESS_BAR} \
    AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR} \
    AIRFLOW_HOME=${AIRFLOW_HOME} \
    AIRFLOW_UID=${AIRFLOW_UID} \
    AIRFLOW_INSTALL_EDITABLE_FLAG="" \
    UPGRADE_TO_NEWER_DEPENDENCIES=${UPGRADE_TO_NEWER_DEPENDENCIES} \
    # By default PIP installs everything to ~/.local
    PIP_USER="true"

# Copy all scripts required for installation - changing any of those should lead to
# rebuilding from here
COPY --chown=airflow:0 scripts/docker/common.sh scripts/docker/install_pip_version.sh \
    /scripts/docker/install_airflow_dependencies_from_branch_tip.sh \
    /scripts/docker/

# In case of Production build image segment we want to pre-install main version of airflow
# dependencies from GitHub so that we do not have to always reinstall it from the scratch.
# The Airflow (and providers in case INSTALL_PROVIDERS_FROM_SOURCES is "false")
# are uninstalled, only dependencies remain
# the cache is only used when "upgrade to newer dependencies" is not set to automatically
# account for removed dependencies (we do not install them in the first place)
# Upgrade to specific PIP version
RUN bash /scripts/docker/install_pip_version.sh; \
    if [[ ${AIRFLOW_PRE_CACHED_PIP_PACKAGES} == "true" && \
          ${UPGRADE_TO_NEWER_DEPENDENCIES} == "false" ]]; then \
        bash /scripts/docker/install_airflow_dependencies_from_branch_tip.sh; \
    fi

COPY --chown=airflow:0 scripts/docker/compile_www_assets.sh scripts/docker/prepare_node_modules.sh /scripts/docker/
COPY --chown=airflow:0 ${AIRFLOW_SOURCES_WWW_FROM} ${AIRFLOW_SOURCES_WWW_TO}

# hadolint ignore=SC2086, SC2010
RUN if [[ ${AIRFLOW_INSTALLATION_METHOD} == "." ]]; then \
        # only prepare node modules and compile assets if the prod image is build from sources
        # otherwise they are already compiled-in. We should do it in one step with removing artifacts \
        # as we want to keep the final image small
        bash /scripts/docker/prepare_node_modules.sh; \
        REMOVE_ARTIFACTS="true" BUILD_TYPE="prod" bash /scripts/docker/compile_www_assets.sh; \
        # Copy generated dist folder (otherwise it will be overridden by the COPY step below)
        mv -f /opt/airflow/airflow/www/static/dist /tmp/dist; \
    fi;

COPY --chown=airflow:0 ${AIRFLOW_SOURCES_FROM} ${AIRFLOW_SOURCES_TO}

# Copy back the generated dist folder
RUN if [[ ${AIRFLOW_INSTALLATION_METHOD} == "." ]]; then \
        mv -f /tmp/dist /opt/airflow/airflow/www/static/dist; \
    fi;

# Add extra python dependencies
ARG ADDITIONAL_PYTHON_DEPS=""
# We can set this value to true in case we want to install .whl .tar.gz packages placed in the
# docker-context-files folder. This can be done for both - additional packages you want to install
# and for airflow as well (you have to set INSTALL_FROM_PYPI to false in this case)
ARG INSTALL_FROM_DOCKER_CONTEXT_FILES=""
# By default we install latest airflow from PyPI. You can set it to false if you want to install
# Airflow from the .whl or .tar.gz packages placed in `docker-context-files` folder.
ARG INSTALL_FROM_PYPI="true"
# Those are additional constraints that are needed for some extras but we do not want to
# Force them on the main Airflow package.
# * certifi<2021.0.0 required to keep snowflake happy
# * dill<0.3.3 required by apache-beam
# * google-ads<14.0.1 required to prevent updating google-python-api>=2.0.0
ARG EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS="dill<0.3.3 certifi<2021.0.0 google-ads<14.0.1"

ENV ADDITIONAL_PYTHON_DEPS=${ADDITIONAL_PYTHON_DEPS} \
    INSTALL_FROM_DOCKER_CONTEXT_FILES=${INSTALL_FROM_DOCKER_CONTEXT_FILES} \
    INSTALL_FROM_PYPI=${INSTALL_FROM_PYPI} \
    EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS=${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}

WORKDIR /opt/airflow

COPY --chown=airflow:0 scripts/docker/install_from_docker_context_files.sh scripts/docker/install_airflow.sh \
     scripts/docker/install_additional_dependencies.sh \
     /scripts/docker/

# hadolint ignore=SC2086, SC2010
RUN if [[ ${INSTALL_FROM_DOCKER_CONTEXT_FILES} == "true" ]]; then \
        bash /scripts/docker/install_from_docker_context_files.sh; \
    elif [[ ${INSTALL_FROM_PYPI} == "true" ]]; then \
        bash /scripts/docker/install_airflow.sh; \
    fi; \
    if [[ -n "${ADDITIONAL_PYTHON_DEPS}" ]]; then \
        bash /scripts/docker/install_additional_dependencies.sh; \
    fi; \
    find "${AIRFLOW_USER_HOME_DIR}/.local/" -name '*.pyc' -print0 | xargs -0 rm -f || true ; \
    find "${AIRFLOW_USER_HOME_DIR}/.local/" -type d -name '__pycache__' -print0 | xargs -0 rm -rf || true ; \
    # make sure that all directories and files in .local are also group accessible
    find "${AIRFLOW_USER_HOME_DIR}/.local" -executable -print0 | xargs --null chmod g+x; \
    find "${AIRFLOW_USER_HOME_DIR}/.local" -print0 | xargs --null chmod g+rw

# In case there is a requirements.txt file in "docker-context-files" it will be installed
# during the build additionally to whatever has been installed so far. It is recommended that
# the requirements.txt contains only dependencies with == version specification
RUN if [[ -f /docker-context-files/requirements.txt ]]; then \
        pip install --no-cache-dir --user -r /docker-context-files/requirements.txt; \
    fi

##############################################################################################
# This is the actual Airflow image - much smaller than the build one. We copy
# installed Airflow and all it's dependencies from the build image to make it smaller.
##############################################################################################
FROM ${PYTHON_BASE_IMAGE} as main

# Nolog bash flag is currently ignored - but you can replace it with other flags (for example
# xtrace - to show commands executed)
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "nolog", "-c"]

ARG AIRFLOW_UID

LABEL org.apache.airflow.distro="debian" \
  org.apache.airflow.module="airflow" \
  org.apache.airflow.component="airflow" \
  org.apache.airflow.image="airflow" \
  org.apache.airflow.uid="${AIRFLOW_UID}"

ARG PYTHON_BASE_IMAGE
ARG AIRFLOW_PIP_VERSION

ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE} \
    # Make sure noninteractive debian install is used and language variables set
    DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8 \
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION}

ARG RUNTIME_APT_DEPS="\
       apt-transport-https \
       apt-utils \
       ca-certificates \
       curl \
       dumb-init \
       freetds-bin \
       gosu \
       krb5-user \
       ldap-utils \
       libldap-2.4-2 \
       libsasl2-2 \
       libsasl2-modules \
       libssl1.1 \
       locales  \
       lsb-release \
       netcat \
       openssh-client \
       rsync \
       sasl2-bin \
       sqlite3 \
       sudo \
       unixodbc"
ARG ADDITIONAL_RUNTIME_APT_DEPS=""
ARG RUNTIME_APT_COMMAND="echo"
ARG ADDITIONAL_RUNTIME_APT_COMMAND=""
ARG ADDITIONAL_RUNTIME_APT_ENV=""
ARG INSTALL_MYSQL_CLIENT="true"
ARG INSTALL_MSSQL_CLIENT="true"
ARG INSTALL_POSTGRES_CLIENT="true"

ENV RUNTIME_APT_DEPS=${RUNTIME_APT_DEPS} \
    ADDITIONAL_RUNTIME_APT_DEPS=${ADDITIONAL_RUNTIME_APT_DEPS} \
    RUNTIME_APT_COMMAND=${RUNTIME_APT_COMMAND} \
    ADDITIONAL_RUNTIME_APT_COMMAND=${ADDITIONAL_RUNTIME_APT_COMMAND} \
    INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT} \
    INSTALL_MSSQL_CLIENT=${INSTALL_MSSQL_CLIENT} \
    INSTALL_POSTGRES_CLIENT=${INSTALL_POSTGRES_CLIENT} \
    GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm" \
    AIRFLOW_INSTALLATION_METHOD=${AIRFLOW_INSTALLATION_METHOD}

COPY scripts/docker/determine_debian_version_specific_variables.sh /scripts/docker/

# Install basic and additional apt dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1 \
    && apt-get install -y --no-install-recommends curl gnupg2 lsb-release \
    && export ${ADDITIONAL_RUNTIME_APT_ENV?} \
    && source /scripts/docker/determine_debian_version_specific_variables.sh \
    && bash -o pipefail -o errexit -o nounset -o nolog -c "${RUNTIME_APT_COMMAND}" \
    && bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_RUNTIME_APT_COMMAND}" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           ${RUNTIME_APT_DEPS} \
           "${DISTRO_LIBFFI}" \
           ${ADDITIONAL_RUNTIME_APT_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/log/*

# Having the variable in final image allows to disable providers manager warnings when
# production image is prepared from sources rather than from package
ARG AIRFLOW_INSTALLATION_METHOD="apache-airflow"
ARG AIRFLOW_IMAGE_REPOSITORY
ARG AIRFLOW_IMAGE_README_URL
ARG AIRFLOW_USER_HOME_DIR
ARG AIRFLOW_HOME

# By default PIP installs everything to ~/.local
ENV PATH="${AIRFLOW_USER_HOME_DIR}/.local/bin:${PATH}" \
    AIRFLOW_UID=${AIRFLOW_UID} \
    AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR} \
    AIRFLOW_HOME=${AIRFLOW_HOME}

# Only copy mysql/mssql installation scripts for now - so that changing the other
# scripts which are needed much later will not invalidate the docker layer here.
COPY scripts/docker/install_mysql.sh /scripts/docker/install_mssql.sh /scripts/docker/install_postgres.sh /scripts/docker/
# We run scripts with bash here to make sure we can execute the scripts. Changing to +x might have an
# unexpected result - the cache for Dockerfiles might get invalidated in case the host system
# had different umask set and group x bit was not set. In Azure the bit might be not set at all.
# That also protects against AUFS Docker backen dproblem where changing the executable bit required sync
RUN bash /scripts/docker/install_mysql.sh prod \
    && bash /scripts/docker/install_mssql.sh \
    && bash /scripts/docker/install_postgres.sh prod \
    && adduser --gecos "First Last,RoomNumber,WorkPhone,HomePhone" --disabled-password \
           --quiet "airflow" --uid "${AIRFLOW_UID}" --gid "0" --home "${AIRFLOW_USER_HOME_DIR}" \
# Make Airflow files belong to the root group and are accessible. This is to accommodate the guidelines from
# OpenShift https://docs.openshift.com/enterprise/3.0/creating_images/guidelines.html
    && mkdir -pv "${AIRFLOW_HOME}" \
    && mkdir -pv "${AIRFLOW_HOME}/dags" \
    && mkdir -pv "${AIRFLOW_HOME}/logs" \
    && chown -R airflow:0 "${AIRFLOW_USER_HOME_DIR}" "${AIRFLOW_HOME}" \
    && chmod -R g+rw "${AIRFLOW_USER_HOME_DIR}" "${AIRFLOW_HOME}" \
    && find "${AIRFLOW_HOME}" -executable -print0 | xargs --null chmod g+x \
    && find "${AIRFLOW_USER_HOME_DIR}" -executable -print0 | xargs --null chmod g+x

COPY --chown=airflow:0 --from=airflow-build-image \
     "${AIRFLOW_USER_HOME_DIR}/.local" "${AIRFLOW_USER_HOME_DIR}/.local"
COPY --chown=airflow:0 scripts/in_container/prod/entrypoint_prod.sh /entrypoint
COPY --chown=airflow:0 scripts/in_container/prod/clean-logs.sh /clean-logs

# Make /etc/passwd root-group-writeable so that user can be dynamically added by OpenShift
# See https://github.com/apache/airflow/issues/9248
# Set default groups for airflow and root user

RUN chmod a+x /entrypoint /clean-logs \
    && chmod g=u /etc/passwd \
    && chmod g+w "${AIRFLOW_USER_HOME_DIR}/.local" \
    && usermod -g 0 airflow -G 0

# make sure that the venv is activated for all users
# including plain sudo, sudo with --interactive flag
RUN sed --in-place=.bak "s/secure_path=\"/secure_path=\"\/.venv\/bin:/" /etc/sudoers

ARG AIRFLOW_VERSION

# See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
# to learn more about the way how signals are handled by the image
# Also set airflow as nice PROMPT message.
# LD_PRELOAD is to workaround https://github.com/apache/airflow/issues/17546
# issue with /usr/lib/x86_64-linux-gnu/libstdc++.so.6: cannot allocate memory in static TLS block
# We do not yet a more "correct" solution to the problem but in order to avoid raising new issues
# by users of the prod image, we implement the workaround now.
# The side effect of this is slightly (in the range of 100s of milliseconds) slower load for any
# binary started and a little memory used for Heap allocated by initialization of libstdc++
# This overhead is not happening for binaries that already link dynamically libstdc++
ENV DUMB_INIT_SETSID="1" \
    PS1="(airflow)" \
    LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libstdc++.so.6" \
    AIRFLOW_VERSION=${AIRFLOW_VERSION} \
    AIRFLOW__CORE__LOAD_EXAMPLES="false" \
    PIP_USER="true"

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

USER ${AIRFLOW_UID}

# Those should be set and used as late as possible as any change in commit/build otherwise invalidates the
# layers right after
ARG BUILD_ID
ARG COMMIT_SHA
ARG AIRFLOW_IMAGE_REPOSITORY
ARG AIRFLOW_IMAGE_DATE_CREATED

ENV BUILD_ID=${BUILD_ID} COMMIT_SHA=${COMMIT_SHA}

LABEL org.apache.airflow.distro="debian" \
  org.apache.airflow.module="airflow" \
  org.apache.airflow.component="airflow" \
  org.apache.airflow.image="airflow" \
  org.apache.airflow.version="${AIRFLOW_VERSION}" \
  org.apache.airflow.uid="${AIRFLOW_UID}" \
  org.apache.airflow.main-image.build-id="${BUILD_ID}" \
  org.apache.airflow.main-image.commit-sha="${COMMIT_SHA}" \
  org.opencontainers.image.source="${AIRFLOW_IMAGE_REPOSITORY}" \
  org.opencontainers.image.created=${AIRFLOW_IMAGE_DATE_CREATED} \
  org.opencontainers.image.authors="dev@airflow.apache.org" \
  org.opencontainers.image.url="https://airflow.apache.org" \
  org.opencontainers.image.documentation="https://airflow.apache.org/docs/docker-stack/index.html" \
  org.opencontainers.image.version="${AIRFLOW_VERSION}" \
  org.opencontainers.image.revision="${COMMIT_SHA}" \
  org.opencontainers.image.vendor="Apache Software Foundation" \
  org.opencontainers.image.licenses="Apache-2.0" \
  org.opencontainers.image.ref.name="airflow" \
  org.opencontainers.image.title="Production Airflow Image" \
  org.opencontainers.image.description="Reference, production-ready Apache Airflow image" \
  io.artifacthub.package.license='Apache-2.0' \
  io.artifacthub.package.readme-url='${AIRFLOW_IMAGE_README_URL}'

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
