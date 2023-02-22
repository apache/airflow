# syntax=docker/dockerfile:1.4
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
ARG AIRFLOW_EXTRAS="amazon,async,celery,cncf.kubernetes,dask,docker,elasticsearch,ftp,google,google_auth,grpc,hashicorp,http,ldap,microsoft.azure,mysql,odbc,pandas,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv"
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
ARG ADDITIONAL_PYTHON_DEPS=""

ARG AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_UID="50000"
ARG AIRFLOW_USER_HOME_DIR=/home/airflow

# latest released version here
ARG AIRFLOW_VERSION="2.5.1"

ARG PYTHON_BASE_IMAGE="python:3.7-slim-bullseye"

ARG AIRFLOW_PIP_VERSION=23.0
ARG AIRFLOW_IMAGE_REPOSITORY="https://github.com/apache/airflow"
ARG AIRFLOW_IMAGE_README_URL="https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"

# By default latest released version of airflow is installed (when empty) but this value can be overridden
# and we can install version according to specification (For example ==2.0.2 or <3.0.0).
ARG AIRFLOW_VERSION_SPECIFICATION=""

# By default PIP has progress bar but you can disable it.
ARG PIP_PROGRESS_BAR="on"

##############################################################################################
# This is the script image where we keep all inlined bash scripts needed in other segments
##############################################################################################
FROM scratch as scripts

##############################################################################################
# Please DO NOT modify the inlined scripts manually. The content of those files will be
# replaced by pre-commit automatically from the "scripts/docker/" folder.
# This is done in order to avoid problems with caching and file permissions and in order to
# make the PROD Dockerfile standalone
##############################################################################################

# The content below is automatically copied from scripts/docker/install_os_dependencies.sh
COPY <<"EOF" /install_os_dependencies.sh
set -euo pipefail

DOCKER_CLI_VERSION=20.10.9

if [[ "$#" != 1 ]]; then
    echo "ERROR! There should be 'runtime' or 'dev' parameter passed as argument.".
    exit 1
fi

if [[ "${1}" == "runtime" ]]; then
    INSTALLATION_TYPE="RUNTIME"
elif   [[ "${1}" == "dev" ]]; then
    INSTALLATION_TYPE="dev"
else
    echo "ERROR! Wrong argument. Passed ${1} and it should be one of 'runtime' or 'dev'.".
    exit 1
fi

function get_dev_apt_deps() {
    if [[ "${DEV_APT_DEPS=}" == "" ]]; then
        DEV_APT_DEPS="apt-transport-https apt-utils build-essential ca-certificates dirmngr \
freetds-bin freetds-dev git gosu graphviz graphviz-dev krb5-user ldap-utils libffi-dev \
libkrb5-dev libldap2-dev libleveldb1d libleveldb-dev libsasl2-2 libsasl2-dev libsasl2-modules \
libssl-dev locales lsb-release openssh-client sasl2-bin \
software-properties-common sqlite3 sudo unixodbc unixodbc-dev"
        export DEV_APT_DEPS
    fi
}

function get_runtime_apt_deps() {
    if [[ "${RUNTIME_APT_DEPS=}" == "" ]]; then
        RUNTIME_APT_DEPS="apt-transport-https apt-utils ca-certificates \
curl dumb-init freetds-bin gosu krb5-user \
ldap-utils libffi7 libldap-2.4-2 libsasl2-2 libsasl2-modules libssl1.1 locales \
lsb-release netcat openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc"
        export RUNTIME_APT_DEPS
    fi
}

function install_docker_cli() {
    local platform
    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        platform="aarch64"
    else
        platform="x86_64"
    fi
    curl --silent \
        "https://download.docker.com/linux/static/stable/${platform}/docker-${DOCKER_CLI_VERSION}.tgz" \
        |  tar -C /usr/bin --strip-components=1 -xvzf - docker/docker
}

function install_debian_dev_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends curl gnupg2 lsb-release
    # shellcheck disable=SC2086
    export ${ADDITIONAL_DEV_APT_ENV?}
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${DEV_APT_COMMAND}"
    fi
    if [[ ${ADDITIONAL_DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_DEV_APT_COMMAND}"
    fi
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${DEV_APT_DEPS} ${ADDITIONAL_DEV_APT_DEPS}
}

function install_debian_runtime_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends curl gnupg2 lsb-release
    # shellcheck disable=SC2086
    export ${ADDITIONAL_RUNTIME_APT_ENV?}
    if [[ "${RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${RUNTIME_APT_COMMAND}"
    fi
    if [[ "${ADDITIONAL_RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_RUNTIME_APT_COMMAND}"
    fi
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${RUNTIME_APT_DEPS} ${ADDITIONAL_RUNTIME_APT_DEPS}
    apt-get autoremove -yqq --purge
    apt-get clean
    rm -rf /var/lib/apt/lists/* /var/log/*
}

if [[ "${INSTALLATION_TYPE}" == "RUNTIME" ]]; then
    get_runtime_apt_deps
    install_debian_runtime_dependencies
    install_docker_cli

else
    get_dev_apt_deps
    install_debian_dev_dependencies
    install_docker_cli
fi
EOF

# The content below is automatically copied from scripts/docker/install_mysql.sh
COPY <<"EOF" /install_mysql.sh
set -euo pipefail
declare -a packages

MYSQL_VERSION="8.0"
readonly MYSQL_VERSION
MARIADB_VERSION="10.5"
readonly MARIADB_VERSION

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_YELLOW=$'\e[1;33m'
readonly COLOR_YELLOW
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"

install_mysql_client() {
    if [[ "${1}" == "dev" ]]; then
        packages=("libmysqlclient-dev" "mysql-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("libmysqlclient21" "mysql-client")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    echo
    echo "${COLOR_BLUE}Installing mysql client version ${MYSQL_VERSION}: ${1}${COLOR_RESET}"
    echo

    local key="467B942D3A79BD29"
    readonly key

    GNUPGHOME="$(mktemp -d)"
    export GNUPGHOME
    set +e
    for keyserver in $(shuf -e ha.pool.sks-keyservers.net hkp://p80.pool.sks-keyservers.net:80 \
                               keyserver.ubuntu.com hkp://keyserver.ubuntu.com:80)
    do
        gpg --keyserver "${keyserver}" --recv-keys "${key}" 2>&1 && break
    done
    set -e
    gpg --export "${key}" > /etc/apt/trusted.gpg.d/mysql.gpg
    gpgconf --kill all
    rm -rf "${GNUPGHOME}"
    unset GNUPGHOME
    echo "deb http://repo.mysql.com/apt/debian/ $(lsb_release -cs) mysql-${MYSQL_VERSION}" > /etc/apt/sources.list.d/mysql.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

install_mariadb_client() {
    if [[ "${1}" == "dev" ]]; then
        packages=("libmariadb-dev" "mariadb-client-core-${MARIADB_VERSION}")
    elif [[ "${1}" == "prod" ]]; then
        packages=("mariadb-client-core-${MARIADB_VERSION}")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    echo
    echo "${COLOR_BLUE}Installing MariaDB client version ${MARIADB_VERSION}: ${1}${COLOR_RESET}"
    echo "${COLOR_YELLOW}MariaDB client binary compatible with MySQL client.${COLOR_RESET}"
    echo
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    if [[ $(uname -m) == "arm64" || $(uname -m) == "aarch64" ]]; then
        install_mariadb_client "${@}"
    else
        install_mysql_client "${@}"
    fi
fi
EOF

# The content below is automatically copied from scripts/docker/install_mssql.sh
COPY <<"EOF" /install_mssql.sh
set -euo pipefail

: "${INSTALL_MSSQL_CLIENT:?Should be true or false}"

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

function install_mssql_client() {
    # Install MsSQL client from Microsoft repositories
    if [[ ${INSTALL_MSSQL_CLIENT:="true"} != "true" ]]; then
        echo
        echo "${COLOR_BLUE}Skip installing mssql client${COLOR_RESET}"
        echo
        return
    fi
    echo
    echo "${COLOR_BLUE}Installing mssql client${COLOR_RESET}"
    echo
    local distro
    local version
    distro=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
    version=$(lsb_release -rs)
    local driver=msodbcsql18
    curl --silent https://packages.microsoft.com/keys/microsoft.asc | apt-key add - >/dev/null 2>&1
    curl --silent "https://packages.microsoft.com/config/${distro}/${version}/prod.list" > \
        /etc/apt/sources.list.d/mssql-release.list
    apt-get update -yqq
    apt-get upgrade -yqq
    ACCEPT_EULA=Y apt-get -yqq install -y --no-install-recommends "${driver}"
    rm -rf /var/lib/apt/lists/*
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

install_mssql_client "${@}"
EOF

# The content below is automatically copied from scripts/docker/install_postgres.sh
COPY <<"EOF" /install_postgres.sh
set -euo pipefail
declare -a packages

COLOR_BLUE=$'\e[34m'
readonly COLOR_BLUE
COLOR_RESET=$'\e[0m'
readonly COLOR_RESET

: "${INSTALL_POSTGRES_CLIENT:?Should be true or false}"

install_postgres_client() {
    echo
    echo "${COLOR_BLUE}Installing postgres client${COLOR_RESET}"
    echo

    if [[ "${1}" == "dev" ]]; then
        packages=("libpq-dev" "postgresql-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("postgresql-client")
    else
        echo
        echo "Specify either prod or dev"
        echo
        exit 1
    fi

    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
    echo "deb https://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

if [[ ${INSTALL_POSTGRES_CLIENT:="true"} == "true" ]]; then
    install_postgres_client "${@}"
fi
EOF

# The content below is automatically copied from scripts/docker/install_pip_version.sh
COPY <<"EOF" /install_pip_version.sh
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_PIP_VERSION:?Should be set}"

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::show_pip_version_and_location

common::install_pip_version
EOF

# The content below is automatically copied from scripts/docker/install_airflow_dependencies_from_branch_tip.sh
COPY <<"EOF" /install_airflow_dependencies_from_branch_tip.sh

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_REPO:?Should be set}"
: "${AIRFLOW_BRANCH:?Should be set}"
: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${INSTALL_POSTGRES_CLIENT:?Should be true or false}"
: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow_dependencies_from_branch_tip() {
    echo
    echo "${COLOR_BLUE}Installing airflow from ${AIRFLOW_BRANCH}. It is used to cache dependencies${COLOR_RESET}"
    echo
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
       AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
    fi
    # Install latest set of dependencies using constraints. In case constraints were upgraded and there
    # are conflicts, this might fail, but it should be fixed in the following installation steps
    set -x
    pip install --root-user-action ignore \
      ${ADDITIONAL_PIP_INSTALL_FLAGS} \
      "https://github.com/${AIRFLOW_REPO}/archive/${AIRFLOW_BRANCH}.tar.gz#egg=apache-airflow[${AIRFLOW_EXTRAS}]" \
      --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" || true
    common::install_pip_version
    pip freeze | grep apache-airflow-providers | xargs pip uninstall --yes 2>/dev/null || true
    set +x
    echo
    echo "${COLOR_BLUE}Uninstalling just airflow. Dependencies remain. Now target airflow can be reinstalled using mostly cached dependencies${COLOR_RESET}"
    echo
    pip uninstall --yes apache-airflow || true
}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_airflow_dependencies_from_branch_tip
EOF

# The content below is automatically copied from scripts/docker/common.sh
COPY <<"EOF" /common.sh
set -euo pipefail

function common::get_colors() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
}


function common::get_airflow_version_specification() {
    if [[ -z ${AIRFLOW_VERSION_SPECIFICATION=}
        && -n ${AIRFLOW_VERSION}
        && ${AIRFLOW_INSTALLATION_METHOD} != "." ]]; then
        AIRFLOW_VERSION_SPECIFICATION="==${AIRFLOW_VERSION}"
    fi
}

function common::override_pip_version_if_needed() {
    if [[ -n ${AIRFLOW_VERSION} ]]; then
        if [[ ${AIRFLOW_VERSION} =~ ^2\.0.* || ${AIRFLOW_VERSION} =~ ^1\.* ]]; then
            export AIRFLOW_PIP_VERSION="23.0"
        fi
    fi
}

function common::get_constraints_location() {
    # auto-detect Airflow-constraint reference and location
    if [[ -z "${AIRFLOW_CONSTRAINTS_REFERENCE=}" ]]; then
        if  [[ ${AIRFLOW_VERSION} =~ v?2.* && ! ${AIRFLOW_VERSION} =~ .*dev.* ]]; then
            AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}
        else
            AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}
        fi
    fi

    if [[ -z ${AIRFLOW_CONSTRAINTS_LOCATION=} ]]; then
        local constraints_base="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${AIRFLOW_CONSTRAINTS_REFERENCE}"
        local python_version
        python_version="$(python --version 2>/dev/stdout | cut -d " " -f 2 | cut -d "." -f 1-2)"
        AIRFLOW_CONSTRAINTS_LOCATION="${constraints_base}/${AIRFLOW_CONSTRAINTS_MODE}-${python_version}.txt"
    fi
}

function common::show_pip_version_and_location() {
   echo "PATH=${PATH}"
   echo "pip on path: $(which pip)"
   echo "Using pip: $(pip --version)"
}

function common::install_pip_version() {
    echo
    echo "${COLOR_BLUE}Installing pip version ${AIRFLOW_PIP_VERSION}${COLOR_RESET}"
    echo
    if [[ ${AIRFLOW_PIP_VERSION} =~ .*https.* ]]; then
        pip install --disable-pip-version-check --no-cache-dir "pip @ ${AIRFLOW_PIP_VERSION}"
    else
        pip install --disable-pip-version-check --no-cache-dir "pip==${AIRFLOW_PIP_VERSION}"
    fi
    mkdir -p "${HOME}/.local/bin"
}
EOF

# The content below is automatically copied from scripts/docker/pip
COPY <<"EOF" /pip
#!/usr/bin/env bash
COLOR_RED=$'\e[31m'
COLOR_RESET=$'\e[0m'
COLOR_YELLOW=$'\e[33m'

if [[ $(id -u) == "0" ]]; then
    echo
    echo "${COLOR_RED}You are running pip as root. Please use 'airflow' user to run pip!${COLOR_RESET}"
    echo
    echo "${COLOR_YELLOW}See: https://airflow.apache.org/docs/docker-stack/build.html#adding-a-new-pypi-package${COLOR_RESET}"
    echo
    exit 1
fi
exec "${HOME}"/.local/bin/pip "${@}"
EOF

# The content below is automatically copied from scripts/docker/install_from_docker_context_files.sh
COPY <<"EOF" /install_from_docker_context_files.sh

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow_and_providers_from_docker_context_files(){
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
    fi
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
    fi

    if [[ ! -d /docker-context-files ]]; then
        echo
        echo "${COLOR_RED}You must provide a folder via --build-arg DOCKER_CONTEXT_FILES=<FOLDER> and you missed it!${COLOR_RESET}"
        echo
        exit 1
    fi

    # shellcheck disable=SC2206
    local pip_flags=(
        # Don't quote this -- if it is empty we don't want it to create an
        # empty array element
        --find-links="file:///docker-context-files"
    )

    # Find Apache Airflow packages in docker-context files
    local reinstalling_apache_airflow_package
    reinstalling_apache_airflow_package=$(ls \
        /docker-context-files/apache?airflow?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    # Add extras when installing airflow
    if [[ -n "${reinstalling_apache_airflow_package}" ]]; then
        # When a provider depends on a dev version of Airflow, we need to
        # specify `apache-airflow==$VER`, otherwise pip will look for it on
        # pip, and fail to find it

        # This will work as long as the wheel file is correctly named, which it
        # will be if it was build by wheel tooling
        local ver
        ver=$(basename "$reinstalling_apache_airflow_package" | cut -d "-" -f 2)
        reinstalling_apache_airflow_package="apache-airflow[${AIRFLOW_EXTRAS}]==$ver"
    fi

    # Find Apache Airflow packages in docker-context files
    local reinstalling_apache_airflow_providers_packages
    reinstalling_apache_airflow_providers_packages=$(ls \
        /docker-context-files/apache?airflow?providers*.{whl,tar.gz} 2>/dev/null || true)
    if [[ -z "${reinstalling_apache_airflow_package}" && \
          -z "${reinstalling_apache_airflow_providers_packages}" ]]; then
        return
    fi

    echo
    echo "${COLOR_BLUE}Force re-installing airflow and providers from local files with eager upgrade${COLOR_RESET}"
    echo
    # force reinstall all airflow + provider package local files with eager upgrade
    set -x
    pip install "${pip_flags[@]}" --root-user-action ignore --upgrade --upgrade-strategy eager \
        ${ADDITIONAL_PIP_INSTALL_FLAGS} \
        ${reinstalling_apache_airflow_package} ${reinstalling_apache_airflow_providers_packages} \
        ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
    set +x

    common::install_pip_version
    pip check
}

function install_all_other_packages_from_docker_context_files() {

    echo
    echo "${COLOR_BLUE}Force re-installing all other package from local files without dependencies${COLOR_RESET}"
    echo
    local reinstalling_other_packages
    # shellcheck disable=SC2010
    reinstalling_other_packages=$(ls /docker-context-files/*.{whl,tar.gz} 2>/dev/null | \
        grep -v apache_airflow | grep -v apache-airflow || true)
    if [[ -n "${reinstalling_other_packages}" ]]; then
        set -x
        pip install ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            --root-user-action ignore --force-reinstall --no-deps --no-index ${reinstalling_other_packages}
        common::install_pip_version
        set +x
    fi
}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_airflow_and_providers_from_docker_context_files

common::show_pip_version_and_location
install_all_other_packages_from_docker_context_files
EOF

# The content below is automatically copied from scripts/docker/install_airflow.sh
COPY <<"EOF" /install_airflow.sh

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

: "${AIRFLOW_PIP_VERSION:?Should be set}"

function install_airflow() {
    # Coherence check for editable installation mode.
    if [[ ${AIRFLOW_INSTALLATION_METHOD} != "." && \
          ${AIRFLOW_INSTALL_EDITABLE_FLAG} == "--editable" ]]; then
        echo
        echo "${COLOR_RED}ERROR! You can only use --editable flag when installing airflow from sources!${COLOR_RESET}"
        echo "${COLOR_RED}       Current installation method is '${AIRFLOW_INSTALLATION_METHOD} and should be '.'${COLOR_RESET}"
        exit 1
    fi
    # Remove mysql from extras if client is not going to be installed
    if [[ ${INSTALL_MYSQL_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/mysql,}
        echo "${COLOR_YELLOW}MYSQL client installation is disabled. Extra 'mysql' installations were therefore omitted.${COLOR_RESET}"
    fi
    # Remove postgres from extras if client is not going to be installed
    if [[ ${INSTALL_POSTGRES_CLIENT} != "true" ]]; then
        AIRFLOW_EXTRAS=${AIRFLOW_EXTRAS/postgres,}
        echo "${COLOR_YELLOW}Postgres client installation is disabled. Extra 'postgres' installations were therefore omitted.${COLOR_RESET}"
    fi
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo "${COLOR_BLUE}Installing all packages with eager upgrade${COLOR_RESET}"
        echo
        # eager upgrade
        pip install --root-user-action ignore --upgrade --upgrade-strategy eager \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}" \
            ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
        if [[ -n "${AIRFLOW_INSTALL_EDITABLE_FLAG}" ]]; then
            # Remove airflow and reinstall it using editable flag
            # We can only do it when we install airflow from sources
            set -x
            pip uninstall apache-airflow --yes
            pip install --root-user-action ignore ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
                ${ADDITIONAL_PIP_INSTALL_FLAGS} \
                "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
            set +x
        fi

        common::install_pip_version
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        pip check
    else \
        echo
        echo "${COLOR_BLUE}Installing all packages with constraints and upgrade if needed${COLOR_RESET}"
        echo
        set -x
        pip install --root-user-action ignore ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}" \
            --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}"
        common::install_pip_version
        # then upgrade if needed without using constraints to account for new limits in setup.py
        pip install --root-user-action ignore --upgrade --upgrade-strategy only-if-needed \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${AIRFLOW_INSTALL_EDITABLE_FLAG} \
            "${AIRFLOW_INSTALLATION_METHOD}[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
        common::install_pip_version
        set +x
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        pip check
    fi

}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_airflow
EOF

# The content below is automatically copied from scripts/docker/install_additional_dependencies.sh
COPY <<"EOF" /install_additional_dependencies.sh
set -euo pipefail

: "${UPGRADE_TO_NEWER_DEPENDENCIES:?Should be true or false}"
: "${ADDITIONAL_PYTHON_DEPS:?Should be set}"
: "${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS:?Should be set}"
: "${AIRFLOW_PIP_VERSION:?Should be set}"

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function install_additional_dependencies() {
    if [[ "${UPGRADE_TO_NEWER_DEPENDENCIES}" != "false" ]]; then
        echo
        echo "${COLOR_BLUE}Installing additional dependencies while upgrading to newer dependencies${COLOR_RESET}"
        echo
        set -x
        pip install --root-user-action ignore --upgrade --upgrade-strategy eager \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${ADDITIONAL_PYTHON_DEPS} ${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}
        common::install_pip_version
        set +x
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        pip check
    else
        echo
        echo "${COLOR_BLUE}Installing additional dependencies upgrading only if needed${COLOR_RESET}"
        echo
        set -x
        pip install --root-user-action ignore --upgrade --upgrade-strategy only-if-needed \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${ADDITIONAL_PYTHON_DEPS}
        common::install_pip_version
        set +x
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        pip check
    fi
}

common::get_colors
common::get_airflow_version_specification
common::override_pip_version_if_needed
common::get_constraints_location
common::show_pip_version_and_location

install_additional_dependencies
EOF


# The content below is automatically copied from scripts/docker/entrypoint_prod.sh
COPY <<"EOF" /entrypoint_prod.sh
#!/usr/bin/env bash
AIRFLOW_COMMAND="${1:-}"

set -euo pipefail

LD_PRELOAD="/usr/lib/$(uname -m)-linux-gnu/libstdc++.so.6"
export LD_PRELOAD

function run_check_with_retries {
    local cmd
    cmd="${1}"
    local countdown
    countdown="${CONNECTION_CHECK_MAX_COUNT}"

    while true
    do
        set +e
        local last_check_result
        local res
        last_check_result=$(eval "${cmd} 2>&1")
        res=$?
        set -e
        if [[ ${res} == 0 ]]; then
            echo
            break
        else
            echo -n "."
            countdown=$((countdown-1))
        fi
        if [[ ${countdown} == 0 ]]; then
            echo
            echo "ERROR! Maximum number of retries (${CONNECTION_CHECK_MAX_COUNT}) reached."
            echo
            echo "Last check result:"
            echo "$ ${cmd}"
            echo "${last_check_result}"
            echo
            exit 1
        else
            sleep "${CONNECTION_CHECK_SLEEP_TIME}"
        fi
    done
}

function run_nc() {
    # Checks if it is possible to connect to the host using netcat.
    #
    # We want to avoid misleading messages and perform only forward lookup of the service IP address.
    # Netcat when run without -n performs both forward and reverse lookup and fails if the reverse
    # lookup name does not match the original name even if the host is reachable via IP. This happens
    # randomly with docker-compose in GitHub Actions.
    # Since we are not using reverse lookup elsewhere, we can perform forward lookup in python
    # And use the IP in NC and add '-n' switch to disable any DNS use.
    # Even if this message might be harmless, it might hide the real reason for the problem
    # Which is the long time needed to start some services, seeing this message might be totally misleading
    # when you try to analyse the problem, that's why it's best to avoid it,
    local host="${1}"
    local port="${2}"
    local ip
    ip=$(python -c "import socket; print(socket.gethostbyname('${host}'))")
    nc -zvvn "${ip}" "${port}"
}


function wait_for_connection {
    # Waits for Connection to the backend specified via URL passed as first parameter
    # Detects backend type depending on the URL schema and assigns
    # default port numbers if not specified in the URL.
    # Then it loops until connection to the host/port specified can be established
    # It tries `CONNECTION_CHECK_MAX_COUNT` times and sleeps `CONNECTION_CHECK_SLEEP_TIME` between checks
    local connection_url
    connection_url="${1}"
    local detected_backend
    detected_backend=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).scheme)" "${connection_url}")
    local detected_host
    detected_host=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).hostname or '')" "${connection_url}")
    local detected_port
    detected_port=$(python -c "from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).port or '')" "${connection_url}")

    echo BACKEND="${BACKEND:=${detected_backend}}"
    readonly BACKEND

    if [[ -z "${detected_port=}" ]]; then
        if [[ ${BACKEND} == "postgres"* ]]; then
            detected_port=5432
        elif [[ ${BACKEND} == "mysql"* ]]; then
            detected_port=3306
        elif [[ ${BACKEND} == "mssql"* ]]; then
            detected_port=1433
        elif [[ ${BACKEND} == "redis"* ]]; then
            detected_port=6379
        elif [[ ${BACKEND} == "amqp"* ]]; then
            detected_port=5672
        fi
    fi

    detected_host=${detected_host:="localhost"}

    # Allow the DB parameters to be overridden by environment variable
    echo DB_HOST="${DB_HOST:=${detected_host}}"
    readonly DB_HOST

    echo DB_PORT="${DB_PORT:=${detected_port}}"
    readonly DB_PORT
    if [[ -n "${DB_HOST=}" ]] && [[ -n "${DB_PORT=}" ]]; then
        run_check_with_retries "run_nc ${DB_HOST@Q} ${DB_PORT@Q}"
    else
        >&2 echo "The connection details to the broker could not be determined. Connectivity checks were skipped."
    fi
}

function create_www_user() {
    local local_password=""
    # Warning: command environment variables (*_CMD) have priority over usual configuration variables
    # for configuration parameters that require sensitive information. This is the case for the SQL database
    # and the broker backend in this entrypoint script.
    if [[ -n "${_AIRFLOW_WWW_USER_PASSWORD_CMD=}" ]]; then
        local_password=$(eval "${_AIRFLOW_WWW_USER_PASSWORD_CMD}")
        unset _AIRFLOW_WWW_USER_PASSWORD_CMD
    elif [[ -n "${_AIRFLOW_WWW_USER_PASSWORD=}" ]]; then
        local_password="${_AIRFLOW_WWW_USER_PASSWORD}"
        unset _AIRFLOW_WWW_USER_PASSWORD
    fi
    if [[ -z ${local_password} ]]; then
        echo
        echo "ERROR! Airflow Admin password not set via _AIRFLOW_WWW_USER_PASSWORD or _AIRFLOW_WWW_USER_PASSWORD_CMD variables!"
        echo
        exit 1
    fi

    airflow users create \
       --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
       --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
       --lastname "${_AIRFLOW_WWW_USER_LASTNAME="Admin"}" \
       --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
       --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
       --password "${local_password}" || true
}

function create_system_user_if_missing() {
    # This is needed in case of OpenShift-compatible container execution. In case of OpenShift random
    # User id is used when starting the image, however group 0 is kept as the user group. Our production
    # Image is OpenShift compatible, so all permissions on all folders are set so that 0 group can exercise
    # the same privileges as the default "airflow" user, this code checks if the user is already
    # present in /etc/passwd and will create the system user dynamically, including setting its
    # HOME directory to the /home/airflow so that (for example) the ${HOME}/.local folder where airflow is
    # Installed can be automatically added to PYTHONPATH
    if ! whoami &> /dev/null; then
      if [[ -w /etc/passwd ]]; then
        echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${AIRFLOW_USER_HOME_DIR}:/sbin/nologin" \
            >> /etc/passwd
      fi
      export HOME="${AIRFLOW_USER_HOME_DIR}"
    fi
}

function set_pythonpath_for_root_user() {
    # Airflow is installed as a local user application which means that if the container is running as root
    # the application is not available. because Python then only load system-wide applications.
    # Now also adds applications installed as local user "airflow".
    if [[ $UID == "0" ]]; then
        local python_major_minor
        python_major_minor="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
        export PYTHONPATH="${AIRFLOW_USER_HOME_DIR}/.local/lib/python${python_major_minor}/site-packages:${PYTHONPATH:-}"
        >&2 echo "The container is run as root user. For security, consider using a regular user account."
    fi
}

function wait_for_airflow_db() {
    # Wait for the command to run successfully to validate the database connection.
    run_check_with_retries "airflow db check"
}

function upgrade_db() {
    # Runs airflow db upgrade
    airflow db upgrade || true
}

function wait_for_celery_broker() {
    # Verifies connection to Celery Broker
    local executor
    executor="$(airflow config get-value core executor)"
    if [[ "${executor}" == "CeleryExecutor" ]]; then
        local connection_url
        connection_url="$(airflow config get-value celery broker_url)"
        wait_for_connection "${connection_url}"
    fi
}

function exec_to_bash_or_python_command_if_specified() {
    # If one of the commands: 'bash', 'python' is used, either run appropriate
    # command with exec
    if [[ ${AIRFLOW_COMMAND} == "bash" ]]; then
       shift
       exec "/bin/bash" "${@}"
    elif [[ ${AIRFLOW_COMMAND} == "python" ]]; then
       shift
       exec "python" "${@}"
    fi
}

function check_uid_gid() {
    if [[ $(id -g) == "0" ]]; then
        return
    fi
    if [[ $(id -u) == "50000" ]]; then
        >&2 echo
        >&2 echo "WARNING! You should run the image with GID (Group ID) set to 0"
        >&2 echo "         even if you use 'airflow' user (UID=50000)"
        >&2 echo
        >&2 echo " You started the image with UID=$(id -u) and GID=$(id -g)"
        >&2 echo
        >&2 echo " This is to make sure you can run the image with an arbitrary UID in the future."
        >&2 echo
        >&2 echo " See more about it in the Airflow's docker image documentation"
        >&2 echo "     http://airflow.apache.org/docs/docker-stack/entrypoint"
        >&2 echo
        # We still allow the image to run with `airflow` user.
        return
    else
        >&2 echo
        >&2 echo "ERROR! You should run the image with GID=0"
        >&2 echo
        >&2 echo " You started the image with UID=$(id -u) and GID=$(id -g)"
        >&2 echo
        >&2 echo "The image should always be run with GID (Group ID) set to 0 regardless of the UID used."
        >&2 echo " This is to make sure you can run the image with an arbitrary UID."
        >&2 echo
        >&2 echo " See more about it in the Airflow's docker image documentation"
        >&2 echo "     http://airflow.apache.org/docs/docker-stack/entrypoint"
        # This will not work so we fail hard
        exit 1
    fi
}

unset PIP_USER

check_uid_gid

umask 0002

CONNECTION_CHECK_MAX_COUNT=${CONNECTION_CHECK_MAX_COUNT:=20}
readonly CONNECTION_CHECK_MAX_COUNT

CONNECTION_CHECK_SLEEP_TIME=${CONNECTION_CHECK_SLEEP_TIME:=3}
readonly CONNECTION_CHECK_SLEEP_TIME

create_system_user_if_missing
set_pythonpath_for_root_user
if [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_airflow_db
fi

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    upgrade_db
fi

if [[ -n "${_AIRFLOW_WWW_USER_CREATE=}" ]] ; then
    create_www_user
fi

if [[ -n "${_PIP_ADDITIONAL_REQUIREMENTS=}" ]] ; then
    >&2 echo
    >&2 echo "!!!!!  Installing additional requirements: '${_PIP_ADDITIONAL_REQUIREMENTS}' !!!!!!!!!!!!"
    >&2 echo
    >&2 echo "WARNING: This is a development/test feature only. NEVER use it in production!"
    >&2 echo "         Instead, build a custom image as described in"
    >&2 echo
    >&2 echo "         https://airflow.apache.org/docs/docker-stack/build.html"
    >&2 echo
    >&2 echo "         Adding requirements at container startup is fragile and is done every time"
    >&2 echo "         the container starts, so it is onlny useful for testing and trying out"
    >&2 echo "         of adding dependencies."
    >&2 echo
    pip install --root-user-action ignore --no-cache-dir ${_PIP_ADDITIONAL_REQUIREMENTS}
fi


exec_to_bash_or_python_command_if_specified "${@}"

if [[ ${AIRFLOW_COMMAND} == "airflow" ]]; then
   AIRFLOW_COMMAND="${2:-}"
   shift
fi

if [[ ${AIRFLOW_COMMAND} =~ ^(scheduler|celery)$ ]] \
    && [[ "${CONNECTION_CHECK_MAX_COUNT}" -gt "0" ]]; then
    wait_for_celery_broker
fi

exec "airflow" "${@}"
EOF

# The content below is automatically copied from scripts/docker/clean-logs.sh
COPY <<"EOF" /clean-logs.sh
#!/usr/bin/env bash


set -euo pipefail

readonly DIRECTORY="${AIRFLOW_HOME:-/usr/local/airflow}"
readonly RETENTION="${AIRFLOW__LOG_RETENTION_DAYS:-15}"

trap "exit" INT TERM

readonly EVERY=$((15*60))

echo "Cleaning logs every $EVERY seconds"

while true; do
  echo "Trimming airflow logs to ${RETENTION} days."
  find "${DIRECTORY}"/logs \
    -type d -name 'lost+found' -prune -o \
    -type f -mtime +"${RETENTION}" -name '*.log' -print0 | \
    xargs -0 rm -f

  seconds=$(( $(date -u +%s) % EVERY))
  (( seconds < 1 )) || sleep $((EVERY - seconds - 1))
  sleep 1
done
EOF

# The content below is automatically copied from scripts/docker/airflow-scheduler-autorestart.sh
COPY <<"EOF" /airflow-scheduler-autorestart.sh
#!/usr/bin/env bash

while echo "Running"; do
    airflow scheduler -n 5
    return_code=$?
    if (( return_code != 0 )); then
        echo "Scheduler crashed with exit code $return_code. Respawning.." >&2
        date >> /tmp/airflow_scheduler_errors.txt
    fi

    sleep 1
done
EOF

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

ARG DEV_APT_DEPS=""
ARG ADDITIONAL_DEV_APT_DEPS=""
ARG DEV_APT_COMMAND=""
ARG ADDITIONAL_DEV_APT_COMMAND=""
ARG ADDITIONAL_DEV_APT_ENV=""

ENV DEV_APT_DEPS=${DEV_APT_DEPS} \
    ADDITIONAL_DEV_APT_DEPS=${ADDITIONAL_DEV_APT_DEPS} \
    DEV_APT_COMMAND=${DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_COMMAND=${ADDITIONAL_DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_ENV=${ADDITIONAL_DEV_APT_ENV}

COPY --from=scripts install_os_dependencies.sh /scripts/docker/
RUN bash /scripts/docker/install_os_dependencies.sh dev

ARG INSTALL_MYSQL_CLIENT="true"
ARG INSTALL_MSSQL_CLIENT="true"
ARG INSTALL_POSTGRES_CLIENT="true"
ARG AIRFLOW_REPO=apache/airflow
ARG AIRFLOW_BRANCH=main
ARG AIRFLOW_EXTRAS
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
# Allows to override constraints source
ARG CONSTRAINTS_GITHUB_REPOSITORY="apache/airflow"
ARG AIRFLOW_CONSTRAINTS_MODE="constraints"
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
# set to "." and "/opt/airflow" respectively.
ARG AIRFLOW_INSTALLATION_METHOD="apache-airflow"
# By default we do not upgrade to latest dependencies
ARG UPGRADE_TO_NEWER_DEPENDENCIES="false"
# By default we install latest airflow from PyPI so we do not need to copy sources of Airflow
# but in case of breeze/CI builds we use latest sources and we override those
# those SOURCES_FROM/TO with "." and "/opt/airflow" respectively
ARG AIRFLOW_SOURCES_FROM="Dockerfile"
ARG AIRFLOW_SOURCES_TO="/Dockerfile"

# By default we do not install from docker context files but if we decide to install from docker context
# files, we should override those variables to "docker-context-files"
ARG DOCKER_CONTEXT_FILES="Dockerfile"

ARG AIRFLOW_HOME
ARG AIRFLOW_USER_HOME_DIR
ARG AIRFLOW_UID

ENV INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT} \
    INSTALL_MSSQL_CLIENT=${INSTALL_MSSQL_CLIENT} \
    INSTALL_POSTGRES_CLIENT=${INSTALL_POSTGRES_CLIENT}

# Only copy mysql/mssql installation scripts for now - so that changing the other
# scripts which are needed much later will not invalidate the docker layer here
COPY --from=scripts install_mysql.sh install_mssql.sh install_postgres.sh /scripts/docker/

RUN bash /scripts/docker/install_mysql.sh dev && \
    bash /scripts/docker/install_mssql.sh && \
    bash /scripts/docker/install_postgres.sh dev
ENV PATH=${PATH}:/opt/mssql-tools/bin

COPY ${DOCKER_CONTEXT_FILES} /docker-context-files

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

# Additional PIP flags passed to all pip install commands except reinstalling pip itself
ARG ADDITIONAL_PIP_INSTALL_FLAGS=""

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
    AIRFLOW_CONSTRAINTS_MODE=${AIRFLOW_CONSTRAINTS_MODE} \
    AIRFLOW_CONSTRAINTS_REFERENCE=${AIRFLOW_CONSTRAINTS_REFERENCE} \
    AIRFLOW_CONSTRAINTS_LOCATION=${AIRFLOW_CONSTRAINTS_LOCATION} \
    DEFAULT_CONSTRAINTS_BRANCH=${DEFAULT_CONSTRAINTS_BRANCH} \
    PATH=${PATH}:${AIRFLOW_USER_HOME_DIR}/.local/bin \
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION} \
    PIP_PROGRESS_BAR=${PIP_PROGRESS_BAR} \
    ADDITIONAL_PIP_INSTALL_FLAGS=${ADDITIONAL_PIP_INSTALL_FLAGS} \
    AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR} \
    AIRFLOW_HOME=${AIRFLOW_HOME} \
    AIRFLOW_UID=${AIRFLOW_UID} \
    AIRFLOW_INSTALL_EDITABLE_FLAG="" \
    UPGRADE_TO_NEWER_DEPENDENCIES=${UPGRADE_TO_NEWER_DEPENDENCIES} \
    # By default PIP installs everything to ~/.local
    PIP_USER="true"

# Copy all scripts required for installation - changing any of those should lead to
# rebuilding from here
COPY --from=scripts common.sh install_pip_version.sh \
     install_airflow_dependencies_from_branch_tip.sh /scripts/docker/

# We can set this value to true in case we want to install .whl/.tar.gz packages placed in the
# docker-context-files folder. This can be done for both additional packages you want to install
# as well as Airflow and Provider packages (it will be automatically detected if airflow
# is installed from docker-context files rather than from PyPI)
ARG INSTALL_PACKAGES_FROM_CONTEXT="false"

# In case of Production build image segment we want to pre-install main version of airflow
# dependencies from GitHub so that we do not have to always reinstall it from the scratch.
# The Airflow (and providers in case INSTALL_PROVIDERS_FROM_SOURCES is "false")
# are uninstalled, only dependencies remain
# the cache is only used when "upgrade to newer dependencies" is not set to automatically
# account for removed dependencies (we do not install them in the first place) and in case
# INSTALL_PACKAGES_FROM_CONTEXT is not set (because then caching it from main makes no sense).
RUN bash /scripts/docker/install_pip_version.sh; \
    if [[ ${AIRFLOW_PRE_CACHED_PIP_PACKAGES} == "true" && \
        ${INSTALL_PACKAGES_FROM_CONTEXT} == "false" && \
        ${UPGRADE_TO_NEWER_DEPENDENCIES} == "false" ]]; then \
        bash /scripts/docker/install_airflow_dependencies_from_branch_tip.sh; \
    fi

COPY --chown=airflow:0 ${AIRFLOW_SOURCES_FROM} ${AIRFLOW_SOURCES_TO}

# Add extra python dependencies
ARG ADDITIONAL_PYTHON_DEPS=""

# Those are additional constraints that are needed for some extras but we do not want to
# Force them on the main Airflow package.
# * dill<0.3.3 required by apache-beam
# * pyarrow>=6.0.0 is because pip resolver decides for Python 3.10 to downgrade pyarrow to 5 even if it is OK
#   for python 3.10 and other dependencies adding the limit helps resolver to make better decisions
# We need to limit the protobuf library to < 4.21.0 because not all google libraries we use
# are compatible with the new protobuf version. All the google python client libraries need
# to be upgraded to >=2.0.0 in order to able to lift that limitation
# https://developers.google.com/protocol-buffers/docs/news/2022-05-06#python-updates
# * authlib, gcloud_aio_auth, adal are needed to generate constraints for PyPI packages and can be removed after we release
#   new google, azure providers
# !!! MAKE SURE YOU SYNCHRONIZE THE LIST BETWEEN: Dockerfile, Dockerfile.ci, find_newer_dependencies.py
ARG EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS="dill<0.3.3 pyarrow>=6.0.0 protobuf<4.21.0 authlib>=1.0.0 gcloud_aio_auth>=4.0.0 adal>=1.2.7"

ENV ADDITIONAL_PYTHON_DEPS=${ADDITIONAL_PYTHON_DEPS} \
    INSTALL_PACKAGES_FROM_CONTEXT=${INSTALL_PACKAGES_FROM_CONTEXT} \
    EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS=${EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS}

WORKDIR ${AIRFLOW_HOME}

COPY --from=scripts install_from_docker_context_files.sh install_airflow.sh \
     install_additional_dependencies.sh /scripts/docker/

# hadolint ignore=SC2086, SC2010
RUN if [[ ${INSTALL_PACKAGES_FROM_CONTEXT} == "true" ]]; then \
        bash /scripts/docker/install_from_docker_context_files.sh; \
    fi; \
    if ! airflow version 2>/dev/null >/dev/null; then \
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
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8 LD_LIBRARY_PATH=/usr/local/lib \
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION}

ARG RUNTIME_APT_DEPS=""
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

COPY --from=scripts install_os_dependencies.sh /scripts/docker/
RUN bash /scripts/docker/install_os_dependencies.sh runtime

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
COPY --from=scripts install_mysql.sh install_mssql.sh install_postgres.sh /scripts/docker/
# We run scripts with bash here to make sure we can execute the scripts. Changing to +x might have an
# unexpected result - the cache for Dockerfiles might get invalidated in case the host system
# had different umask set and group x bit was not set. In Azure the bit might be not set at all.
# That also protects against AUFS Docker backend problem where changing the executable bit required sync
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

COPY --from=airflow-build-image --chown=airflow:0 \
     "${AIRFLOW_USER_HOME_DIR}/.local" "${AIRFLOW_USER_HOME_DIR}/.local"
COPY --from=scripts entrypoint_prod.sh /entrypoint
COPY --from=scripts clean-logs.sh /clean-logs
COPY --from=scripts airflow-scheduler-autorestart.sh /airflow-scheduler-autorestart

# Make /etc/passwd root-group-writeable so that user can be dynamically added by OpenShift
# See https://github.com/apache/airflow/issues/9248
# Set default groups for airflow and root user

RUN chmod a+rx /entrypoint /clean-logs \
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
ENV DUMB_INIT_SETSID="1" \
    PS1="(airflow)" \
    AIRFLOW_VERSION=${AIRFLOW_VERSION} \
    AIRFLOW__CORE__LOAD_EXAMPLES="false" \
    PIP_USER="true" \
    PATH="/root/bin:${PATH}"

# Add protection against running pip as root user
RUN mkdir -pv /root/bin
COPY --from=scripts pip /root/bin/pip
RUN chmod u+x /root/bin/pip

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
  org.opencontainers.image.description="Reference, production-ready Apache Airflow image"
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
