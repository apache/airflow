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
#                        ${HOME}/.local virtualenv which is also considered
#                        As --user folder by python when creating venv with
#                        --system-site-packages
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
ARG AIRFLOW_EXTRAS="aiobotocore,amazon,async,celery,cncf-kubernetes,common-io,common-messaging,docker,elasticsearch,fab,ftp,git,google,google-auth,graphviz,grpc,hashicorp,http,ldap,microsoft-azure,mysql,odbc,openlineage,pandas,postgres,redis,sendgrid,sftp,slack,snowflake,ssh,statsd,uv"
ARG ADDITIONAL_AIRFLOW_EXTRAS=""
ARG ADDITIONAL_PYTHON_DEPS=""

ARG AIRFLOW_HOME=/opt/airflow
ARG AIRFLOW_IMAGE_TYPE="prod"
ARG AIRFLOW_UID="50000"
ARG AIRFLOW_USER_HOME_DIR=/home/airflow

# latest released version here
ARG AIRFLOW_VERSION="3.1.5"

ARG BASE_IMAGE="debian:bookworm-slim"
ARG AIRFLOW_PYTHON_VERSION="3.12.12"

# PYTHON_LTO: Controls whether Python is built with Link-Time Optimization (LTO).
#
# Link-Time Optimization uses MD5 checksums during the compilation process to verify
# object files and intermediate representations. In FIPS-compliant environments, MD5
# is blocked as it's not an approved cryptographic algorithm (see FIPS 140-2/140-3).
# This can cause Python builds with LTO to fail when FIPS mode is enabled.
#
# When building FIPS-compliant images, set this to "false" to disable LTO:
#   docker build --build-arg PYTHON_LTO="false" ...
#
# Default: "true" (LTO enabled for better performance)
#
# Related: https://github.com/apache/airflow/issues/58337
ARG PYTHON_LTO="true"

# You can swap comments between those two args to test pip from the main version
# When you attempt to test if the version of `pip` from specified branch works for our builds
# Also use `force pip` label on your PR to swap all places we use `uv` to `pip`
ARG AIRFLOW_PIP_VERSION=25.3
# ARG AIRFLOW_PIP_VERSION="git+https://github.com/pypa/pip.git@main"
ARG AIRFLOW_UV_VERSION=0.9.18
ARG AIRFLOW_USE_UV="false"
ARG UV_HTTP_TIMEOUT="300"
ARG AIRFLOW_IMAGE_REPOSITORY="https://github.com/apache/airflow"
ARG AIRFLOW_IMAGE_README_URL="https://raw.githubusercontent.com/apache/airflow/main/docs/docker-stack/README.md"

# By default we install latest airflow from PyPI so we do not need to copy sources of Airflow
# from the host - so we are using Dockerfile and copy it to /Dockerfile in target image
# because this is the only file we know exists locally. This way you can build the image in PyPI with
# **just** the Dockerfile and no need for any other files from Airflow repository.
# However, in case of breeze/development use we use latest sources and we override those
# SOURCES_FROM/TO with "." and "/opt/airflow" respectively - so that sources of Airflow (and all providers)
# are used to build the PROD image used in tests.
ARG AIRFLOW_SOURCES_FROM="Dockerfile"
ARG AIRFLOW_SOURCES_TO="/Dockerfile"

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
# replaced by prek automatically from the "scripts/docker/" folder.
# This is done in order to avoid problems with caching and file permissions and in order to
# make the PROD Dockerfile standalone
##############################################################################################

# The content below is automatically copied from scripts/docker/install_os_dependencies.sh
COPY <<"EOF" /install_os_dependencies.sh
#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 1 ]]; then
    echo
    echo "ERROR! There should be 'runtime', 'ci' or 'dev' parameter passed as argument.".
    echo
    exit 1
fi

AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION:-3.10.18}
PYTHON_LTO=${PYTHON_LTO:-true}
GOLANG_MAJOR_MINOR_VERSION=${GOLANG_MAJOR_MINOR_VERSION:-1.24.4}

if [[ "${1}" == "runtime" ]]; then
    INSTALLATION_TYPE="RUNTIME"
elif   [[ "${1}" == "dev" ]]; then
    INSTALLATION_TYPE="DEV"
elif   [[ "${1}" == "ci" ]]; then
    INSTALLATION_TYPE="CI"
else
    echo
    echo "ERROR! Wrong argument. Passed ${1} and it should be one of 'runtime', 'ci' or 'dev'.".
    echo
    exit 1
fi

function get_dev_apt_deps() {
    if [[ "${DEV_APT_DEPS=}" == "" ]]; then
        DEV_APT_DEPS="\
apt-transport-https \
apt-utils \
build-essential \
dirmngr \
freetds-bin \
freetds-dev \
git \
graphviz \
graphviz-dev \
krb5-user \
lcov \
ldap-utils \
libbluetooth-dev \
libbz2-dev \
libc6-dev \
libdb-dev \
libev-dev \
libev4 \
libffi-dev \
libgdbm-compat-dev \
libgdbm-dev \
libgdbm-dev \
libgeos-dev \
libkrb5-dev \
libldap2-dev \
libleveldb-dev \
libleveldb1d \
liblzma-dev \
libncurses5-dev \
libreadline6-dev \
libsasl2-2 \
libsasl2-dev \
libsasl2-modules \
libsqlite3-dev \
libssl-dev \
libxmlsec1 \
libxmlsec1-dev \
libzstd-dev \
locales \
lsb-release \
lzma \
lzma-dev \
openssh-client \
openssl \
pkg-config \
pkgconf \
sasl2-bin \
sqlite3 \
sudo \
tk-dev \
unixodbc \
unixodbc-dev \
uuid-dev \
wget \
xz-utils \
zlib1g-dev \
"
        export DEV_APT_DEPS
    fi
}

function get_runtime_apt_deps() {
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    debian_version_apt_deps="\
libffi8 \
libldap-2.5-0 \
libssl3 \
netcat-openbsd\
"
    echo
    echo "APPLIED INSTALLATION CONFIGURATION FOR DEBIAN VERSION: ${debian_version}"
    echo
    if [[ "${RUNTIME_APT_DEPS=}" == "" ]]; then
        RUNTIME_APT_DEPS="\
${debian_version_apt_deps} \
apt-transport-https \
apt-utils \
curl \
dumb-init \
freetds-bin \
git \
gnupg \
iputils-ping \
krb5-user \
ldap-utils \
libev4 \
libgeos-dev \
libsasl2-2 \
libsasl2-modules \
libxmlsec1 \
locales \
lsb-release \
openssh-client \
rsync \
sasl2-bin \
sqlite3 \
sudo \
unixodbc \
wget\
"
        export RUNTIME_APT_DEPS
    fi
}

function install_docker_cli() {
    apt-get update
    apt-get install ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    # shellcheck disable=SC1091
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y --no-install-recommends docker-ce-cli
}

function install_debian_dev_dependencies() {
    apt-get update
    apt-get install -yqq --no-install-recommends apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends wget curl gnupg2 lsb-release ca-certificates
    # shellcheck disable=SC2086
    export ${ADDITIONAL_DEV_APT_ENV?}
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${DEV_APT_COMMAND}"
    fi
    if [[ ${ADDITIONAL_DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_DEV_APT_COMMAND}"
    fi
    apt-get update
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${DEV_APT_DEPS}
}

function install_additional_dev_dependencies() {
    if [[ "${ADDITIONAL_DEV_APT_DEPS=}" != "" ]]; then
        # shellcheck disable=SC2086
        apt-get install -y --no-install-recommends ${ADDITIONAL_DEV_APT_DEPS}
    fi
}

function link_python() {
    # link python binaries to /usr/local/bin and /usr/python/bin with and without 3 suffix
    # Links in /usr/local/bin are needed for tools that expect python to be there
    # Links in /usr/python/bin are needed for tools that are detecting home of python installation including
    # lib/site-packages. The /usr/python/bin should be first in PATH in order to help with the last part.
    for dst in pip3 python3 python3-config; do
        src="$(echo "${dst}" | tr -d 3)"
        echo "Linking ${dst} in /usr/local/bin and /usr/python/bin"
        ln -sv "/usr/python/bin/${dst}" "/usr/local/bin/${dst}"
        for dir in /usr/local/bin /usr/python/bin; do
            if [[ ! -e "${dir}/${src}" ]]; then
                echo "Creating ${src} - > ${dst} link in ${dir}"
                ln -sv "${dir}/${dst}" "${dir}/${src}"
            fi
        done
    done
    for dst in /usr/python/lib/*
    do
        src="/usr/local/lib/$(basename "${dst}")"
        if [[ -e "${src}" ]]; then
            rm -rf "${src}"
        fi
        echo "Linking ${dst} to ${src}"
        ln -sv "${dst}" "${src}"
    done
    ldconfig
}

function install_debian_runtime_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends wget curl gnupg2 lsb-release ca-certificates
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
    link_python
    rm -rf /var/lib/apt/lists/* /var/log/*
}

function install_python() {
    # If system python (3.11 in bookworm) is installed (via automatic installation of some dependencies for example), we need
    # to fail and make sure that it is not there, because there can be strange interactions if we install
    # newer version and system libraries are installed, because
    # when you create a virtualenv part of the shared libraries of Python can be taken from the system
    # Installation leading to weird errors when you want to install some modules - for example when you install ssl:
    # /usr/python/lib/python3.11/lib-dynload/_ssl.cpython-311-aarch64-linux-gnu.so: undefined symbol: _PyModule_Add
    if dpkg -l | grep '^ii' | grep '^ii  libpython' >/dev/null; then
        echo
        echo "ERROR! System python is installed by one of the previous steps"
        echo
        echo "Please make sure that no python packages are installed by default. Displaying the reason why libpython3.11 is installed:"
        echo
        apt-get install -yqq aptitude >/dev/null
        aptitude why libpython3.11
        echo
        exit 1
    else
        echo
        echo "GOOD! System python is not installed - OK"
        echo
    fi
    wget -O python.tar.xz "https://www.python.org/ftp/python/${AIRFLOW_PYTHON_VERSION%%[a-z]*}/Python-${AIRFLOW_PYTHON_VERSION}.tar.xz"
    wget -O python.tar.xz.asc "https://www.python.org/ftp/python/${AIRFLOW_PYTHON_VERSION%%[a-z]*}/Python-${AIRFLOW_PYTHON_VERSION}.tar.xz.asc";
    declare -A keys=(
        # gpg: key B26995E310250568: public key "\xc5\x81ukasz Langa (GPG langa.pl) <lukasz@langa.pl>" imported
        # https://peps.python.org/pep-0596/#release-manager-and-crew
        [3.9]="E3FF2839C048B25C084DEBE9B26995E310250568"
        # gpg: key 64E628F8D684696D: public key "Pablo Galindo Salgado <pablogsal@gmail.com>" imported
        # https://peps.python.org/pep-0619/#release-manager-and-crew
        [3.10]="A035C8C19219BA821ECEA86B64E628F8D684696D"
        # gpg: key 64E628F8D684696D: public key "Pablo Galindo Salgado <pablogsal@gmail.com>" imported
        # https://peps.python.org/pep-0664/#release-manager-and-crew
        [3.11]="A035C8C19219BA821ECEA86B64E628F8D684696D"
        # gpg: key A821E680E5FA6305: public key "Thomas Wouters <thomas@python.org>" imported
        # https://peps.python.org/pep-0693/#release-manager-and-crew
        [3.12]="7169605F62C751356D054A26A821E680E5FA6305"
        # gpg: key A821E680E5FA6305: public key "Thomas Wouters <thomas@python.org>" imported
        # https://peps.python.org/pep-0719/#release-manager-and-crew
        [3.13]="7169605F62C751356D054A26A821E680E5FA6305"
    )
    major_minor_version="${AIRFLOW_PYTHON_VERSION%.*}"
    echo "Verifying Python ${AIRFLOW_PYTHON_VERSION} (${major_minor_version})"
    GNUPGHOME="$(mktemp -d)"; export GNUPGHOME;
    gpg_key="${keys[${major_minor_version}]}"
    echo "Using GPG key ${gpg_key}"
    gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys "${gpg_key}"
    gpg --batch --verify python.tar.xz.asc python.tar.xz;
    gpgconf --kill all
    rm -rf "$GNUPGHOME" python.tar.xz.asc
    mkdir -p /usr/src/python
    tar --extract --directory /usr/src/python --strip-components=1 --file python.tar.xz
    rm python.tar.xz
    cd /usr/src/python
    arch="$(dpkg --print-architecture)"; arch="${arch##*-}"
    gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"
    EXTRA_CFLAGS="$(dpkg-buildflags --get CFLAGS)"
    EXTRA_CFLAGS="${EXTRA_CFLAGS:-} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer";
    LDFLAGS="$(dpkg-buildflags --get LDFLAGS)"
    LDFLAGS="${LDFLAGS:--Wl},--strip-all"
    # Link-Time Optimization (LTO) uses MD5 checksums for object file verification during
    # compilation. In FIPS mode, MD5 is blocked as a non-approved algorithm, causing builds
    # to fail. The PYTHON_LTO variable allows disabling LTO for FIPS-compliant builds.
    # See: https://github.com/apache/airflow/issues/58337
    local lto_option=""
    if [[ "${PYTHON_LTO:-true}" == "true" ]]; then
        lto_option="--with-lto"
    fi
    ./configure --enable-optimizations --prefix=/usr/python/ --with-ensurepip --build="$gnuArch" \
        --enable-loadable-sqlite-extensions --enable-option-checking=fatal \
            --enable-shared ${lto_option}
    make -s -j "$(nproc)" "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
        "LDFLAGS=${LDFLAGS:--Wl},-rpath='\$\$ORIGIN/../lib'" python
    make -s -j "$(nproc)" install
    cd /
    rm -rf /usr/src/python
    find /usr/python -depth \
      \( \
        \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
        -o \( -type f -a \( -name 'libpython*.a' \) \) \
    \) -exec rm -rf '{}' +
    link_python
}

function install_golang() {
    curl "https://dl.google.com/go/go${GOLANG_MAJOR_MINOR_VERSION}.linux-$(dpkg --print-architecture).tar.gz" -o "go${GOLANG_MAJOR_MINOR_VERSION}.linux.tar.gz"
    rm -rf /usr/local/go && tar -C /usr/local -xzf go"${GOLANG_MAJOR_MINOR_VERSION}".linux.tar.gz
}

function apt_clean() {
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false
    rm -rf /var/lib/apt/lists/* /var/log/*
}

if [[ "${INSTALLATION_TYPE}" == "RUNTIME" ]]; then
    get_runtime_apt_deps
    install_debian_runtime_dependencies
    install_docker_cli
    apt_clean
else
    get_dev_apt_deps
    install_debian_dev_dependencies
    install_python
    install_additional_dev_dependencies
    if [[ "${INSTALLATION_TYPE}" == "CI" ]]; then
        install_golang
    fi
    install_docker_cli
    apt_clean
fi
EOF

# The content below is automatically copied from scripts/docker/install_mysql.sh
COPY <<"EOF" /install_mysql.sh
#!/usr/bin/env bash
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

set -euo pipefail

common::get_colors
declare -a packages

readonly MARIADB_LTS_VERSION="10.11"

: "${INSTALL_MYSQL_CLIENT:?Should be true or false}"
: "${INSTALL_MYSQL_CLIENT_TYPE:-mariadb}"

if [[ "${INSTALL_MYSQL_CLIENT}" != "true" && "${INSTALL_MYSQL_CLIENT}" != "false" ]]; then
    echo
    echo "${COLOR_RED}INSTALL_MYSQL_CLIENT must be either true or false${COLOR_RESET}"
    echo
    exit 1
fi

if [[ "${INSTALL_MYSQL_CLIENT_TYPE}" != "mysql" && "${INSTALL_MYSQL_CLIENT_TYPE}" != "mariadb" ]]; then
    echo
    echo "${COLOR_RED}INSTALL_MYSQL_CLIENT_TYPE must be either mysql or mariadb${COLOR_RESET}"
    echo
    exit 1
fi

if [[ "${INSTALL_MYSQL_CLIENT_TYPE}" == "mysql" ]]; then
    echo
    echo "${COLOR_RED}The 'mysql' client type is not supported any more. Use 'mariadb' instead.${COLOR_RESET}"
    echo
    echo "The MySQL drivers are wrongly packaged and released by Oracle with an expiration date on their GPG keys,"
    echo "which causes builds to fail after the expiration date. MariaDB client is protocol-compatible with MySQL client."
    echo ""
    echo "Every two years the MySQL packages fail and Oracle team is always surprised and struggling"
    echo "with fixes and re-signing the packages which lasts few days"
    echo "See https://bugs.mysql.com/bug.php?id=113432 for more details."
    echo "As a community we are not able to support this broken packaging practice from Oracle"
    echo "Feel free however to install MySQL drivers on your own as extension of the image."
    echo
    exit 1
fi

retry() {
    local retries=3
    local count=0
    # adding delay of 10 seconds
    local delay=10
    until "$@"; do
        exit_code=$?
        count=$((count + 1))
        if [[ $count -lt $retries ]]; then
            echo "Command failed. Attempt $count/$retries. Retrying in ${delay}s..."
            sleep $delay
        else
            echo "Command failed after $retries attempts."
            return $exit_code
        fi
    done
}

install_mariadb_client() {
    # List of compatible package Oracle MySQL -> MariaDB:
    # `mysql-client` -> `mariadb-client` or `mariadb-client-compat` (11+)
    # `libmysqlclientXX` (where XX is a number) -> `libmariadb3-compat`
    # `libmysqlclient-dev` -> `libmariadb-dev-compat`
    #
    # Different naming against Debian repo which we used before
    # that some of packages might contains `-compat` suffix, Debian repo -> MariaDB repo:
    # `libmariadb-dev` -> `libmariadb-dev-compat`
    # `mariadb-client-core` -> `mariadb-client` or `mariadb-client-compat` (11+)
    if [[ "${1}" == "dev" ]]; then
        packages=("libmariadb-dev-compat" "mariadb-client")
    elif [[ "${1}" == "prod" ]]; then
        packages=("libmariadb3-compat" "mariadb-client")
    else
        echo
        echo "${COLOR_RED}Specify either prod or dev${COLOR_RESET}"
        echo
        exit 1
    fi

    common::import_trusted_gpg "0xF1656F24C74CD1D8" "mariadb"

    echo
    echo "${COLOR_BLUE}Installing MariaDB client version ${MARIADB_LTS_VERSION}: ${1}${COLOR_RESET}"
    echo "${COLOR_YELLOW}MariaDB client protocol-compatible with MySQL client.${COLOR_RESET}"
    echo

    echo "deb [arch=amd64,arm64] https://archive.mariadb.org/mariadb-${MARIADB_LTS_VERSION}/repo/debian/ $(lsb_release -cs) main" > \
        /etc/apt/sources.list.d/mariadb.list
    # Make sure that dependencies from MariaDB repo are preferred over Debian dependencies
    printf "Package: *\nPin: release o=MariaDB\nPin-Priority: 999\n" > /etc/apt/preferences.d/mariadb
    retry apt-get update
    retry apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

if [[ ${INSTALL_MYSQL_CLIENT:="true"} == "true" ]]; then
    install_mariadb_client "${@}"
fi
EOF

# The content below is automatically copied from scripts/docker/install_mssql.sh
COPY <<"EOF" /install_mssql.sh
#!/usr/bin/env bash
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

set -euo pipefail

common::get_colors
declare -a packages

: "${INSTALL_MSSQL_CLIENT:?Should be true or false}"


function install_mssql_client() {
    # Install MsSQL client from Microsoft repositories
    if [[ ${INSTALL_MSSQL_CLIENT:="true"} != "true" ]]; then
        echo
        echo "${COLOR_BLUE}Skip installing mssql client${COLOR_RESET}"
        echo
        return
    fi
    packages=("msodbcsql18")

    common::import_trusted_gpg "EB3E94ADBE1229CF" "microsoft"

    echo
    echo "${COLOR_BLUE}Installing mssql client${COLOR_RESET}"
    echo

    echo "deb [arch=amd64,arm64] https://packages.microsoft.com/debian/$(lsb_release -rs)/prod $(lsb_release -cs) main" > \
        /etc/apt/sources.list.d/mssql-release.list &&
    mkdir -p /opt/microsoft/msodbcsql18 &&
    touch /opt/microsoft/msodbcsql18/ACCEPT_EULA &&
    apt-get update -yqq &&
    apt-get upgrade -yqq &&
    apt-get -yqq install --no-install-recommends "${packages[@]}" &&
    apt-get autoremove -yqq --purge &&
    apt-get clean &&
    rm -rf /var/lib/apt/lists/*
}

install_mssql_client "${@}"
EOF

# The content below is automatically copied from scripts/docker/install_postgres.sh
COPY <<"EOF" /install_postgres.sh
#!/usr/bin/env bash
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"
set -euo pipefail

common::get_colors
declare -a packages

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

    common::import_trusted_gpg "7FCC7D46ACCC4CF8" "postgres"

    echo "deb [arch=amd64,arm64] https://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > \
        /etc/apt/sources.list.d/pgdg.list
    apt-get update
    apt-get install --no-install-recommends -y "${packages[@]}"
    apt-get autoremove -yqq --purge
    apt-get clean && rm -rf /var/lib/apt/lists/*
}

if [[ ${INSTALL_POSTGRES_CLIENT:="true"} == "true" ]]; then
    install_postgres_client "${@}"
fi
EOF

# The content below is automatically copied from scripts/docker/install_packaging_tools.sh
COPY <<"EOF" /install_packaging_tools.sh
#!/usr/bin/env bash
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

common::get_colors
common::get_packaging_tool
common::show_packaging_tool_version_and_location
common::install_packaging_tools
EOF

# The content below is automatically copied from scripts/docker/common.sh
COPY <<"EOF" /common.sh
#!/usr/bin/env bash
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

function common::get_packaging_tool() {
    : "${AIRFLOW_USE_UV:?Should be set}"

    ## IMPORTANT: IF YOU MODIFY THIS FUNCTION YOU SHOULD ALSO MODIFY CORRESPONDING FUNCTION IN
    ## `scripts/in_container/_in_container_utils.sh`
    if [[ ${AIRFLOW_USE_UV} == "true" ]]; then
        echo
        echo "${COLOR_BLUE}Using 'uv' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL="uv"
        export PACKAGING_TOOL_CMD="uv pip"
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
         # (binary lxml embeds its own libxml2, while xmlsec uses system one).
         # See https://bugs.launchpad.net/lxml/+bug/2110068
         if [[ ${AIRFLOW_INSTALLATION_METHOD=} == "." && -f "./pyproject.toml" ]]; then
            # for uv only install dev group when we install from sources
            export EXTRA_INSTALL_FLAGS="--group=dev --no-binary lxml --no-binary xmlsec"
        else
            export EXTRA_INSTALL_FLAGS="--no-binary lxml --no-binary xmlsec"
        fi
        export EXTRA_UNINSTALL_FLAGS=""
        export UPGRADE_TO_HIGHEST_RESOLUTION="--upgrade --resolution highest"
        export UPGRADE_IF_NEEDED="--upgrade"
        UV_CONCURRENT_DOWNLOADS=$(nproc --all)
        export UV_CONCURRENT_DOWNLOADS
        if [[ ${INCLUDE_PRE_RELEASE=} == "true" ]]; then
            EXTRA_INSTALL_FLAGS="${EXTRA_INSTALL_FLAGS} --prerelease if-necessary"
        fi
    else
        echo
        echo "${COLOR_BLUE}Using 'pip' to install Airflow${COLOR_RESET}"
        echo
        export PACKAGING_TOOL="pip"
        export PACKAGING_TOOL_CMD="pip"
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        export EXTRA_INSTALL_FLAGS="--root-user-action ignore --no-binary lxml,xmlsec"
        export EXTRA_UNINSTALL_FLAGS="--yes"
        export UPGRADE_TO_HIGHEST_RESOLUTION="--upgrade --upgrade-strategy eager"
        export UPGRADE_IF_NEEDED="--upgrade --upgrade-strategy only-if-needed"
        if [[ ${INCLUDE_PRE_RELEASE=} == "true" ]]; then
            EXTRA_INSTALL_FLAGS="${EXTRA_INSTALL_FLAGS} --pre"
        fi
    fi
}

function common::get_airflow_version_specification() {
    if [[ -z ${AIRFLOW_VERSION_SPECIFICATION=}
        && -n ${AIRFLOW_VERSION}
        && ${AIRFLOW_INSTALLATION_METHOD} != "." ]]; then
        AIRFLOW_VERSION_SPECIFICATION="==${AIRFLOW_VERSION}"
    fi
}

function common::get_constraints_location() {
    # auto-detect Airflow-constraint reference and location
    if [[ -z "${AIRFLOW_CONSTRAINTS_REFERENCE=}" ]]; then
        if  [[ ${AIRFLOW_VERSION} =~ v?2.* || ${AIRFLOW_VERSION} =~ v?3.* ]]; then
            AIRFLOW_CONSTRAINTS_REFERENCE=constraints-${AIRFLOW_VERSION}
        else
            AIRFLOW_CONSTRAINTS_REFERENCE=${DEFAULT_CONSTRAINTS_BRANCH}
        fi
    fi

    if [[ -z ${AIRFLOW_CONSTRAINTS_LOCATION=} ]]; then
        local constraints_base="https://raw.githubusercontent.com/${CONSTRAINTS_GITHUB_REPOSITORY}/${AIRFLOW_CONSTRAINTS_REFERENCE}"
        local python_version
        python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        AIRFLOW_CONSTRAINTS_LOCATION="${constraints_base}/${AIRFLOW_CONSTRAINTS_MODE}-${python_version}.txt"
    fi

    if [[ ${AIRFLOW_CONSTRAINTS_LOCATION} =~ http.* ]]; then
        echo
        echo "${COLOR_BLUE}Downloading constraints from ${AIRFLOW_CONSTRAINTS_LOCATION} to ${HOME}/constraints.txt ${COLOR_RESET}"
        echo
        curl -sSf -o "${HOME}/constraints.txt" "${AIRFLOW_CONSTRAINTS_LOCATION}"
    else
        echo
        echo "${COLOR_BLUE}Copying constraints from ${AIRFLOW_CONSTRAINTS_LOCATION} to ${HOME}/constraints.txt ${COLOR_RESET}"
        echo
        cp "${AIRFLOW_CONSTRAINTS_LOCATION}" "${HOME}/constraints.txt"
    fi
}

function common::show_packaging_tool_version_and_location() {
   echo "PATH=${PATH}"
   echo "Installed pip: $(pip --version): $(which pip)"
   if [[ ${PACKAGING_TOOL} == "pip" ]]; then
       echo "${COLOR_BLUE}Using 'pip' to install Airflow${COLOR_RESET}"
   else
       echo "${COLOR_BLUE}Using 'uv' to install Airflow${COLOR_RESET}"
       echo "Installed uv: $(uv --version 2>/dev/null || echo "Not installed yet"): $(which uv 2>/dev/null)"
   fi
}

function common::install_packaging_tools() {
    : "${AIRFLOW_USE_UV:?Should be set}"
    if [[ "${VIRTUAL_ENV=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Checking packaging tools in venv: ${VIRTUAL_ENV}${COLOR_RESET}"
        echo
    else
        echo
        echo "${COLOR_BLUE}Checking packaging tools for system Python installation: $(which python)${COLOR_RESET}"
        echo
    fi
    if [[ ${AIRFLOW_PIP_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest pip version${COLOR_RESET}"
        echo
        pip install --root-user-action ignore --disable-pip-version-check --upgrade pip
    elif [[ ! ${AIRFLOW_PIP_VERSION} =~ ^[0-9].* ]]; then
        echo
        echo "${COLOR_BLUE}Installing pip version from spec ${AIRFLOW_PIP_VERSION}${COLOR_RESET}"
        echo
        # shellcheck disable=SC2086
        pip install --root-user-action ignore --disable-pip-version-check "pip @ ${AIRFLOW_PIP_VERSION}"
    else
        local installed_pip_version
        installed_pip_version=$(python -c 'from importlib.metadata import version; print(version("pip"))')
        if [[ ${installed_pip_version} != "${AIRFLOW_PIP_VERSION}" ]]; then
            echo
            echo "${COLOR_BLUE}(Re)Installing pip version: ${AIRFLOW_PIP_VERSION}${COLOR_RESET}"
            echo
            pip install --root-user-action ignore --disable-pip-version-check "pip==${AIRFLOW_PIP_VERSION}"
        fi
    fi
    if [[ ${AIRFLOW_UV_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest uv version${COLOR_RESET}"
        echo
        pip install --root-user-action ignore --disable-pip-version-check --upgrade uv
    elif [[ ! ${AIRFLOW_UV_VERSION} =~ ^[0-9].* ]]; then
        echo
        echo "${COLOR_BLUE}Installing uv version from spec ${AIRFLOW_UV_VERSION}${COLOR_RESET}"
        echo
        # shellcheck disable=SC2086
        pip install --root-user-action ignore --disable-pip-version-check "uv @ ${AIRFLOW_UV_VERSION}"
    else
        local installed_uv_version
        installed_uv_version=$(python -c 'from importlib.metadata import version; print(version("uv"))' 2>/dev/null || echo "Not installed yet")
        if [[ ${installed_uv_version} != "${AIRFLOW_UV_VERSION}" ]]; then
            echo
            echo "${COLOR_BLUE}(Re)Installing uv version: ${AIRFLOW_UV_VERSION}${COLOR_RESET}"
            echo
            # shellcheck disable=SC2086
            pip install --root-user-action ignore --disable-pip-version-check "uv==${AIRFLOW_UV_VERSION}"
        fi
    fi
    if  [[ ${AIRFLOW_PREK_VERSION=} == "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing latest prek, uv${COLOR_RESET}"
        echo
        uv tool install prek --with uv
        # make sure that the venv/user in .local exists
        mkdir -p "${HOME}/.local/bin"
    else
        echo
        echo "${COLOR_BLUE}Installing predefined versions of prek, uv:${COLOR_RESET}"
        echo "${COLOR_BLUE}prek(${AIRFLOW_PREK_VERSION}) uv(${AIRFLOW_UV_VERSION})${COLOR_RESET}"
        echo
        uv tool install "prek==${AIRFLOW_PREK_VERSION}" --with "uv==${AIRFLOW_UV_VERSION}"
        # make sure that the venv/user in .local exists
        mkdir -p "${HOME}/.local/bin"
    fi
}

function common::import_trusted_gpg() {
    common::get_colors

    local key=${1:?${COLOR_RED}First argument expects OpenPGP Key ID${COLOR_RESET}}
    local name=${2:?${COLOR_RED}Second argument expected trust storage name${COLOR_RESET}}
    # Please note that not all servers could be used for retrieve keys
    #  sks-keyservers.net: Unmaintained and DNS taken down due to GDPR requests.
    #  keys.openpgp.org: User ID Mandatory, not suitable for APT repositories
    #  keyring.debian.org: Only accept keys in Debian keyring.
    #  pgp.mit.edu: High response time.
    local keyservers=(
        "hkps://keyserver.ubuntu.com"
        "hkps://pgp.surf.nl"
    )

    GNUPGHOME="$(mktemp -d)"
    export GNUPGHOME
    set +e
    for keyserver in $(shuf -e "${keyservers[@]}"); do
        echo "${COLOR_BLUE}Try to receive GPG public key ${key} from ${keyserver}${COLOR_RESET}"
        gpg --keyserver "${keyserver}" --recv-keys "${key}" 2>&1 && break
        echo "${COLOR_YELLOW}Unable to receive GPG public key ${key} from ${keyserver}${COLOR_RESET}"
    done
    set -e
    gpg --export "${key}" > "/etc/apt/trusted.gpg.d/${name}.gpg"
    gpgconf --kill all
    rm -rf "${GNUPGHOME}"
    unset GNUPGHOME
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
    echo "${COLOR_YELLOW}See: https://airflow.apache.org/docs/docker-stack/build.html#adding-new-pypi-packages-individually${COLOR_RESET}"
    echo
    exit 1
fi
exec "${HOME}"/.local/bin/pip "${@}"
EOF

# The content below is automatically copied from scripts/docker/install_from_docker_context_files.sh
COPY <<"EOF" /install_from_docker_context_files.sh

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"


function install_airflow_and_providers_from_docker_context_files(){
    local flags=()
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

    # This is needed to get distribution names for local context distributions
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} --constraint ${HOME}/constraints.txt packaging

    if [[ -n ${AIRFLOW_EXTRAS=} ]]; then
        AIRFLOW_EXTRAS_TO_INSTALL="[${AIRFLOW_EXTRAS}]"
    else
        AIRFLOW_EXTRAS_TO_INSTALL=""
    fi

    # Find apache-airflow distribution in docker-context files
    readarray -t install_airflow_distribution < <(EXTRAS="${AIRFLOW_EXTRAS_TO_INSTALL}" \
        python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found apache-airflow distributions in docker-context-files folder: ${install_airflow_distribution[*]}${COLOR_RESET}"
    echo

    if [[ -z "${install_airflow_distribution[*]}" && ${AIRFLOW_VERSION=} != "" ]]; then
        # When we install only provider distributions from docker-context files, we need to still
        # install airflow from PyPI when AIRFLOW_VERSION is set. This handles the case where
        # pre-release dockerhub image of airflow is built, but we want to install some providers from
        # docker-context files
        install_airflow_distribution=("apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}")
    fi

    # Find apache-airflow-core distribution in docker-context files
    readarray -t install_airflow_core_distribution < <(EXTRAS="" \
        python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?core?[0-9]*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found apache-airflow-core distributions in docker-context-files folder: ${install_airflow_core_distribution[*]}${COLOR_RESET}"
    echo

    if [[ -z "${install_airflow_core_distribution[*]}" && ${AIRFLOW_VERSION=} != "" ]]; then
        # When we install only provider distributions from docker-context files, we need to still
        # install airflow from PyPI when AIRFLOW_VERSION is set. This handles the case where
        # pre-release dockerhub image of airflow is built, but we want to install some providers from
        # docker-context files
        install_airflow_core_distribution=("apache-airflow-core==${AIRFLOW_VERSION}")
    fi

    # Find Provider/TaskSDK/CTL distributions in docker-context files
    readarray -t airflow_distributions< <(python /scripts/docker/get_distribution_specs.py /docker-context-files/apache?airflow?{providers,task?sdk,airflowctl}*.{whl,tar.gz} 2>/dev/null || true)
    echo
    echo "${COLOR_BLUE}Found provider distributions in docker-context-files folder: ${airflow_distributions[*]}${COLOR_RESET}"
    echo

    if [[ ${USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS=} == "true" ]]; then
        local python_version
        python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        local local_constraints_file=/docker-context-files/constraints-"${python_version}"/${AIRFLOW_CONSTRAINTS_MODE}-"${python_version}".txt

        if [[ -f "${local_constraints_file}" ]]; then
            echo
            echo "${COLOR_BLUE}Installing docker-context-files distributions with constraints found in ${local_constraints_file}${COLOR_RESET}"
            echo
            # force reinstall all airflow + provider distributions with constraints found in
            flags=(--upgrade --constraint "${local_constraints_file}")
            echo
            echo "${COLOR_BLUE}Copying ${local_constraints_file} to ${HOME}/constraints.txt${COLOR_RESET}"
            echo
            cp "${local_constraints_file}" "${HOME}/constraints.txt"
        else
            echo
            echo "${COLOR_BLUE}Installing docker-context-files distributions with constraints from GitHub${COLOR_RESET}"
            echo
            flags=(--constraint "${HOME}/constraints.txt")
        fi
    else
        echo
        echo "${COLOR_BLUE}Installing docker-context-files distributions without constraints${COLOR_RESET}"
        echo
        flags=()
    fi

    set -x
    ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} \
        ${ADDITIONAL_PIP_INSTALL_FLAGS} \
        "${flags[@]}" \
        "${install_airflow_distribution[@]}" "${install_airflow_core_distribution[@]}" "${airflow_distributions[@]}"
    set +x
    common::install_packaging_tools
    # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
    # between `pip` and `uv` installations
    # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
    # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
    uv pip check
}

function install_all_other_distributions_from_docker_context_files() {
    echo
    echo "${COLOR_BLUE}Force re-installing all other distributions from local files without dependencies${COLOR_RESET}"
    echo
    local reinstalling_other_distributions
    # shellcheck disable=SC2010
    reinstalling_other_distributions=$(ls /docker-context-files/*.{whl,tar.gz} 2>/dev/null | \
        grep -v apache_airflow | grep -v apache-airflow || true)
    if [[ -n "${reinstalling_other_distributions}" ]]; then
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            --force-reinstall --no-deps --no-index ${reinstalling_other_distributions}
        common::install_packaging_tools
        set +x
    fi
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow_and_providers_from_docker_context_files

install_all_other_distributions_from_docker_context_files
EOF

# The content below is automatically copied from scripts/docker/get_distribution_specs.py
COPY <<"EOF" /get_distribution_specs.py
#!/usr/bin/env python
from __future__ import annotations

import os
import sys
from pathlib import Path

from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    parse_sdist_filename,
    parse_wheel_filename,
)


def print_package_specs(extras: str = "") -> None:
    for package_path in sys.argv[1:]:
        try:
            package, _, _, _ = parse_wheel_filename(Path(package_path).name)
        except InvalidWheelFilename:
            try:
                package, _ = parse_sdist_filename(Path(package_path).name)
            except InvalidSdistFilename:
                print(f"Could not parse package name from {package_path}", file=sys.stderr)
                continue
        print(f"{package}{extras} @ file://{package_path}")


if __name__ == "__main__":
    print_package_specs(extras=os.environ.get("EXTRAS", ""))
EOF


# The content below is automatically copied from scripts/docker/install_airflow_when_building_images.sh
COPY <<"EOF" /install_airflow_when_building_images.sh
#!/usr/bin/env bash

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function install_from_sources() {
    local installation_command_flags
    local fallback_no_constraints_installation
    fallback_no_constraints_installation="false"
    local extra_sync_flags
    extra_sync_flags=""
    if [[ ${VIRTUAL_ENV=} != "" ]]; then
        extra_sync_flags="--active"
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        if [[ ${PACKAGING_TOOL_CMD} == "pip" ]]; then
            set +x
            echo
            echo "${COLOR_RED}We only support uv not pip installation for upgrading dependencies!.${COLOR_RESET}"
            echo
            exit 1
        fi
        set +x
        echo
        echo "${COLOR_BLUE}Attempting to upgrade all packages to highest versions.${COLOR_RESET}"
        echo
        # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
        # (binary lxml embeds its own libxml2, while xmlsec uses system one).
        # See https://bugs.launchpad.net/lxml/+bug/2110068
        set -x
        uv sync --all-packages --resolution highest --group dev --group docs --group docs-gen \
            --group leveldb ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
            --no-python-downloads --no-managed-python
    else
        # We only use uv here but Installing using constraints is not supported with `uv sync`, so we
        # do not use ``uv sync`` because we are not committing and using uv.lock yet.
        # Once we switch to uv.lock (with the workflow that dependabot will update it
        # and constraints will be generated from it, we should be able to simply use ``uv sync`` here)
        # So for now when we are installing with constraints we need to install airflow distributions first and
        # separately each provider that has some extra development dependencies - otherwise `dev`
        # dependency groups will not be installed  because ``uv pip install --editable .`` only installs dev
        # dependencies for the "top level" pyproject.toml
        set +x
        echo
        echo
        echo "${COLOR_BLUE}Installing first airflow distribution with constraints.${COLOR_RESET}"
        echo
        installation_command_flags=" --editable .[${AIRFLOW_EXTRAS}] \
              --editable ./airflow-core --editable ./task-sdk --editable ./airflow-ctl \
              --editable ./kubernetes-tests --editable ./docker-tests --editable ./helm-tests \
              --editable ./task-sdk-integration-tests \
              --editable ./airflow-ctl-tests \
              --editable ./airflow-e2e-tests \
              --editable ./devel-common[all] --editable ./dev \
              --group dev --group docs --group docs-gen --group leveldb"
        local -a projects_with_devel_dependencies
        while IFS= read -r -d '' pyproject_toml_file; do
             project_folder=$(dirname ${pyproject_toml_file})
             echo "${COLOR_BLUE}Checking provider ${project_folder} for development dependencies ${COLOR_RESET}"
             first_line_of_devel_deps=$(grep -A 1 "# Additional devel dependencies (do not remove this line and add extra development dependencies)" ${project_folder}/pyproject.toml | tail -n 1)
             if [[ "$first_line_of_devel_deps" != "]" ]]; then
                projects_with_devel_dependencies+=("${project_folder}")
             fi
             installation_command_flags+=" --editable ${project_folder}"
        done < <(find "providers" -name "pyproject.toml" -print0 | sort -z)
        set -x
        if ! ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags} --constraint "${HOME}/constraints.txt"; then
            fallback_no_constraints_installation="true"
        else
            # For production image, we do not add devel dependencies in prod image
            if [[ ${AIRFLOW_IMAGE_TYPE=} == "ci" ]]; then
                set +x
                echo
                echo "${COLOR_BLUE}Installing all providers with development dependencies.${COLOR_RESET}"
                echo
                for project_folder in "${projects_with_devel_dependencies[@]}"; do
                    echo "${COLOR_BLUE}Installing provider ${project_folder} with development dependencies.${COLOR_RESET}"
                    set -x
                    if ! uv pip install --editable .  --directory "${project_folder}" \
                        --constraint "${HOME}/constraints.txt" --group dev \
                        --no-python-downloads --no-managed-python; then
                            fallback_no_constraints_installation="true"
                    fi
                    set +x
                done
            fi
        fi
        set +x
        if [[ ${fallback_no_constraints_installation} == "true" ]]; then
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies conflicting with constraints.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to no-constraints installation.${COLOR_RESET}"
            echo
            # --no-binary  is needed in order to avoid libxml and xmlsec using different version of libxml2
            # (binary lxml embeds its own libxml2, while xmlsec uses system one).
            # See https://bugs.launchpad.net/lxml/+bug/2110068
            set -x
            uv sync --all-packages --group dev --group docs --group docs-gen \
                --group leveldb ${extra_sync_flags} --no-binary-package lxml --no-binary-package xmlsec \
                --no-python-downloads --no-managed-python
            set +x
        fi
    fi
}

function install_from_external_spec() {
     local installation_command_flags
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "apache-airflow" ]]; then
        installation_command_flags="apache-airflow[${AIRFLOW_EXTRAS}]${AIRFLOW_VERSION_SPECIFICATION}"
    else
        echo
        echo "${COLOR_RED}The '${INSTALLATION_METHOD}' installation method is not supported${COLOR_RESET}"
        echo
        echo "${COLOR_YELLOW}Supported methods are ('.', 'apache-airflow')${COLOR_RESET}"
        echo
        exit 1
    fi
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Remove airflow and all provider distributions installed before potentially${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} freeze | grep apache-airflow | xargs ${PACKAGING_TOOL_CMD} uninstall ${EXTRA_UNINSTALL_FLAGS} 2>/dev/null || true
        set +x
        echo
        echo "${COLOR_BLUE}Installing all packages with highest resolutions. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_TO_HIGHEST_RESOLUTION} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
        set +x
    else
        echo
        echo "${COLOR_BLUE}Installing all packages with constraints. Installation method: ${AIRFLOW_INSTALLATION_METHOD}${COLOR_RESET}"
        echo
        set -x
        if ! ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags} --constraint "${HOME}/constraints.txt"; then
            set +x
            echo
            echo "${COLOR_YELLOW}Likely pyproject.toml has new dependencies conflicting with constraints.${COLOR_RESET}"
            echo
            echo "${COLOR_BLUE}Falling back to no-constraints installation.${COLOR_RESET}"
            echo
            set -x
            ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_IF_NEEDED} ${ADDITIONAL_PIP_INSTALL_FLAGS} ${installation_command_flags}
            set +x
        fi
    fi
}


function install_airflow_when_building_images() {
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
    # Determine the installation_command_flags based on AIRFLOW_INSTALLATION_METHOD method
    if [[ ${AIRFLOW_INSTALLATION_METHOD} == "." ]]; then
        install_from_sources
    else
        install_from_external_spec
    fi
    set +x
    common::install_packaging_tools
    echo
    echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
    echo
    # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
    # between `pip` and `uv` installations
    # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
    # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
    uv pip check
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_airflow_when_building_images
EOF

# The content below is automatically copied from scripts/docker/install_additional_dependencies.sh
COPY <<"EOF" /install_additional_dependencies.sh
#!/usr/bin/env bash
set -euo pipefail

: "${ADDITIONAL_PYTHON_DEPS:?Should be set}"

. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function install_additional_dependencies() {
    if [[ "${UPGRADE_RANDOM_INDICATOR_STRING=}" != "" ]]; then
        echo
        echo "${COLOR_BLUE}Installing additional dependencies while upgrading to newer dependencies${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_TO_HIGHEST_RESOLUTION} \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${ADDITIONAL_PYTHON_DEPS}
        set +x
        common::install_packaging_tools
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
        # between `pip` and `uv` installations
        # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
        # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
        uv pip check
    else
        echo
        echo "${COLOR_BLUE}Installing additional dependencies upgrading only if needed${COLOR_RESET}"
        echo
        set -x
        ${PACKAGING_TOOL_CMD} install ${EXTRA_INSTALL_FLAGS} ${UPGRADE_IF_NEEDED} \
            ${ADDITIONAL_PIP_INSTALL_FLAGS} \
            ${ADDITIONAL_PYTHON_DEPS}
        set +x
        common::install_packaging_tools
        echo
        echo "${COLOR_BLUE}Running 'pip check'${COLOR_RESET}"
        echo
        # Here we should use `pip check` not `uv pip check` to detect any incompatibilities that might happen
        # between `pip` and `uv` installations
        # However, in the current version of `pip` there is a bug that incorrectly detects `pagefind-bin` as unsupported
        # https://github.com/pypa/pip/issues/13709 -> once this is fixed, we should bring `pip check` back.
        uv pip check
    fi
}

common::get_colors
common::get_packaging_tool
common::get_airflow_version_specification
common::get_constraints_location
common::show_packaging_tool_version_and_location

install_additional_dependencies
EOF

# The content below is automatically copied from scripts/docker/create_prod_venv.sh
COPY <<"EOF" /create_prod_venv.sh
#!/usr/bin/env bash
. "$( dirname "${BASH_SOURCE[0]}" )/common.sh"

function create_prod_venv() {
    echo
    echo "${COLOR_BLUE}Removing ${HOME}/.local and re-creating it as virtual environment.${COLOR_RESET}"
    rm -rf ~/.local
    python -m venv ~/.local
    echo "${COLOR_BLUE}The ${HOME}/.local virtualenv created.${COLOR_RESET}"
}

common::get_colors
common::get_packaging_tool
common::show_packaging_tool_version_and_location
create_prod_venv
common::install_packaging_tools
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

    if airflow config get-value core auth_manager | grep -q "FabAuthManager"; then
        airflow users create \
           --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
           --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
           --lastname "${_AIRFLOW_WWW_USER_LASTNAME="Admin"}" \
           --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
           --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
           --password "${local_password}" || true
    else
        echo "Skipping user creation as auth manager different from Fab is used"
    fi
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
        python_major_minor=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        export PYTHONPATH="${AIRFLOW_USER_HOME_DIR}/.local/lib/python${python_major_minor}/site-packages:${PYTHONPATH:-}"
        >&2 echo "The container is run as root user. For security, consider using a regular user account."
    fi
}

function wait_for_airflow_db() {
    # Wait for the command to run successfully to validate the database connection.
    run_check_with_retries "airflow db check"
}

function migrate_db() {
    # Runs airflow db migrate
    airflow db migrate || true
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
        >&2 echo "     https://airflow.apache.org/docs/docker-stack/entrypoint.html"
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
        >&2 echo "     https://airflow.apache.org/docs/docker-stack/entrypoint.html"
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

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] || [[ -n "${_AIRFLOW_DB_MIGRATE=}" ]] ; then
    migrate_db
fi

if [[ -n "${_AIRFLOW_DB_UPGRADE=}" ]] ; then
    >&2 echo "WARNING: Environment variable '_AIRFLOW_DB_UPGRADE' is deprecated please use '_AIRFLOW_DB_MIGRATE' instead"
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
    >&2 echo "         the container starts, so it is only useful for testing and trying out"
    >&2 echo "         of adding dependencies."
    >&2 echo
    pip install --root-user-action ignore ${_PIP_ADDITIONAL_REQUIREMENTS}
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

if [[ "$#" -eq 0 && "${_AIRFLOW_DB_MIGRATE}" == "true" ]]; then
    echo "[INFO] No commands passed and _AIRFLOW_DB_MIGRATE=true. Exiting script with code 0."
    exit 0
fi

exec "airflow" "${@}"
EOF

# The content below is automatically copied from scripts/docker/clean-logs.sh
COPY <<"EOF" /clean-logs.sh
#!/usr/bin/env bash


set -euo pipefail

readonly DIRECTORY="${AIRFLOW_HOME:-/usr/local/airflow}"
readonly RETENTION="${AIRFLOW__LOG_RETENTION_DAYS:-15}"
readonly FREQUENCY="${AIRFLOW__LOG_CLEANUP_FREQUENCY_MINUTES:-15}"

trap "exit" INT TERM

readonly EVERY=$((FREQUENCY*60))

echo "Cleaning logs every $EVERY seconds"

while true; do
  echo "Trimming airflow logs to ${RETENTION} days."
  find "${DIRECTORY}"/logs \
    -type d -name 'lost+found' -prune -o \
    -type f -mtime +"${RETENTION}" -name '*.log' -print0 | \
    xargs -0 rm -f || true

  find "${DIRECTORY}"/logs -type d -empty -delete || true

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
FROM ${BASE_IMAGE} as airflow-build-image

# Nolog bash flag is currently ignored - but you can replace it with
# xtrace - to show commands executed)
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "nolog", "-c"]

ARG BASE_IMAGE

# Make sure noninteractive debian install is used and language variables set
ENV BASE_IMAGE=${BASE_IMAGE} \
    DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8 \
    PIP_CACHE_DIR=/tmp/.cache/pip \
    UV_CACHE_DIR=/tmp/.cache/uv

ARG DEV_APT_DEPS=""
ARG ADDITIONAL_DEV_APT_DEPS=""
ARG DEV_APT_COMMAND=""
ARG ADDITIONAL_DEV_APT_COMMAND=""
ARG ADDITIONAL_DEV_APT_ENV=""
ARG AIRFLOW_PYTHON_VERSION

ENV DEV_APT_DEPS=${DEV_APT_DEPS} \
    ADDITIONAL_DEV_APT_DEPS=${ADDITIONAL_DEV_APT_DEPS} \
    DEV_APT_COMMAND=${DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_COMMAND=${ADDITIONAL_DEV_APT_COMMAND} \
    ADDITIONAL_DEV_APT_ENV=${ADDITIONAL_DEV_APT_ENV} \
    AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION}

ARG PYTHON_LTO


COPY --from=scripts install_os_dependencies.sh /scripts/docker/
RUN PYTHON_LTO=${PYTHON_LTO} bash /scripts/docker/install_os_dependencies.sh dev

# In case system python is installed, setting LD_LIBRARY_PATH prevents any case the system python
# libraries will be accidentally used before the library installed from sources (which is newer and
# python interpreter might break if accidentally the old system libraries are used.
ENV LD_LIBRARY_PATH="/usr/python/lib"

ARG INSTALL_MYSQL_CLIENT="true"
ARG INSTALL_MYSQL_CLIENT_TYPE="mariadb"
ARG INSTALL_MSSQL_CLIENT="true"
ARG INSTALL_POSTGRES_CLIENT="true"

ENV INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT} \
    INSTALL_MYSQL_CLIENT_TYPE=${INSTALL_MYSQL_CLIENT_TYPE} \
    INSTALL_MSSQL_CLIENT=${INSTALL_MSSQL_CLIENT} \
    INSTALL_POSTGRES_CLIENT=${INSTALL_POSTGRES_CLIENT}

COPY --from=scripts common.sh /scripts/docker/

# Only copy mysql/mssql installation scripts for now - so that changing the other
# scripts which are needed much later will not invalidate the docker layer here
COPY --from=scripts install_mysql.sh install_mssql.sh install_postgres.sh /scripts/docker/

RUN bash /scripts/docker/install_mysql.sh dev && \
    bash /scripts/docker/install_mssql.sh dev && \
    bash /scripts/docker/install_postgres.sh dev
ENV PATH=${PATH}:/opt/mssql-tools/bin

# By default we do not install from docker context files but if we decide to install from docker context
# files, we should override those variables to "docker-context-files"
ARG DOCKER_CONTEXT_FILES="Dockerfile"
ARG AIRFLOW_IMAGE_TYPE
ARG AIRFLOW_HOME
ARG AIRFLOW_USER_HOME_DIR
ARG AIRFLOW_UID

RUN adduser --gecos "First Last,RoomNumber,WorkPhone,HomePhone" --disabled-password \
       --quiet "airflow" --uid "${AIRFLOW_UID}" --gid "0" --home "${AIRFLOW_USER_HOME_DIR}" && \
    mkdir -p ${AIRFLOW_HOME} && chown -R "airflow:0" "${AIRFLOW_USER_HOME_DIR}" ${AIRFLOW_HOME}

COPY --chown=${AIRFLOW_UID}:0 ${DOCKER_CONTEXT_FILES} /docker-context-files

USER airflow

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

# By default PIP has progress bar but you can disable it.
ARG PIP_PROGRESS_BAR
# This is airflow version that is put in the label of the image build
ARG AIRFLOW_VERSION
# By default latest released version of airflow is installed (when empty) but this value can be overridden
# and we can install version according to specification (For example ==2.0.2 or <3.0.0).
ARG AIRFLOW_VERSION_SPECIFICATION
# Determines the way airflow is installed. By default we install airflow from PyPI `apache-airflow` package
# But it also can be `.` from local installation or GitHub URL pointing to specific branch or tag
# Of Airflow. Note That for local source installation you need to have local sources of
# Airflow checked out together with the Dockerfile and AIRFLOW_SOURCES_FROM and AIRFLOW_SOURCES_TO
# set to "." and "/opt/airflow" respectively.
ARG AIRFLOW_INSTALLATION_METHOD="apache-airflow"
# By default we do not upgrade to latest dependencies
ARG UPGRADE_RANDOM_INDICATOR_STRING=""
ARG AIRFLOW_SOURCES_FROM
ARG AIRFLOW_SOURCES_TO

ENV AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR}

RUN if [[ -f /docker-context-files/pip.conf ]]; then \
        mkdir -p ${AIRFLOW_USER_HOME_DIR}/.config/pip; \
        cp /docker-context-files/pip.conf "${AIRFLOW_USER_HOME_DIR}/.config/pip/pip.conf"; \
    fi; \
    if [[ -f /docker-context-files/.piprc ]]; then \
        cp /docker-context-files/.piprc "${AIRFLOW_USER_HOME_DIR}/.piprc"; \
    fi

# Additional PIP flags passed to all pip install commands except reinstalling pip itself
ARG ADDITIONAL_PIP_INSTALL_FLAGS=""

ARG AIRFLOW_PIP_VERSION
ARG AIRFLOW_UV_VERSION
ARG AIRFLOW_USE_UV
ARG UV_HTTP_TIMEOUT
ARG INCLUDE_PRE_RELEASE="false"

ENV AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION} \
    AIRFLOW_UV_VERSION=${AIRFLOW_UV_VERSION} \
    UV_HTTP_TIMEOUT=${UV_HTTP_TIMEOUT} \
    AIRFLOW_USE_UV=${AIRFLOW_USE_UV} \
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
    PATH=${AIRFLOW_USER_HOME_DIR}/.local/bin:${PATH} \
    PIP_PROGRESS_BAR=${PIP_PROGRESS_BAR} \
    ADDITIONAL_PIP_INSTALL_FLAGS=${ADDITIONAL_PIP_INSTALL_FLAGS} \
    AIRFLOW_HOME=${AIRFLOW_HOME} \
    AIRFLOW_IMAGE_TYPE=${AIRFLOW_IMAGE_TYPE} \
    AIRFLOW_UID=${AIRFLOW_UID} \
    INCLUDE_PRE_RELEASE=${INCLUDE_PRE_RELEASE} \
    UPGRADE_RANDOM_INDICATOR_STRING=${UPGRADE_RANDOM_INDICATOR_STRING}


# Copy all scripts required for installation - changing any of those should lead to
# rebuilding from here
COPY --from=scripts common.sh install_packaging_tools.sh create_prod_venv.sh /scripts/docker/

# We can set this value to true in case we want to install .whl/.tar.gz packages placed in the
# docker-context-files folder. This can be done for both additional packages you want to install
# as well as Airflow and provider distributions (it will be automatically detected if airflow
# is installed from docker-context files rather than from PyPI)
ARG INSTALL_DISTRIBUTIONS_FROM_CONTEXT="false"

# Normally constraints are not used when context packages are build - because we might have packages
# that are conflicting with Airflow constraints, however there are cases when we want to use constraints
# for example in CI builds when we already have source-package constraints - either from github branch or
# from eager-upgraded constraints by the CI builds
ARG USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS="false"

# In case of Production build image segment we want to pre-install main version of airflow
# dependencies from GitHub so that we do not have to always reinstall it from the scratch.
# The Airflow and providers are uninstalled, only dependencies remain
# the cache is only used when "upgrade to newer dependencies" is not set to automatically
# account for removed dependencies (we do not install them in the first place) and in case
# INSTALL_DISTRIBUTIONS_FROM_CONTEXT is not set (because then caching it from main makes no sense).

# By default PIP installs everything to ~/.local and it's also treated as VIRTUALENV
ENV VIRTUAL_ENV="${AIRFLOW_USER_HOME_DIR}/.local"
ENV PATH="/usr/python/bin:$PATH"
RUN bash /scripts/docker/install_packaging_tools.sh; bash /scripts/docker/create_prod_venv.sh

COPY --chown=airflow:0 ${AIRFLOW_SOURCES_FROM} ${AIRFLOW_SOURCES_TO}

# Add extra python dependencies
ARG ADDITIONAL_PYTHON_DEPS=""


ARG VERSION_SUFFIX=""

ENV ADDITIONAL_PYTHON_DEPS=${ADDITIONAL_PYTHON_DEPS} \
    INSTALL_DISTRIBUTIONS_FROM_CONTEXT=${INSTALL_DISTRIBUTIONS_FROM_CONTEXT} \
    USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS=${USE_CONSTRAINTS_FOR_CONTEXT_DISTRIBUTIONS} \
    VERSION_SUFFIX=${VERSION_SUFFIX}

WORKDIR ${AIRFLOW_HOME}

COPY --from=scripts install_from_docker_context_files.sh install_airflow_when_building_images.sh \
     install_additional_dependencies.sh create_prod_venv.sh get_distribution_specs.py /scripts/docker/

# Useful for creating a cache id based on the underlying architecture, preventing the use of cached python packages from
# an incorrect architecture.
ARG TARGETARCH
# Value to be able to easily change cache id and therefore use a bare new cache
ARG DEPENDENCY_CACHE_EPOCH="11"

# hadolint ignore=SC2086, SC2010, DL3042
RUN --mount=type=cache,id=prod-$TARGETARCH-$DEPENDENCY_CACHE_EPOCH,target=/tmp/.cache/,uid=${AIRFLOW_UID} \
    if [[ ${INSTALL_DISTRIBUTIONS_FROM_CONTEXT} == "true" ]]; then \
        bash /scripts/docker/install_from_docker_context_files.sh; \
    fi; \
    if ! airflow version 2>/dev/null >/dev/null; then \
        bash /scripts/docker/install_airflow_when_building_images.sh; \
    fi; \
    if [[ -n "${ADDITIONAL_PYTHON_DEPS}" ]]; then \
        bash /scripts/docker/install_additional_dependencies.sh; \
    fi; \
    find "${AIRFLOW_USER_HOME_DIR}/.local/" -name '*.pyc' -print0 | xargs -0 rm -f || true ; \
    find "${AIRFLOW_USER_HOME_DIR}/.local/" -type d -name '__pycache__' -print0 | xargs -0 rm -rf || true ; \
    # make sure that all directories and files in .local are also group accessible
    find "${AIRFLOW_USER_HOME_DIR}/.local" -executable ! -type l -print0 | xargs --null chmod g+x; \
    find "${AIRFLOW_USER_HOME_DIR}/.local" ! -type l -print0 | xargs --null chmod g+rw

# In case there is a requirements.txt file in "docker-context-files" it will be installed
# during the build additionally to whatever has been installed so far. It is recommended that
# the requirements.txt contains only dependencies with == version specification
# hadolint ignore=DL3042
RUN --mount=type=cache,id=prod-$TARGETARCH-$DEPENDENCY_CACHE_EPOCH,target=/tmp/.cache/,uid=${AIRFLOW_UID} \
    if [[ -f /docker-context-files/requirements.txt ]]; then \
        pip install -r /docker-context-files/requirements.txt; \
    fi

##############################################################################################
# This is the actual Airflow image - much smaller than the build one. We copy
# installed Airflow and all its dependencies from the build image to make it smaller.
##############################################################################################
FROM ${BASE_IMAGE} as main

# Nolog bash flag is currently ignored - but you can replace it with other flags (for example
# xtrace - to show commands executed)
SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-o", "nolog", "-c"]

ARG AIRFLOW_UID

LABEL org.apache.airflow.distro="debian" \
  org.apache.airflow.module="airflow" \
  org.apache.airflow.component="airflow" \
  org.apache.airflow.image="airflow" \
  org.apache.airflow.uid="${AIRFLOW_UID}"

ARG BASE_IMAGE

# Make sure noninteractive debian install is used and language variables set
ENV BASE_IMAGE=${BASE_IMAGE} \
    DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8 \
    PIP_CACHE_DIR=/tmp/.cache/pip \
    UV_CACHE_DIR=/tmp/.cache/uv

ARG RUNTIME_APT_DEPS=""
ARG ADDITIONAL_RUNTIME_APT_DEPS=""
ARG RUNTIME_APT_COMMAND="echo"
ARG ADDITIONAL_RUNTIME_APT_COMMAND=""
ARG ADDITIONAL_RUNTIME_APT_ENV=""
ARG INSTALL_MYSQL_CLIENT="true"
ARG INSTALL_MYSQL_CLIENT_TYPE="mariadb"
ARG INSTALL_MSSQL_CLIENT="true"
ARG INSTALL_POSTGRES_CLIENT="true"
ARG AIRFLOW_INSTALLATION_METHOD="apache-airflow"

ENV RUNTIME_APT_DEPS=${RUNTIME_APT_DEPS} \
    ADDITIONAL_RUNTIME_APT_DEPS=${ADDITIONAL_RUNTIME_APT_DEPS} \
    RUNTIME_APT_COMMAND=${RUNTIME_APT_COMMAND} \
    ADDITIONAL_RUNTIME_APT_COMMAND=${ADDITIONAL_RUNTIME_APT_COMMAND} \
    INSTALL_MYSQL_CLIENT=${INSTALL_MYSQL_CLIENT} \
    INSTALL_MYSQL_CLIENT_TYPE=${INSTALL_MYSQL_CLIENT_TYPE} \
    INSTALL_MSSQL_CLIENT=${INSTALL_MSSQL_CLIENT} \
    INSTALL_POSTGRES_CLIENT=${INSTALL_POSTGRES_CLIENT} \
    GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm" \
    AIRFLOW_INSTALLATION_METHOD=${AIRFLOW_INSTALLATION_METHOD}

ARG PYTHON_LTO

COPY --from=airflow-build-image "/usr/python/" "/usr/python/"
COPY --from=scripts install_os_dependencies.sh /scripts/docker/
RUN bash /scripts/docker/install_os_dependencies.sh runtime

# Having the variable in final image allows to disable providers manager warnings when
# production image is prepared from sources rather than from package
ARG AIRFLOW_IMAGE_REPOSITORY
ARG AIRFLOW_IMAGE_README_URL
ARG AIRFLOW_USER_HOME_DIR
ARG AIRFLOW_HOME
ARG AIRFLOW_IMAGE_TYPE

# By default PIP installs everything to ~/.local
ENV PATH="${AIRFLOW_USER_HOME_DIR}/.local/bin:/usr/python/bin:${PATH}" \
    VIRTUAL_ENV="${AIRFLOW_USER_HOME_DIR}/.local" \
    AIRFLOW_UID=${AIRFLOW_UID} \
    AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR} \
    AIRFLOW_HOME=${AIRFLOW_HOME} \
    AIRFLOW_IMAGE_TYPE=${AIRFLOW_IMAGE_TYPE}

COPY --from=scripts common.sh /scripts/docker/

# Only copy mysql/mssql installation scripts for now - so that changing the other
# scripts which are needed much later will not invalidate the docker layer here.
COPY --from=scripts install_mysql.sh install_mssql.sh install_postgres.sh /scripts/docker/
# We run scripts with bash here to make sure we can execute the scripts. Changing to +x might have an
# unexpected result - the cache for Dockerfiles might get invalidated in case the host system
# had different umask set and group x bit was not set. In Azure the bit might be not set at all.
# That also protects against AUFS Docker backend problem where changing the executable bit required sync
RUN bash /scripts/docker/install_mysql.sh prod \
    && bash /scripts/docker/install_mssql.sh prod \
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
    && find "${AIRFLOW_HOME}" -executable ! -type l -print0 | xargs --null chmod g+x \
    && find "${AIRFLOW_USER_HOME_DIR}" -executable ! -type l -print0 | xargs --null chmod g+x

ARG AIRFLOW_SOURCES_FROM
ARG AIRFLOW_SOURCES_TO

COPY --from=airflow-build-image --chown=airflow:0 \
     "${AIRFLOW_USER_HOME_DIR}/.local" "${AIRFLOW_USER_HOME_DIR}/.local"
COPY --from=airflow-build-image --chown=airflow:0 \
     "${AIRFLOW_USER_HOME_DIR}/constraints.txt" "${AIRFLOW_USER_HOME_DIR}/constraints.txt"
# In case of editable build also copy airflow sources so that they are available in the main image
# For regular image (non-editable) this will be just Dockerfile copied to /Dockerfile
COPY --from=airflow-build-image --chown=airflow:0 "${AIRFLOW_SOURCES_TO}" "${AIRFLOW_SOURCES_TO}"

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
RUN sed --in-place=.bak "s/secure_path=\"/secure_path=\"$(echo -n ${AIRFLOW_USER_HOME_DIR} | \
        sed 's/\//\\\//g')\/.local\/bin:/" /etc/sudoers

ARG AIRFLOW_VERSION
ARG AIRFLOW_PIP_VERSION
ARG AIRFLOW_UV_VERSION
ARG AIRFLOW_USE_UV
ARG AIRFLOW_PYTHON_VERSION

# See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
# to learn more about the way how signals are handled by the image
# Also set airflow as nice PROMPT message.
ENV DUMB_INIT_SETSID="1" \
    PS1="(airflow)" \
    AIRFLOW_VERSION=${AIRFLOW_VERSION} \
    AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION} \
    AIRFLOW__CORE__LOAD_EXAMPLES="false" \
    PATH="/root/bin:${PATH}" \
    AIRFLOW_PIP_VERSION=${AIRFLOW_PIP_VERSION} \
    AIRFLOW_UV_VERSION=${AIRFLOW_UV_VERSION} \
    AIRFLOW_USE_UV=${AIRFLOW_USE_UV}

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
  org.apache.airflow.python.version="${AIRFLOW_PYTHON_VERSION}" \
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
