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
# shellcheck shell=bash
set -euo pipefail

if [[ "$#" != 1 ]]; then
    echo
    echo "ERROR! There should be 'runtime', 'ci' or 'dev' parameter passed as argument.".
    echo
    exit 1
fi

GOLANG_MAJOR_MINOR_VERSION=${GOLANG_MAJOR_MINOR_VERSION:-1.24.4}
TEMURIN_VERSION=${TEMURIN_VERSION:-11}
NODEJS_VERSION=${NODEJS_VERSION:-22.23.1}
# Keep in sync with the "packageManager" pin in ts-sdk/package.json so the version corepack
# resolves for ts-sdk is the one baked into the image.
PNPM_VERSION=${PNPM_VERSION:-10.28.1}
RUSTUP_DEFAULT_TOOLCHAIN=${RUSTUP_DEFAULT_TOOLCHAIN:-stable}
RUSTUP_VERSION=${RUSTUP_VERSION:-1.29.0}
# "hardened" images get Python from a Docker Hardened Image base, "legacy" ones compile it from
# source on top of a plain debian-slim base.
AIRFLOW_IMAGE_FLAVOR=${AIRFLOW_IMAGE_FLAVOR:-hardened}
AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION:-3.13.16}
COSIGN_VERSION=${COSIGN_VERSION:-3.0.5}
if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
    # The Debian 12 hardened images ship Python under /opt/python, the Debian 13 ones install it as
    # Debian packages under /usr - so ask the base image's Python where it lives.
    PYTHON_HOME=${PYTHON_HOME:-$(python3 -c 'import sys; print(sys.base_prefix)')}
    # Read before apt runs: a package depending on python3 can pull another Python version in.
    BASE_PYTHON_MAJOR_MINOR=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
elif [[ "${AIRFLOW_IMAGE_FLAVOR}" == "legacy" ]]; then
    PYTHON_HOME=${PYTHON_HOME:-/usr/python}
else
    echo
    echo "ERROR! AIRFLOW_IMAGE_FLAVOR should be 'hardened' or 'legacy', not '${AIRFLOW_IMAGE_FLAVOR}'."
    echo
    exit 1
fi

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
gzip \
krb5-user \
ldap-utils \
libc6-dev \
libev-dev \
libev4 \
libffi-dev \
libgeos-dev \
libkrb5-dev \
libldap2-dev \
libleveldb-dev \
libleveldb1d \
libsasl2-2 \
libsasl2-dev \
libsasl2-modules \
libssl-dev \
libxmlsec1 \
libxmlsec1-dev \
locales \
openssh-client \
openssl \
pkg-config \
pkgconf \
sasl2-bin \
sqlite3 \
sudo \
tdsodbc \
unixodbc \
unixodbc-dev \
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
    if [[ "${debian_version}" == "bookworm" ]]; then
        debian_version_apt_deps="\
libffi8 \
libldap-2.5-0 \
libssl3 \
netcat-openbsd\
"
    else
        # trixie renamed the libraries that moved to a 64-bit time_t (libssl3 -> libssl3t64) and
        # dropped the soname from the LDAP library package name.
        debian_version_apt_deps="\
libffi8 \
libldap2 \
libssl3t64 \
netcat-openbsd\
"
    fi
    echo
    echo "APPLIED INSTALLATION CONFIGURATION FOR DEBIAN VERSION: ${debian_version}"
    echo
    # libxmlsec1-openssl was added because libxmlsec1 ships no crypto engine of its own - the engines
    # are separate packages - so the "xmlsec" module (pulled in by python3-saml) imported with
    # "libxmlsec1-openssl.so.1: cannot open shared object file" without it.
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
libxmlsec1-openssl \
locales \
openssh-client \
rsync \
sasl2-bin \
sqlite3 \
sudo \
tdsodbc \
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
    curl -fsSL --retry 3 --retry-delay 5 https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    # shellcheck disable=SC1091
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y --no-install-recommends docker-ce-cli
}

function keep_image_conffiles() {
    # The hardened base images ship a modified /etc/debian_version, so any apt run that upgrades
    # base-files stops at dpkg's interactive conffile prompt and fails a non-interactive build. Making
    # dpkg keep the image's copy by default covers every apt run - this build's and those of images
    # extending it - so no install command has to remember the flags.
    mkdir -p /etc/dpkg/dpkg.cfg.d
    printf '%s\n' force-confdef force-confold > /etc/dpkg/dpkg.cfg.d/airflow-keep-conffiles
}

function restore_debian_base_files() {
    # The hardened base images ship a minimal /etc, but Debian maintainer scripts assume the files
    # a stock Debian has: sasl2-bin chowns its run directory to the "sasl" group from base-passwd,
    # and tmux registers its shell with add-shell, which reads /etc/shells. The base-passwd package
    # only ships the reference copies of the account files - update-passwd is what merges them
    # into /etc.
    # libpam-runtime generates the /etc/pam.d/common-* files that the PAM configs already in the
    # image "@include" - without them "adduser --gecos" aborts with a PAM error from chfn.
    # Debian Policy lets a maintainer script rely on any essential package without declaring it, and
    # libgcrypt20 takes that up: its postinst runs a helper with a "#!/bin/dash" shebang. The hardened
    # images ship /bin/sh as a symlink to bash and no dash at all, so configuring the package aborts
    # with exit 127 and takes the whole apt transaction with it. Installing it first restores the
    # assumption, and hands /bin/sh back to dash the way a stock Debian has it.
    apt-get install -y --no-install-recommends dash
    apt-get install -y --no-install-recommends base-passwd libpam-runtime
    update-passwd
    # The Debian 13 hardened images strip /etc/pam.d further: libpam-runtime and passwd are already
    # installed there, so installing them generates nothing, and passwd, chpasswd and chfn abort with
    # "pam_start() failed". Reinstalling passwd brings back its deleted PAM configs, and
    # pam-auth-update - which treats the deleted common-* files as local changes - needs --force.
    apt-get install -y --no-install-recommends --reinstall -o Dpkg::Options::=--force-confmiss passwd
    if [[ ! -e /etc/pam.d/common-auth ]]; then
        pam-auth-update --package --force
    fi
    if [[ ! -e /etc/shells ]]; then
        printf '%s\n' "# /etc/shells: valid login shells" /bin/sh /bin/bash > /etc/shells
    fi
}

function install_debian_dev_dependencies() {
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        keep_image_conffiles
    fi
    apt-get update
    apt-get install -yqq --no-install-recommends apt-utils >/dev/null 2>&1
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        restore_debian_base_files
    fi
    apt-get install -y --no-install-recommends wget curl gnupg2 ca-certificates
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
    # Airflow images have always exposed Python under /usr/python - documentation, volume mounts and
    # user customizations refer to that path - while the hardened base images ship it in /opt/python
    # (Debian 12) or /usr (Debian 13), so keep the historical location working as a symlink.
    if [[ ! -e /usr/python ]]; then
        ln -sv "${PYTHON_HOME}" /usr/python
    fi
    # The hardened base images have no /usr/local tree at all
    mkdir -p /usr/local/bin /usr/local/lib
    # link python binaries to /usr/local/bin and /usr/python/bin with and without 3 suffix
    # Links in /usr/local/bin are needed for tools that expect python to be there
    # Links in /usr/python/bin are needed for tools that are detecting home of python installation including
    # lib/site-packages. The /usr/python/bin should be first in PATH in order to help with the last part.
    for dst in pip3 python3 python3-config; do
        src="$(echo "${dst}" | tr -d 3)"
        if [[ ! -e "/usr/python/bin/${dst}" ]]; then
            continue
        fi
        echo "Linking ${dst} in /usr/local/bin and /usr/python/bin"
        ln -sfv "/usr/python/bin/${dst}" "/usr/local/bin/${dst}"
        for dir in /usr/local/bin /usr/python/bin; do
            if [[ ! -e "${dir}/${src}" ]]; then
                echo "Creating ${src} - > ${dst} link in ${dir}"
                ln -sv "${dir}/${dst}" "${dir}/${src}"
            fi
        done
    done
    # A Python installed under /usr already has its libraries where the dynamic linker looks, and
    # linking all of /usr/lib into /usr/local/lib would only shadow the system libraries.
    if [[ "$(readlink -f "${PYTHON_HOME}")" != "/usr" ]]; then
        for dst in /usr/python/lib/*
        do
            src="/usr/local/lib/$(basename "${dst}")"
            if [[ -e "${src}" ]]; then
                rm -rf "${src}"
            fi
            echo "Linking ${dst} to ${src}"
            ln -sv "${dst}" "${src}"
        done
    fi
    ldconfig
}

function restore_thread_stack_size() {
    # The hardened base images build Python with -DTHREAD_STACK_SIZE=0x100000, so every thread Python
    # starts gets a 1 MiB stack instead of glibc's default (the 8 MiB "ulimit -s" the previous images
    # used). On Python 3.12 and 3.13 the C recursion guard is a fixed depth count sized for the larger
    # stack, so deeply nested input - e.g. a JSON body parsed in a web server worker thread - overflows
    # the stack and kills the process instead of raising RecursionError. A .pth file runs at every
    # interpreter start-up without taking the sitecustomize module name users may already rely on.
    local site_packages
    site_packages="$(/usr/python/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
    echo "Restoring the 8 MiB default thread stack size in ${site_packages}"
    echo "import threading; threading.stack_size(8 * 1024 * 1024)" > "${site_packages}/airflow-thread-stack-size.pth"
}

function compile_python_stdlib() {
    # The hardened base images ship the standard library with no .pyc files at all. Python then misses
    # the bytecode cache on every stdlib import, and in a read-only or non-writable directory it cannot
    # create one - each miss leaves a negative dentry in the kernel, which grows without bound in a
    # long-running container and can exhaust memory on the host. Compiling the standard library here
    # restores what the previously compiled-in-image Python shipped.
    # See https://github.com/apache/airflow/pull/58944 and https://lwn.net/Articles/814535/
    local stdlib
    stdlib="$(/usr/python/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["stdlib"])')"
    echo "Compiling Python standard library in ${stdlib}"
    # compileall exits non-zero when any file fails to compile, and the standard library ships files
    # that are meant not to compile (deliberately broken syntax used by the test suite).
    /usr/python/bin/python -m compileall -q -j "$(nproc)" -o 0 -o 1 -o 2 "${stdlib}" || true
}

function check_no_system_python() {
    # Python - from the hardened base image or compiled from sources - must stay the only Python in the
    # image. A system Python pulled in as a dependency of an apt package shares its shared libraries
    # with ours and leads to errors such as:
    # /usr/python/lib/python3.11/lib-dynload/_ssl.cpython-311-aarch64-linux-gnu.so: undefined symbol: _PyModule_Add
    # Debian names its Python libraries "libpython3.13". The trixie hardened images register their own
    # Python with dpkg as "libpython-3.13", which is the Python we want, so it must not match.
    # Debian 13 hardened images package their Python as "python-3.13" and serve other versions from
    # their apt repository, so a package depending on python3 can also pull in a hardened Python of a
    # different version than the image's own.
    local other_hardened_python=""
    if [[ -n "${BASE_PYTHON_MAJOR_MINOR=}" ]]; then
        other_hardened_python=$(dpkg -l | awk '/^ii  (lib)?python-3\.[0-9]+/ {print $2}' \
            | grep -v -E "python-${BASE_PYTHON_MAJOR_MINOR//./\\.}(-|:|$)" || true)
    fi
    if [[ -n "${other_hardened_python}" ]]; then
        echo
        echo "ERROR! A Python other than the image's ${BASE_PYTHON_MAJOR_MINOR} was installed: ${other_hardened_python//$'\n'/ }"
        echo
        apt-get install -yqq aptitude >/dev/null
        aptitude why "$(echo "${other_hardened_python}" | head -1 | cut -d: -f1)"
        echo
        exit 1
    fi
    if dpkg -l | grep -E '^ii  libpython3\.[0-9]+' >/dev/null; then
        echo
        echo "ERROR! System python is installed by one of the previous steps"
        echo
        echo "Please make sure that no python packages are installed by default. Displaying the reason why libpython is installed:"
        echo
        apt-get install -yqq aptitude >/dev/null
        aptitude why "$(dpkg -l | grep -E '^ii  libpython3\.[0-9]+' | head -1 | awk '{print $2}')"
        echo
        exit 1
    else
        echo
        echo "GOOD! System python is not installed - OK"
        echo
    fi
}

function install_cosign() {
    local arch
    arch="$(dpkg --print-architecture)"
    declare -A cosign_sha256s=(
        # https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign_checksums.txt
        [amd64]="db15cc99e6e4837daabab023742aaddc3841ce57f193d11b7c3e06c8003642b2"
        [arm64]="d098f3168ae4b3aa70b4ca78947329b953272b487727d1722cb3cb098a1a20ab"
    )
    local cosign_sha256="${cosign_sha256s[${arch}]}"
    if [[ -z "${cosign_sha256}" ]]; then
        echo "Unsupported architecture for cosign: ${arch}"
        exit 1
    fi
    curl -fsSL --retry 3 --retry-delay 5 \
        "https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-linux-${arch}" \
        -o /tmp/cosign
    echo "${cosign_sha256}  /tmp/cosign" | sha256sum --check
    chmod +x /tmp/cosign
}

function install_python() {
    # Only the legacy image flavor compiles Python - the hardened flavor gets it from the base image.
    wget --tries=3 --waitretry=5 -O python.tar.xz "https://www.python.org/ftp/python/${AIRFLOW_PYTHON_VERSION%%[a-z]*}/Python-${AIRFLOW_PYTHON_VERSION}.tar.xz"
    local major_minor_version
    major_minor_version="${AIRFLOW_PYTHON_VERSION%.*}"
    echo "Verifying Python ${AIRFLOW_PYTHON_VERSION} (${major_minor_version})"
    # Sigstore verification (PEP 761)
    declare -A sigstore_identities=(
        # https://peps.python.org/pep-0664/#release-manager-and-crew
        [3.11]="pablogsal@python.org"
        # https://peps.python.org/pep-0693/#release-manager-and-crew
        [3.12]="thomas@python.org"
        # https://peps.python.org/pep-0719/#release-manager-and-crew
        [3.13]="thomas@python.org"
        # https://peps.python.org/pep-0745/#release-manager-and-crew
        [3.14]="hugo@python.org"
    )
    declare -A sigstore_issuers=(
        [3.11]="https://accounts.google.com"
        [3.12]="https://accounts.google.com"
        [3.13]="https://accounts.google.com"
        [3.14]="https://github.com/login/oauth"
    )
    wget --tries=3 --waitretry=5 -O python.tar.xz.sigstore \
        "https://www.python.org/ftp/python/${AIRFLOW_PYTHON_VERSION%%[a-z]*}/Python-${AIRFLOW_PYTHON_VERSION}.tar.xz.sigstore"
    install_cosign
    local identity="${sigstore_identities[${major_minor_version}]}"
    local issuer="${sigstore_issuers[${major_minor_version}]}"
    /tmp/cosign verify-blob \
        --bundle python.tar.xz.sigstore \
        --certificate-identity "${identity}" \
        --certificate-oidc-issuer "${issuer}" \
        python.tar.xz
    rm -f python.tar.xz.sigstore /tmp/cosign
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
    local build_log
    build_log=$(mktemp)
    echo "Building Python ${AIRFLOW_PYTHON_VERSION} from source..."
    if ! (
        ./configure --enable-optimizations --prefix=/usr/python/ --with-ensurepip --build="$gnuArch" \
            --enable-loadable-sqlite-extensions --enable-option-checking=fatal \
                --enable-shared --with-lto && \
        make -s -j "$(nproc)" "EXTRA_CFLAGS=${EXTRA_CFLAGS:-}" \
            "LDFLAGS=${LDFLAGS:--Wl},-rpath='\$\$ORIGIN/../lib'" python && \
        make -s -j "$(nproc)" install
    ) > "${build_log}" 2>&1; then
        echo
        echo "ERROR! Python build failed. Build output:"
        echo
        cat "${build_log}"
        rm -f "${build_log}"
        exit 1
    fi
    rm -f "${build_log}"
    cd /
    rm -rf /usr/src/python
    find /usr/python -depth \
      \( \
        \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
        -o \( -type f -a \( -name 'libpython*.a' \) \) \
    \) -exec rm -rf '{}' +
    link_python
}

function install_debian_runtime_dependencies() {
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        keep_image_conffiles
    fi
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        restore_debian_base_files
    fi
    apt-get install -y --no-install-recommends wget curl gnupg2 ca-certificates
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
    check_no_system_python
    link_python
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        restore_thread_stack_size
        compile_python_stdlib
    fi
    rm -rf /var/lib/apt/lists/* /var/log/*
}

function install_golang() {
    curl --retry 3 --retry-delay 5 "https://dl.google.com/go/go${GOLANG_MAJOR_MINOR_VERSION}.linux-$(dpkg --print-architecture).tar.gz" -o "go${GOLANG_MAJOR_MINOR_VERSION}.linux.tar.gz"
    rm -rf /usr/local/go && tar -C /usr/local -xzf go"${GOLANG_MAJOR_MINOR_VERSION}".linux.tar.gz
    rm -f go"${GOLANG_MAJOR_MINOR_VERSION}".linux.tar.gz
}

function install_jdk() {
    # Install Eclipse Temurin JDK from the Adoptium apt repository (https://adoptium.net/installation/linux/).
    apt-get update -qq
    apt-get install -y --no-install-recommends wget gnupg apt-transport-https ca-certificates
    mkdir -p /etc/apt/keyrings
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
        | tee /etc/apt/keyrings/adoptium.asc > /dev/null
    # shellcheck disable=SC1091
    DISTRO_CODENAME=$(. /etc/os-release; echo "${VERSION_CODENAME}")
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] \
https://packages.adoptium.net/artifactory/deb ${DISTRO_CODENAME} main" \
        | tee /etc/apt/sources.list.d/adoptium.list > /dev/null
    apt-get update -qq
    apt-get install -y --no-install-recommends "temurin-${TEMURIN_VERSION}-jdk"
    apt-get clean
    rm -rf /var/lib/apt/lists/*
}

function install_nodejs() {
    local arch
    arch="$(dpkg --print-architecture)"
    declare -A nodejs_targets=(
        [amd64]="linux-x64"
        [arm64]="linux-arm64"
    )
    declare -A nodejs_sha256s=(
        # https://nodejs.org/dist/v${NODEJS_VERSION}/SHASUMS256.txt
        [amd64]="9749e988f437343b7fa832c69ded82a312e41a03116d766797ac14f6f9eee578"
        [arm64]="0294e8b915ab75f92c7513d2fcb830ae06e10684e6c603e99a87dbf8835389c1"
    )
    local target="${nodejs_targets[${arch}]}"
    local nodejs_sha256="${nodejs_sha256s[${arch}]}"
    if [[ -z "${target}" ]]; then
        echo "Unsupported architecture for nodejs: ${arch}"
        exit 1
    fi
    curl --retry 3 --retry-delay 5 \
        "https://nodejs.org/dist/v${NODEJS_VERSION}/node-v${NODEJS_VERSION}-${target}.tar.xz" \
        -o /tmp/nodejs.tar.xz
    echo "${nodejs_sha256}  /tmp/nodejs.tar.xz" | sha256sum --check
    tar -xJf /tmp/nodejs.tar.xz --strip-components=1 -C /usr/local --no-same-owner
    rm -f /tmp/nodejs.tar.xz
    corepack enable --install-directory /usr/local/bin
    # corepack enable only writes shims; prepare downloads and caches the pnpm binary into the
    # image so it is available offline and matches the version ts-sdk pins.
    corepack prepare "pnpm@${PNPM_VERSION}" --activate
}

function install_rustup() {
    local arch
    arch="$(dpkg --print-architecture)"
    declare -A rustup_targets=(
        [amd64]="x86_64-unknown-linux-gnu"
        [arm64]="aarch64-unknown-linux-gnu"
    )
    declare -A rustup_sha256s=(
        # https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/{target}/rustup-init.sha256
        [amd64]="4acc9acc76d5079515b46346a485974457b5a79893cfb01112423c89aeb5aa10"
        [arm64]="9732d6c5e2a098d3521fca8145d826ae0aaa067ef2385ead08e6feac88fa5792"
    )
    local target="${rustup_targets[${arch}]}"
    local rustup_sha256="${rustup_sha256s[${arch}]}"
    if [[ -z "${target}" ]]; then
        echo "Unsupported architecture for rustup: ${arch}"
        exit 1
    fi
    curl --proto '=https' --tlsv1.2 -sSf --retry 3 --retry-delay 5 \
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${target}/rustup-init" \
        -o /tmp/rustup-init
    echo "${rustup_sha256}  /tmp/rustup-init" | sha256sum --check
    chmod +x /tmp/rustup-init
    # Building wheels from source needs only rustc and cargo. The default profile also adds
    # rust-docs, clippy and rustfmt, which add tens of thousands of files to the image.
    /tmp/rustup-init -y --profile minimal --default-toolchain "${RUSTUP_DEFAULT_TOOLCHAIN}"
    rm -f /tmp/rustup-init
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
    check_no_system_python
    if [[ "${AIRFLOW_IMAGE_FLAVOR}" == "hardened" ]]; then
        link_python
        restore_thread_stack_size
        compile_python_stdlib
    else
        install_python
    fi
    install_additional_dev_dependencies
    install_rustup
    if [[ "${INSTALLATION_TYPE}" == "CI" ]]; then
        install_golang
        install_jdk
        install_nodejs
    fi
    install_docker_cli
    apt_clean
fi
