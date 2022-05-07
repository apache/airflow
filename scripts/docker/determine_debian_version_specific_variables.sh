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
# shellcheck disable=SC2148
# Determines variables that are specific for different distro versions
function determine_debian_version_specific_variables() {
    local color_red
    color_red=$'\e[31m'
    local color_reset
    color_reset=$'\e[0m'

    local debian_version
    debian_version=$(lsb_release -cs)
    if [[ ${debian_version} == "buster" ]]; then
        export DISTRO_LIBENCHANT="libenchant-dev"
        export DISTRO_LIBGCC="libgcc-8-dev"
        export DISTRO_SELINUX="python-selinux"
        export DISTRO_LIBFFI="libffi6"
        # Note missing man directories on debian-buster
        # https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
        mkdir -pv /usr/share/man/man1
        mkdir -pv /usr/share/man/man7
    elif [[ ${debian_version} == "bullseye" ]]; then
        export DISTRO_LIBENCHANT="libenchant-2-2"
        export DISTRO_LIBGCC="libgcc-10-dev"
        export DISTRO_SELINUX="python3-selinux"
        export DISTRO_LIBFFI="libffi7"
    else
        echo
        echo "${color_red}Unknown distro version ${debian_version}${color_reset}"
        echo
        exit 1
    fi
}

determine_debian_version_specific_variables
