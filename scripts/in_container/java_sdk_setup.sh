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



# 1. Check Java
check_java() {
    local java_bin="/files/openjdk/bin/java"
    local version_output

    # First check if the locally installed OpenJDK exists and works.
    if [ -x "$java_bin" ] && version_output=$("$java_bin" -version 2>&1); then
        echo "Found existing OpenJDK at $java_bin. OK."
        return
    fi

    # On macOS, /usr/bin/java exists as a shim even without a JDK installed,
    # so we must test with `java -version` directly.
    if ! version_output=$(java -version 2>&1); then
        echo "Java is not installed."
        install_java
        return
    fi

    local java_version
    java_version=$(echo "$version_output" | head -n1 | sed -E 's/.*"([0-9]+)(\.[0-9]+)*.*/\1/')

    if ! [[ "$java_version" =~ ^[0-9]+$ ]]; then
        echo "Could not determine Java version."
        install_java
        return
    fi

    if [ "$java_version" -ge 11 ]; then
        echo "Java $java_version detected. OK."
    else
        echo "Java version $java_version found, but >= 11 is required."
        install_java
    fi
}


install_java() {
    echo "Installing OpenJDK 11 in Breeze..."

    curl -L -o /files/openjdk-11-aarch64.tar.gz \
        https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.30+7/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.30_7.tar.gz

    rm -rf /files/openjdk && mkdir -p /files/openjdk && \
        tar -xzf /files/openjdk-11-aarch64.tar.gz --strip-components=1 -C /files/openjdk

    /files/openjdk/bin/java -version
    echo ""
}

check_java
# Install Java Provider
pip install -e /opt/airflow/providers/languages/java/
