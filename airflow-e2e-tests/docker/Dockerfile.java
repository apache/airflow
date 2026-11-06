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

# Extends the standard Airflow image with a headless JRE so JavaCoordinator
# can spawn JVM subprocesses for @task.stub tasks.
#
# Pin Java 17 (rather than default-jre-headless): the Scala Spark example runs
# Apache Spark 3.5.x, which supports Java 8/11/17 but not Java 21. Debian trixie ships
# no Java 17, so it comes from the Eclipse Temurin (Adoptium) repository.
ARG DOCKER_IMAGE
FROM ${DOCKER_IMAGE}

# "Adoptium GPG Key (DEB/RPM Signing Key)", published at
# https://adoptium.net/installation/gpg-verification/
ARG ADOPTIUM_KEY_FINGERPRINT="3B04D753C9050D9A5D343F39843C48A565F8F04B"

SHELL ["/bin/bash", "-o", "pipefail", "-o", "errexit", "-o", "nounset", "-c"]
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL --retry 3 https://packages.adoptium.net/artifactory/api/gpg/key/public \
        -o /etc/apt/keyrings/adoptium.asc \
    && fingerprint="$(gpg --show-keys --with-colons /etc/apt/keyrings/adoptium.asc \
        | awk -F: '/^fpr:/ {print $10; exit}')" \
    && if [ "${fingerprint}" != "${ADOPTIUM_KEY_FINGERPRINT}" ]; then \
        echo "Adoptium key fingerprint '${fingerprint}' != expected '${ADOPTIUM_KEY_FINGERPRINT}'" >&2; \
        exit 1; \
    fi \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb \
$(. /etc/os-release && echo "${VERSION_CODENAME}") main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends temurin-17-jre \
    && rm -rf /var/lib/apt/lists/*
USER airflow
