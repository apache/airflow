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
ARG BASE_AIRFLOW_IMAGE
FROM ${BASE_AIRFLOW_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER 0

ENV GO_INSTALL_DIR=/usr/local/go

# Install Go
RUN DOWNLOAD_URL="https://dl.google.com/go/go1.16.4.linux-amd64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/go.linux-amd64.tar.gz" \
    && mkdir -p "${GO_INSTALL_DIR}" \
    && tar xzf "${TMP_DIR}/go.linux-amd64.tar.gz" -C "${GO_INSTALL_DIR}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"

ENV GOROOT=/usr/local/go
ENV PATH="$GOROOT/bin:$PATH"

USER ${AIRFLOW_UID}
