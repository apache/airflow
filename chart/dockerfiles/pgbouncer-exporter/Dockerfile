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
ARG ALPINE_VERSION="3.19"
ARG GO_VERSION

FROM golang:${GO_VERSION} AS builder

ARG PGBOUNCER_EXPORTER_VERSION

WORKDIR /usr/src/myapp

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

RUN URL="https://github.com/jbub/pgbouncer_exporter/archive/v${PGBOUNCER_EXPORTER_VERSION}.tar.gz" \
    && curl -L "${URL}" | tar -zx --strip-components 1 \
    && PLATFORM=$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "amd64" )\
    && GOOS=linux GOARCH="${PLATFORM}" CGO_ENABLED=0 go build -v

FROM alpine:${ALPINE_VERSION} AS final

# We want to make sure this one includes latest security fixes.
# "Pin versions in apk add" https://github.com/hadolint/hadolint/wiki/DL3018
# hadolint ignore=DL3018
RUN apk --no-cache add libressl libressl-dev openssl

COPY --from=builder /usr/src/myapp/pgbouncer_exporter /bin

ARG PGBOUNCER_EXPORTER_VERSION
ARG AIRFLOW_PGBOUNCER_EXPORTER_VERSION
ARG GO_VERSION
ARG COMMIT_SHA

LABEL org.apache.airflow.component="pgbouncer-exporter" \
    org.apache.airflow.pgbouncer-exporter.version="${PGBOUNCER_EXPORTER_VERSION}" \
    org.apache.airflow.go.version="${GO_VERSION}" \
    org.apache.airflow.airflow-pgbouncer-exporter.version="${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}" \
    org.apache.airflow.commit-sha="${COMMIT_SHA}" \
    maintainer="Apache Airflow Community <dev@airflow.apache.org>"

HEALTHCHECK CMD ["/bin/pgbouncer_exporter", "health"]

USER nobody

ENTRYPOINT ["/bin/pgbouncer_exporter"]
CMD ["server"]
