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
ARG ALPINE_VERSION="3.12"

FROM alpine:${ALPINE_VERSION} AS builder

ARG PGBOUNCER_EXPORTER_VERSION
ARG AIRFLOW_PGBOUNCER_EXPORTER_VERSION
ARG GO_VERSION
ARG COMMIT_SHA

LABEL org.apache.airflow.component="pgbouncer-exporter"
LABEL org.apache.airflow.pgbouncer_exporter.version="${PGBOUNCER_EXPORTER_VERSION}"
LABEL org.apache.airflow.go.version="${GO_VERSION}"
LABEL org.apache.airflow.airflow_pgbouncer_exporter.version="${AIRFLOW_PGBOUNCER_EXPORTER_VERSION}"
LABEL org.apache.airflow.commit_sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

# hadolint ignore=DL3018
RUN apk --no-cache add libressl libressl-dev

COPY pgbouncer_exporter-${PGBOUNCER_EXPORTER_VERSION}/pgbouncer_exporter /bin

HEALTHCHECK CMD ["/bin/pgbouncer_exporter", "health"]

ENTRYPOINT ["/bin/pgbouncer_exporter"]
CMD ["server"]
