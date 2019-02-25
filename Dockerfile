#
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

FROM python:3.6-slim

ENV AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS="all"
ARG PYTHON_DEPS=""
ARG BUILD_DEPS="freetds-dev libkrb5-dev libssl-dev libffi-dev libpq-dev git"
ARG APT_DEPS="libsasl2-dev freetds-bin build-essential default-libmysqlclient-dev apt-utils curl rsync netcat locales"

ENV PATH="$HOME/.npm-packages/bin:$PATH"

RUN set -euxo pipefail \
    && apt update \
    && if [ -n "${APT_DEPS}" ]; then apt install -y $APT_DEPS; fi \
    && curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && apt update \
    && apt install -y nodejs \
    && apt autoremove -yqq --purge \
    && apt clean

COPY . /opt/airflow/

WORKDIR /opt/airflow/airflow/www
RUN npm install \
    && npm run prod

WORKDIR /opt/airflow
RUN set -euxo pipefail \
    && apt update \
    && if [ -n "${BUILD_DEPS}" ]; then apt install -y $BUILD_DEPS; fi \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install --no-cache-dir ${PYTHON_DEPS}; fi \
    && pip install --no-cache-dir --upgrade pip==19.0.1 \
    && pip install --no-cache-dir --no-use-pep517 -e .[$AIRFLOW_DEPS] \
    && apt purge --auto-remove -yqq $BUILD_DEPS \
    && apt autoremove -yqq --purge \
    && apt clean

WORKDIR $AIRFLOW_HOME
RUN mkdir -p $AIRFLOW_HOME
COPY scripts/docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["--help"]
