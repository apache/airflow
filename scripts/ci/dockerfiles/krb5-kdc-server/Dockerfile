#
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

FROM debian:buster-slim

ARG AIRFLOW_KRB5KDCSERVER_VERSION
ARG COMMIT_SHA

# environment variables
ENV DEBIAN_FRONTEND noninteractive

# Kerberos server
RUN apt-get update && apt-get install --no-install-recommends -y \
    dumb-init \
    ntp \
    python-dev \
    python-pip \
    python-wheel \
    python-setuptools \
    python-pkg-resources \
    krb5-admin-server \
    krb5-kdc \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /var/log/supervisord/

RUN mkdir /app/

# Supervisord
RUN pip install supervisor==3.3.4

COPY ./krb-conf/server/kdc.conf /etc/krb5kdc/kdc.conf
COPY ./krb-conf/server/kadm5.acl /etc/krb5kdc/kadm5.acl
COPY ./krb-conf/client/krb5.conf /etc/krb5.conf
COPY ./start_kdc.sh /entrypoint
# supervisord
COPY ./supervisord.conf /etc/supervisord.conf

WORKDIR /app

LABEL org.apache.airflow.component="krb5-kdc-server"
LABEL org.apache.airflow.airflow_krb5_kdc_server.version="${AIRFLOW_KRB5KDCSERVER_VERSION}"
LABEL org.apache.airflow.commit_sha="${COMMIT_SHA}"
LABEL maintainer="Apache Airflow Community <dev@airflow.apache.org>"

RUN chmod a+x /entrypoint

EXPOSE 88
# when container is starting
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
