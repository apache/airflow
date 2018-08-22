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

FROM ubuntu:xenial

# environment variables
ENV DEBIAN_FRONTEND noninteractive

# Kerberos server
RUN apt-get update && apt-get install --no-install-recommends -y \
    ntp \
    python-dev \
    python-pip \
    python-wheel \
    python-setuptools \
    python-pkg-resources \
    krb5-admin-server \
    krb5-kdc

RUN mkdir /app/

# Supervisord
RUN pip install supervisor==3.3.4
RUN mkdir -p /var/log/supervisord/

COPY ./krb-conf/server/kdc.conf /etc/krb5kdc/kdc.conf
COPY ./krb-conf/server/kadm5.acl /etc/krb5kdc/kadm5.acl
COPY ./krb-conf/client/krb5.conf /etc/krb5.conf
COPY ./start_kdc.sh /app/start_kdc.sh

# supervisord
COPY ./supervisord.conf /etc/supervisord.conf

WORKDIR /app

# when container is starting
CMD ["/bin/bash", "/app/start_kdc.sh"]
