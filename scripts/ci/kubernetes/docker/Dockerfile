#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

FROM ubuntu:16.04

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python-dev \
        python-pip \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        inetutils-telnet \
        bind9utils

RUN pip install -U setuptools && \
    pip install -U pip

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN pip install kubernetes && \
    pip install cryptography && \
    pip install psycopg2==2.7.3.1  # I had issues with older versions of psycopg2, just a warning

# install airflow
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip install /tmp/airflow.tar.gz

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
