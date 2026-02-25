.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Running Airflow with a self-signed certificate
##############################################

Airflow can be configured to run with a self-signed certificate but this
requires a couple of extra steps to enable Workers to trust the API Server.
This guide is based on the :doc:`docker-compose/index` setup.

.. caution::

  This procedure is intended for learning, exploration and development. It is
  not suitable for production use.

Generating the certificate
==========================

The first step is the generation of the certificate. This requires the addition
of ``localhost`` and ``airflow-apiserver`` as Subject Alternative Names so that
the health check and Worker to API Server communications function.

.. code-block:: sh

  export AIRFLOW_CN=example-common-name
  openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -sha256 -days 3650 -nodes \
  -subj "/CN=$AIRFLOW_CN" \
  -addext "subjectAltName=DNS:localhost,DNS:airflow-apiserver"

Where ``example-common-name`` is the common name of your server. Place
``cert.pem`` and ``key.pem`` in the ``config`` folder.

Altering ``docker-compose.yaml``
================================

Add the following two environment variables below and alter the API Server URL
to HTTPS:

.. code-block:: sh

  AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'https://airflow-apiserver:8080/execution/'
  # Added to enable SSL
  AIRFLOW__API__SSL_CERT: '/opt/airflow/config/cert.pem'
  AIRFLOW__API__SSL_KEY: '/opt/airflow/config/key.pem'

Alter the API Server health check to trust the certificate:

.. code-block:: sh

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    healthcheck:
      # Add --cacert to trust certificate
      test: ["CMD", "curl", "--fail", "--cacert", "${AIRFLOW_PROJ_DIR:-.}/config/cert.pem", "https://localhost:8080/api/v2/monitor/health"]

Running Airflow
===============

Now you can start all services:

.. code-block:: sh

  docker compose up

The webserver is available at: ``https://localhost:8080``
