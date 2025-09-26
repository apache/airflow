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



Traces Configuration
=====================

Airflow can be set up to send traces in `OpenTelemetry <https://opentelemetry.io>`__.

Setup - OpenTelemetry
---------------------

To use OpenTelemetry you must first install the required packages:

.. code-block:: bash

   pip install 'apache-airflow[otel]'

Add the following lines to your configuration file e.g. ``airflow.cfg``

.. code-block:: ini

    [traces]
    otel_on = True
    otel_task_log_event = True

Configure the SDK, by exporting the regular OTel variables to your environment

.. code-block:: ini

    - exporter
      |_ values: 'otlp', 'console'
      |_ default: 'otlp'
    OTEL_TRACES_EXPORTER

    - export protocol
      |_ values: 'grpc', 'http/protobuf'
      |_ default: 'grpc'
    OTEL_EXPORTER_OTLP_PROTOCOL

    - endpoint
      |_ example for grpc protocol: 'http://localhost:4317'
      |_ example for http protocol: 'http://localhost:4318/v1/traces'
      |_ if SSL is enabled, use 'https' instead of 'http'
    OTEL_EXPORTER_OTLP_ENDPOINT
      or
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

    - service name
      |_ default: 'Airflow'
    OTEL_SERVICE_NAME

    - resource attributes
      |_ values: 'key1=value1,key2=value2,...'
      |_ example: 'service.name=my-service,service.version=1.0.0'
    OTEL_RESOURCE_ATTRIBUTES

    - list of headers to apply to all outgoing traces
      |_ values: 'key1=value1,key2=value2,...'
      |_ example: 'api-key=key,other-config-value=value'
    OTEL_EXPORTER_OTLP_HEADERS

Enable Https
-----------------

To establish an HTTPS connection to the OpenTelemetry collector
You need to configure the SSL certificate and key within the OpenTelemetry collector's ``config.yml`` file.

.. code-block:: yaml

   receivers:
     otlp:
       protocols:
         http:
           endpoint: 0.0.0.0:4318
           tls:
             cert_file: "/path/to/cert/cert.crt"
             key_file: "/path/to/key/key.pem"
