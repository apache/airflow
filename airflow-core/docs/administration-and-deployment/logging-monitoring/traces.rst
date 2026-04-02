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
    otel_host = localhost
    otel_port = 8889
    otel_application = airflow
    otel_ssl_active = False
    otel_task_log_event = True

.. note::

    **The following config keys have been deprecated and will be removed in the future**

        .. code-block:: ini

            [traces]
            otel_host = localhost
            otel_port = 8889
            otel_debugging_on = False
            otel_service = Airflow
            otel_ssl_active = False

    The OpenTelemetry SDK should be configured using standard OpenTelemetry environment variables
    such as ``OTEL_EXPORTER_OTLP_ENDPOINT``, ``OTEL_EXPORTER_OTLP_PROTOCOL``, etc.

    See the OpenTelemetry `exporter protocol specification <https://opentelemetry.io/docs/specs/otel/protocol/exporter/#configuration-options>`_  and
    `SDK environment variable documentation <https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#periodic-exporting-metricreader>`_ for more information.

Adding Custom Spans in Tasks
-----------------------------

DAG authors can instrument their tasks with custom spans using the ``trace`` object from
``airflow.sdk.observability``. This is a thin shim over the standard OpenTelemetry
``opentelemetry.trace`` module, so all standard OpenTelemetry tracing APIs are available.

.. code-block:: python

    from airflow.sdk import task
    from airflow.sdk.observability import trace

    tracer = trace.get_tracer(__name__)


    @task
    def my_task():
        with tracer.start_as_current_span("my_span") as span:
            span.set_attribute("key", "value")
            # ... task logic ...

Custom spans created this way are automatically nested as children of the Airflow-managed
task span when tracing is enabled. When tracing is disabled, the no-op tracer provided by
the OpenTelemetry API is used, so tasks run without any overhead.

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
