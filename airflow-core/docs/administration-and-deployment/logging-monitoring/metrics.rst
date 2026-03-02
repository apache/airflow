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



Metrics Configuration
=====================

Airflow can be set up to send metrics to `StatsD <https://github.com/etsy/statsd>`__
or `OpenTelemetry <https://opentelemetry.io/>`__.

Setup - StatsD
--------------

To use StatsD you must first install the required packages:

.. code-block:: bash

   pip install 'apache-airflow[statsd]'

then add the following lines to your configuration file e.g. ``airflow.cfg``

.. code-block:: ini

    [metrics]
    statsd_on = True
    statsd_host = localhost
    statsd_port = 8125
    statsd_prefix = airflow

If you want to use a custom StatsD client instead of the default one provided by Airflow,
the following key must be added to the configuration file alongside the module path of your
custom StatsD client. This module must be available on your :envvar:`PYTHONPATH`.

.. code-block:: ini

    [metrics]
    statsd_custom_client_path = x.y.customclient

See :doc:`../modules_management` for details on how Python and Airflow manage modules.


Setup - OpenTelemetry
---------------------

To use OpenTelemetry you must first install the required packages:

.. code-block:: bash

   pip install 'apache-airflow[otel]'

An OpenTelemetry `Collector <https://opentelemetry.io/docs/concepts/components/#collector>`_ (or compatible service) is required for connectivity to a metrics backend.
Add the Collector details to your configuration file e.g. ``airflow.cfg``

.. code-block:: ini

    [metrics]
    otel_on = True
    otel_host = localhost
    otel_port = 8889
    otel_prefix = airflow
    otel_interval_milliseconds = 30000  # The interval between exports, defaults to 60000
    otel_service = Airflow
    otel_ssl_active = False

.. note::

    **The following config keys have been deprecated and will be removed in the future**

        .. code-block:: ini

            [metrics]
            otel_host = localhost
            otel_port = 8889
            otel_interval_milliseconds = 30000
            otel_debugging_on = False
            otel_service = Airflow
            otel_ssl_active = False

    The OpenTelemetry SDK should be configured using standard OpenTelemetry environment variables
    such as ``OTEL_EXPORTER_OTLP_ENDPOINT``, ``OTEL_EXPORTER_OTLP_PROTOCOL``, etc.

    See the OpenTelemetry `exporter protocol specification <https://opentelemetry.io/docs/specs/otel/protocol/exporter/#configuration-options>`_  and
    `SDK environment variable documentation <https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#periodic-exporting-metricreader>`_ for more information.


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

Allow/Block Lists
-----------------

If you want to avoid sending all the available metrics, you can configure an allow list or block list
of prefixes to send or block only the metrics that start with the elements of the list:

.. code-block:: ini

    [metrics]
    metrics_allow_list = scheduler,executor,dagrun,pool,triggerer,celery

.. code-block:: ini

    [metrics]
    metrics_block_list = scheduler,executor,dagrun,pool,triggerer,celery


Rename Metrics
--------------

If you want to redirect metrics to a different name, you can configure the ``stat_name_handler`` option
in ``[metrics]`` section.  It should point to a function that validates the stat name, applies changes
to the stat name if necessary, and returns the transformed stat name. The function may look as follows:

.. code-block:: python

    def my_custom_stat_name_handler(stat_name: str) -> str:
        return stat_name.lower()[:32]


Other Configuration Options
---------------------------

.. note::

    For a detailed listing of configuration options regarding metrics,
    see the configuration reference documentation - :ref:`config:metrics`.


Metric Descriptions
===================

.. include:: metric_tables.rst
