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

Sending metrics over a Unix Domain Socket
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metrics can be sent over a Unix Domain Socket instead of UDP by setting
``statsd_socket_path``:

.. code-block:: ini

    [metrics]
    statsd_on = True
    statsd_socket_path = /var/run/statsd/statsd.sock
    statsd_prefix = airflow

When ``statsd_socket_path`` is set, ``statsd_host``, ``statsd_port``, and
``statsd_ipv6`` are ignored.

The standard StatsD backend uses a stream Unix socket. When the Datadog backend
is enabled, both stream and datagram Unix sockets are supported:

.. code-block:: ini

    [metrics]
    statsd_datadog_enabled = True
    statsd_socket_path = /var/run/datadog/dsd.socket
    statsd_prefix = airflow

For maximum compatibility, configure a plain filesystem path. The Datadog
backend additionally accepts ``unix://``, ``unixgram://``, and
``unixstream://`` URLs.

If you want to use a custom StatsD client instead of the default one provided by Airflow,
the following key must be added to the configuration file alongside the module path of your
custom StatsD client. This module must be available on your :envvar:`PYTHONPATH`.

.. code-block:: ini

    [metrics]
    statsd_custom_client_path = x.y.customclient

When ``statsd_socket_path`` is configured, a custom client must inherit from
``statsd.UnixSocketStatsClient``. Otherwise, it must inherit from
``statsd.StatsClient``.

See :doc:`../modules_management` for details on how Python and Airflow manage modules.

.. note::

    StatsD has no resource concept, so metrics cannot be attributed to the process that
    produced them. When several processes run the same component, such as schedulers in high
    availability, each exports the same series and the server keeps whichever value arrived last.
    Use OpenTelemetry to tell them apart, as described in
    :ref:`identifying-components-and-their-instances`.


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

    To send metrics to an endpoint with a non-default path, set
    ``OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`` to the complete metrics endpoint. This takes precedence
    over ``OTEL_EXPORTER_OTLP_ENDPOINT`` and avoids applying the default ``/v1/metrics`` suffix:

    .. code-block:: bash

        export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="https://metrics.example.com/opentelemetry/api/v1/push"
        export OTEL_EXPORTER_OTLP_METRICS_PROTOCOL="http/protobuf"

    See the OpenTelemetry `exporter protocol specification <https://opentelemetry.io/docs/specs/otel/protocol/exporter/#configuration-options>`_  and
    `SDK environment variable documentation <https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#periodic-exporting-metricreader>`_ for more information.


.. _identifying-components-and-their-instances:

Identifying components and their instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OpenTelemetry labels each metric with the resource that produced it. Two resource attributes
decide how much of a deployment can be told apart:

``service.name``
    Which component reported the metric. It defaults to ``airflow`` for every Airflow process, so
    a scheduler, a triggerer and a worker arrive under one name. Set it per component to attribute
    a metric to the kind of process that produced it.

``service.instance.id``
    Which process of that component reported the metric. It is unset by default, so processes
    running the same component (e.g. 2+ schedulers) are indistinguishable. Set it per process
    to attribute a metric to one of them.

Airflow reads ``service.name`` from ``OTEL_SERVICE_NAME``, and every other resource attribute from
``OTEL_RESOURCE_ATTRIBUTES``:

.. code-block:: bash

    # on one of the schedulers
    export OTEL_SERVICE_NAME="airflow-scheduler"
    export OTEL_RESOURCE_ATTRIBUTES="service.instance.id=$(hostname)"

Processes that share a resource also share a series, and the backend keeps whichever export
arrived last. When several processes run the same component, data are lost instead of aggregated:
each scheduler samples the metadata database on its own loop, so a gauge such as
``pool.open_slots`` reports an arbitrary scheduler's sample rather than a value derived from all
of them.

Once each process is identified, its samples form their own series and can be combined
deliberately — for example, the lowest number of open slots any scheduler observed:

.. code-block:: text

    min by (pool_name) (airflow_pool_open_slots)

How the attributes surface depends on the backend. Those implementing the OpenTelemetry
`Prometheus compatibility <https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/>`_
spec expose them as the ``job`` and ``instance`` labels.


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

Histogram Metrics and Backend Requirements
------------------------------------------

Airflow's timing metrics (``timing()`` / ``timer()``) are emitted as OpenTelemetry
histograms aggregated with
`exponential bucket histograms <https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exponentialhistogram>`_,
so bucket boundaries adapt automatically to the observed range and you do not have to
hand-tune explicit buckets for metrics that span very different scales (milliseconds to
hours).

To ingest these correctly end-to-end, the metrics backend you connect to must support
OpenTelemetry exponential histograms and (for Prometheus) their conversion to native
histograms:

* **OpenTelemetry Collector** — use ``opentelemetry-collector-contrib`` version 0.115.0
  or above. Older versions do not translate OTLP exponential histograms into Prometheus
  native histograms.
* **Prometheus** — native histograms must be enabled explicitly, and how you do that
  depends on the Prometheus version:

  * **2.40 to 3.8** — start Prometheus with the ``--enable-feature=native-histograms``
    flag.
  * **3.8 and above** — set ``scrape_native_histograms: true`` in the scrape
    configuration (this option was added in 3.8, and from 3.9 the feature flag is a
    no-op so the config setting is required):

    .. code-block:: yaml

        global:
            scrape_native_histograms: true

If the backend does not support native histograms, exponential-histogram data points may
be dropped or rendered incorrectly. A reference stack (Collector, Prometheus, and Grafana)
wired up for local development is available via ``breeze start-airflow --integration otel``;
see the contributor docs for details.

Allow/Block Lists
-----------------

If you want to avoid sending all the available metrics, you can configure an allow list or block list
to send or block only certain metrics. Each list is a comma-separated set of regular expressions
matched anywhere in the metric name (anchor a pattern with ``^`` to match a prefix). If both lists
are set, the block list is ignored:

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


Custom Metrics
--------------

You can emit your own metrics from inside a task, plugin, or custom operator through
the same stats client Airflow uses internally. In Airflow 3 the recommended import
path is ``airflow.sdk.observability``:

.. code-block:: python

    from airflow.sdk.observability import stats

    stats.incr("my_service.processed")
    stats.decr("my_service.in_flight")
    stats.gauge("my_service.queue_depth", 42)
    stats.timing("my_service.batch_ms", 1234)

    with stats.timer("my_service.batch"):
        ...

.. versionadded:: 3.3.0
    The module-level ``stats`` functions (``stats.incr()``, ``stats.gauge()``, and so on).

On earlier versions, use the ``Stats`` class instead:
``from airflow.sdk.observability.stats import Stats``, then ``Stats.incr(...)``.

``incr``, ``decr``, ``gauge``, ``timing`` and ``timer`` also accept an optional
``tags`` mapping for dimensional metrics on backends that support them:

.. code-block:: python

    stats.incr("my_service.requests", tags={"endpoint": "checkout"})

``incr`` and ``decr`` also accept ``count`` and ``rate``, and ``gauge`` accepts
``rate`` and ``delta``, following the `StatsD data types
<https://statsd.readthedocs.io/en/stable/types.html#data-types>`__.

.. note::

    Tag support depends on the backend. The classic StatsD protocol has no concept of tags.

    * **OpenTelemetry** (``otel_on``) sends tags as native attributes.
    * **StatsD** (``statsd_on``) drops the ``tags`` mapping by default. To turn tags into labels,
      enable a tagged wire format, either ``statsd_influxdb_enabled = True`` (InfluxDB
      ``name,key=value``) or ``statsd_datadog_enabled = True`` (DogStatsD ``|#key:value``). The
      Prometheus ``statsd_exporter`` reads the tags from either format and turns them into labels.
      These flags only change how tags are written on the wire. You can also embed the values in the
      metric name and map those name segments back to labels with ``statsd_exporter`` mapping rules.

.. note::

    Metric names must be 250 characters or fewer and may only contain the characters
    ``a-z``, ``A-Z``, ``0-9``, ``_``, ``.``, ``-`` and ``/``. An invalid name is logged
    and the metric is not emitted.

.. note::

    These metrics are silently dropped unless a backend is enabled (see `Setup - StatsD`_
    or `Setup - OpenTelemetry`_).

.. note::

    If your custom metrics do not appear, check ``[metrics] metrics_allow_list`` and
    ``[metrics] metrics_block_list`` (see `Allow/Block Lists`_). When
    ``metrics_allow_list`` is set, only metrics matching it are emitted, so a custom
    metric that is not listed is silently dropped.


Other Configuration Options
---------------------------

.. note::

    For a detailed listing of configuration options regarding metrics,
    see the configuration reference documentation - :ref:`config:metrics`.


Metric Descriptions
===================

.. include:: metric_tables.rst
