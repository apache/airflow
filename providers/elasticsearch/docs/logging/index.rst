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

.. _write-logs-elasticsearch:

Writing logs to Elasticsearch
-----------------------------

Airflow can be configured to read task logs from Elasticsearch and optionally write logs to stdout in standard or json format. These logs can later be collected and forwarded to the Elasticsearch cluster using tools like fluentd, logstash or others.

Airflow also supports writing log to Elasticsearch directly without requiring additional software like filebeat and logstash. To enable this feature, set ``write_to_es`` and ``json_format`` to ``True`` and ``write_stdout`` to ``False`` in ``airflow.cfg``. Please be aware that if you set both ``write_to_es`` and ``delete_local_logs`` in logging section to true, Airflow will delete the local copy of task logs upon successfully writing task logs to ElasticSearch.

You can choose to have all task logs from workers output to the highest parent level process, instead of the standard file locations. This allows for some additional flexibility in container environments like Kubernetes, where container stdout is already being logged to the host nodes. From there a log shipping tool can be used to forward them along to Elasticsearch. To use this feature, set the ``write_stdout`` option in ``airflow.cfg``.
You can also choose to have the logs output in a JSON format, using the ``json_format`` option. Airflow uses the standard Python logging module and JSON fields are directly extracted from the LogRecord object. To use this feature, set the ``json_fields`` option in ``airflow.cfg``. Add the fields to the comma-delimited string that you want collected for the logs. These fields are from the LogRecord object in the ``logging`` module. `Documentation on different attributes can be found here <https://docs.python.org/3/library/logging.html#logrecord-objects/>`_.

First, to use the handler, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>

To output task logs to stdout in JSON format, the following config could be used:

.. code-block:: ini

    [logging]
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>
    write_stdout = True
    json_format = True

To output task logs to ElasticSearch, the following config could be used: (set ``delete_local_logs`` to true if you don't want retain a local copy of task log)

.. code-block:: ini

    [logging]
    remote_logging = True
    delete_local_logs = False

    [elasticsearch]
    host = <host>:<port>
    write_stdout = False
    json_format = True
    write_to_es = True
    target_index = [name of the index to store logs]

.. _elasticsearch-airflow-3-0-to-3-1-local-settings:

Enabling the Elasticsearch task handler on Airflow 3.0.0 – 3.1.7
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

This section is **only about reading task logs back into the Airflow UI**. Tasks running
on workers will write logs as usual (to local files, stdout, or — with appropriate log
shipping — to Elasticsearch) regardless of the override below. Without the override on
Airflow 3.0.0 – 3.1.7, logs reach Elasticsearch fine but the **UI cannot render them**
because no handler is registered to fetch them back.

The wiring that registers ``ElasticsearchTaskHandler`` inside the stock
``airflow_local_settings.py`` (the file that builds ``DEFAULT_LOGGING_CONFIG``) landed in
Airflow **3.2.0** (`apache/airflow#62121
<https://github.com/apache/airflow/pull/62121>`_) and was backported to Airflow **3.1.8**
(`apache/airflow#62940 <https://github.com/apache/airflow/pull/62940>`_). On Airflow
**3.0.0 – 3.1.7** installing the provider is not enough: to make the UI's log viewer
fetch logs from Elasticsearch you must ship a custom logging config that swaps the
``task`` handler **and** sets ``REMOTE_TASK_LOG`` at module scope. The override requires
``apache-airflow-providers-elasticsearch`` **6.5.0+** (`apache/airflow#53821
<https://github.com/apache/airflow/pull/53821>`_), which is where
``ElasticsearchRemoteLogIO`` was introduced.

Create a module on the Python path — for example ``config/airflow_local_settings.py`` —
and point Airflow at it via ``[logging] logging_config_class``:

.. code-block:: python

    from airflow.config_templates.airflow_local_settings import (
        BASE_LOG_FOLDER,
        DEFAULT_LOGGING_CONFIG,
    )
    from airflow.providers.common.compat.sdk import conf
    from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchRemoteLogIO

    ELASTICSEARCH_HOST = conf.get("elasticsearch", "host", fallback=None)

    REMOTE_TASK_LOG = None
    DEFAULT_REMOTE_CONN_ID = None

    if ELASTICSEARCH_HOST:
        DEFAULT_LOGGING_CONFIG["handlers"]["task"] = {
            "class": "airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler",
            "formatter": "airflow",
            "base_log_folder": str(BASE_LOG_FOLDER),
            "end_of_log_mark": conf.get("elasticsearch", "end_of_log_mark", fallback="end_of_log"),
            "host": ELASTICSEARCH_HOST,
            "frontend": conf.get("elasticsearch", "frontend", fallback=""),
            "write_stdout": conf.getboolean("elasticsearch", "write_stdout"),
            "write_to_es": conf.getboolean("elasticsearch", "write_to_es", fallback=False),
            "json_format": conf.getboolean("elasticsearch", "json_format"),
            "json_fields": conf.get("elasticsearch", "json_fields"),
            "host_field": conf.get("elasticsearch", "host_field", fallback="host"),
            "offset_field": conf.get("elasticsearch", "offset_field", fallback="offset"),
        }
        REMOTE_TASK_LOG = ElasticsearchRemoteLogIO(
            host=ELASTICSEARCH_HOST,
            target_index=conf.get("elasticsearch", "target_index", fallback="airflow-logs"),
            write_stdout=conf.getboolean("elasticsearch", "write_stdout"),
            write_to_es=conf.getboolean("elasticsearch", "write_to_es", fallback=False),
            offset_field=conf.get("elasticsearch", "offset_field", fallback="offset"),
            host_field=conf.get("elasticsearch", "host_field", fallback="host"),
            base_log_folder=str(BASE_LOG_FOLDER),
            delete_local_copy=conf.getboolean("logging", "delete_local_logs"),
            json_format=conf.getboolean("elasticsearch", "json_format"),
            log_id_template=conf.get(
                "elasticsearch",
                "log_id_template",
                fallback="{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
            ),
        )

Then, in ``airflow.cfg``:

.. code-block:: ini

    [logging]
    remote_logging = True
    logging_config_class = config.airflow_local_settings.DEFAULT_LOGGING_CONFIG

.. note::

   Earlier versions of this guide relied on ``ElasticsearchTaskHandler`` self-registering
   ``REMOTE_TASK_LOG`` from inside ``__init__`` when ``dictConfig`` instantiated it.
   That implicit registration is now deprecated (``AirflowProviderDeprecationWarning``)
   and will be removed in a future provider release; define ``REMOTE_TASK_LOG`` at
   module scope as shown above. See :ref:`write-logs-advanced` for the full
   ``logging_config_class`` contract.

On Airflow **3.1.8+** or **3.2.0+** this override is unnecessary — the stock
``airflow_local_settings.py`` already contains an ``elif ELASTICSEARCH_HOST:`` branch, so
configuring the ``[elasticsearch]`` section in ``airflow.cfg`` is sufficient.

.. _write-logs-elasticsearch-tls:

Writing logs to Elasticsearch over TLS
''''''''''''''''''''''''''''''''''''''

To add custom configurations to ElasticSearch (e.g. turning on ``ssl_verify``, adding a custom self-signed
cert, etc.) use the ``elasticsearch_configs`` setting in your ``airflow.cfg``

Note that these configurations also apply when you enable writing logs to ElasticSearch

.. code-block:: ini

    [logging]
    remote_logging = True

    [elasticsearch_configs]
    verify_certs=True
    ca_certs=/path/to/CA_certs

Additionally, in the ``elasticsearch_configs`` section, you can pass any parameters supported by the `Elasticsearch Python client <https://elasticsearch-py.readthedocs.io/en/stable/api/elasticsearch.html>`_. These parameters will be passed directly into the ``elasticsearch.Elasticsearch(**kwargs)`` client. For example:

.. code-block:: ini

    [elasticsearch_configs]
    http_compress = True
    ca_certs = /root/ca.pem
    api_key = "SOMEAPIKEY"
    verify_certs = True

Pinning the ``compatible-with`` content-negotiation level
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Since provider 6.5.1, the Elasticsearch dependency is ``elasticsearch>=8.10,<10``,
which means a default install resolves to an ``elasticsearch>=9`` Python client.
That client unconditionally negotiates ``compatible-with=9`` on every request,
which Elasticsearch 8.x servers reject with HTTP 400
``media_type_header_exception``. Both the task log writer and the
``ElasticsearchSQLHook`` / ``ElasticsearchPythonHook`` are affected.

If you need to keep a single Airflow image compatible with an
``elasticsearch<9`` server, set ``[elasticsearch] es_compat_with`` to the server
major version. The provider then rewrites the client transport so every outbound
request carries ``Accept`` / ``Content-Type:
application/vnd.elasticsearch+json; compatible-with=<major>`` (and the matching
``+x-ndjson`` form for bulk requests):

.. code-block:: ini

    [elasticsearch]
    es_compat_with = 8

Only a positive integer major version is accepted (``"7"``, ``"8"``, ``"9"``);
any other value (e.g. ``"v8"``, ``"8.0"``) fails fast with an
``AirflowConfigException`` at client construction time so the misconfiguration
is obvious in the worker startup log instead of producing a per-request 400
storm.

.. note::

   The fix is installed at the **transport layer** (a wrapper around
   ``client.transport.perform_request``) and therefore overrides the
   per-API-method ``Accept`` / ``Content-Type`` headers that elasticsearch-py
   negotiates from its own client major. Constructor-level ``headers=`` on
   ``Elasticsearch.__init__`` and the ``elasticsearch_configs`` section do
   **not** work for this purpose — elasticsearch-py re-applies its own
   ``compatible-with=<client_major>`` headers right before the request goes
   out, after any constructor headers.

When the option is unset the client behaves as before (negotiating its own
major version).

.. _elasticsearch-document-schema:

Expected Elasticsearch document schema
'''''''''''''''''''''''''''''''''''''''

When using an external log shipper (Fluent Bit, Fluentd, Logstash, etc.) to index task logs into
Elasticsearch, each document must contain certain fields for Airflow to retrieve and render logs correctly.

**Required fields**

``log_id``
    Identifies which task instance a log line belongs to. The value must match the
    ``[elasticsearch] log_id_template`` setting.

``offset``
    A monotonically increasing integer that defines the ordering of log lines within a single task attempt.
    The field name can be customized with the ``offset_field`` handler parameter.

``event``
    The log message text. This is the primary field used by Airflow 3 to display log content.
    If ``event`` is absent, Airflow falls back to the ``message`` field automatically, which preserves
    compatibility with Airflow 2.x log formats.

**Optional fields**

The following fields are recognized and displayed by the Airflow UI when present:

.. list-table::
   :header-rows: 1
   :widths: 20 60 20

   * - Field
     - Description
     - Notes
   * - ``timestamp``
     - ISO-8601 timestamp of the log line.
     - ``@timestamp`` is mapped to ``timestamp`` automatically.
   * - ``level``
     - Log level (e.g. ``INFO``, ``WARNING``, ``ERROR``).
     - ``levelname`` is mapped to ``level`` automatically.
   * - ``chan``
     - The logging channel name.
     -
   * - ``logger``
     - The logger name that produced the record.
     -
   * - ``error_detail``
     - Structured traceback written by the Airflow 3 task supervisor.
     - Empty values are ignored.
   * - ``message``
     - Log message text (Airflow 2.x convention).
     - Used as fallback when ``event`` is absent.
   * - ``host``
     - The hostname of the worker that produced the log. Used to group log lines by source.
     - Field name can be customized with the ``host_field`` handler parameter.

**Field mappings**

Airflow applies the following automatic mappings when reading documents, so your log shipper can use
either form:

- ``@timestamp`` → ``timestamp``
- ``levelname`` → ``level``
- ``message`` → ``event`` (only when ``event`` is not present)

**Minimal document example**

The smallest document that will render correctly in the Airflow UI:

.. code-block:: json

    {
      "log_id": "my_dag-my_task-manual__2025-06-01T00:00:00+00:00-0-1",
      "offset": 1,
      "event": "Task execution started"
    }

**Full document example**

A document using all recognized fields:

.. code-block:: json

    {
      "log_id": "my_dag-my_task-manual__2025-06-01T00:00:00+00:00-0-1",
      "offset": 1,
      "event": "Task execution started",
      "timestamp": "2025-06-01T00:00:01.123Z",
      "level": "INFO",
      "chan": "task",
      "logger": "airflow.task",
      "host": "worker-01"
    }

.. _log-link-elasticsearch:

Elasticsearch External Link
'''''''''''''''''''''''''''

A user can configure Airflow to show a link to an Elasticsearch log viewing system (e.g. Kibana).

To enable it, ``airflow.cfg`` must be configured as in the example below. Note the required ``{log_id}`` in the URL, when constructing the external link, Airflow replaces this parameter with the same ``log_id_template`` used for writing logs (see `Writing logs to Elasticsearch`_).

.. code-block:: ini

    [elasticsearch]
    # Qualified URL for an elasticsearch frontend (like Kibana) with a template argument for log_id
    # Code will construct log_id using the log_id template from the argument above.
    # NOTE: scheme will default to https if one is not provided
    frontend = <host_port>/{log_id}

Changes to ``[elasticsearch] log_id_template``
''''''''''''''''''''''''''''''''''''''''''''''

If you ever need to make changes to ``[elasticsearch] log_id_template``, Airflow 2.3.0+ is able to keep track of
old values so your existing task runs logs can still be fetched. Once you are on Airflow 2.3.0+, in general, you
can just change ``log_id_template`` at will and Airflow will keep track of the changes.

However, when you are upgrading to 2.3.0+, Airflow may not be able to properly save your previous ``log_id_template``.
If after upgrading you find your task logs are no longer accessible, try adding a row in the ``log_template`` table with ``id=0``
containing your previous ``log_id_template``. For example, if you used the defaults in 2.2.5:

.. code-block:: sql

    INSERT INTO log_template (id, filename, elasticsearch_id, created_at) VALUES (0, '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log', '{dag_id}-{task_id}-{execution_date}-{try_number}', NOW());
