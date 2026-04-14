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

.. _write-logs-opensearch:

Writing logs to Opensearch
-----------------------------

Only ``apache-airflow-providers-opensearch`` **1.9.0+** is compatible with Airflow 3 for
viewing task logs in the UI. Earlier provider versions can still be installed on Airflow
3.x, but the UI will not be able to render task logs from OpenSearch.

Airflow can be configured to read task logs from Opensearch and optionally write logs to stdout in standard or json format. These logs can later be collected and forwarded to the cluster using tools like fluentd, logstash or others.

Airflow also supports writing logs to OpenSearch directly without requiring additional software like fluentd or logstash. To enable this feature, set ``write_to_os`` and ``json_format`` to ``True`` and ``write_stdout`` to ``False`` in ``airflow.cfg``.

You can choose to have all task logs from workers output to the highest parent level process, instead of the standard file locations. This allows for some additional flexibility in container environments like Kubernetes, where container stdout is already being logged to the host nodes. From there a log shipping tool can be used to forward them along to Opensearch. To use this feature, set the ``write_stdout`` option in ``airflow.cfg``.
You can also choose to have the logs output in a JSON format, using the ``json_format`` option. Airflow uses the standard Python logging module and JSON fields are directly extracted from the LogRecord object. To use this feature, set the ``json_fields`` option in ``airflow.cfg``. Add the fields to the comma-delimited string that you want collected for the logs. These fields are from the LogRecord object in the ``logging`` module. `Documentation on different attributes can be found here <https://docs.python.org/3/library/logging.html#logrecord-objects/>`_.

First, to use the handler, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    remote_logging = True

    [opensearch]
    host = <host>
    port = <port>
    username = <username>
    password = <password>

To output task logs to stdout in JSON format, the following config could be used:

.. code-block:: ini

    [logging]
    remote_logging = True

    [opensearch]
    write_stdout = True
    json_format = True

To output task logs to OpenSearch directly, the following config could be used: (set ``delete_local_logs`` to ``True`` if you do not want to retain a local copy of the task log)

.. code-block:: ini

    [logging]
    remote_logging = True
    delete_local_logs = False

    [opensearch]
    host = <host>
    port = <port>
    username = <username>
    password = <password>
    write_stdout = False
    json_format = True
    write_to_os = True
    target_index = [name of the index to store logs]

.. _opensearch-airflow-3-0-to-3-2-local-settings:

Enabling the OpenSearch task handler on Airflow 3.0.0 – 3.2.0
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

This section is **only about reading task logs back into the Airflow UI**. Tasks running
on workers will write logs as usual (to local files, stdout, or — with appropriate log
shipping — to OpenSearch) regardless of the override below. Without the override on
Airflow 3.0.0 – 3.2.0, logs reach OpenSearch fine but the **UI cannot render them**
because no handler is registered to fetch them back.

The wiring that registers ``OpensearchTaskHandler`` inside the stock
``airflow_local_settings.py`` (the file that builds ``DEFAULT_LOGGING_CONFIG``) only landed
in Airflow **3.2.1**. On Airflow **3.0.0 – 3.2.0** installing the provider is not enough:
to make the UI's log viewer fetch logs from OpenSearch you must ship a custom logging
config that swaps the ``task`` handler. The handler self-registers as the remote-log
reader on construction (via ``REMOTE_TASK_LOG`` on 3.0/3.1 and ``_ActiveLoggingConfig``
on 3.2), so swapping the handler class is the only change required.

Create a module on the Python path — for example ``config/airflow_local_settings.py`` —
and point Airflow at it via ``[logging] logging_config_class``:

.. code-block:: python

    from airflow.config_templates.airflow_local_settings import (
        BASE_LOG_FOLDER,
        DEFAULT_LOGGING_CONFIG,
    )
    from airflow.providers.common.compat.sdk import conf

    OPENSEARCH_HOST = conf.get("opensearch", "host", fallback=None)

    if OPENSEARCH_HOST:
        DEFAULT_LOGGING_CONFIG["handlers"]["task"] = {
            "class": "airflow.providers.opensearch.log.os_task_handler.OpensearchTaskHandler",
            "formatter": "airflow",
            "base_log_folder": str(BASE_LOG_FOLDER),
            "end_of_log_mark": "end_of_log",
            "host": OPENSEARCH_HOST,
            "port": conf.getint("opensearch", "port", fallback=9200),
            "username": conf.get("opensearch", "username"),
            "password": conf.get("opensearch", "password"),
            "write_stdout": conf.getboolean("opensearch", "write_stdout"),
            "json_format": conf.getboolean("opensearch", "json_format"),
            "json_fields": conf.get("opensearch", "json_fields"),
            "host_field": conf.get("opensearch", "host_field", fallback="host"),
            "offset_field": conf.get("opensearch", "offset_field", fallback="offset"),
            "write_to_opensearch": conf.getboolean("opensearch", "write_to_os", fallback=False),
            "target_index": conf.get("opensearch", "target_index", fallback="airflow-logs"),
        }

Then, in ``airflow.cfg``:

.. code-block:: ini

    [logging]
    remote_logging = True
    logging_config_class = config.airflow_local_settings.DEFAULT_LOGGING_CONFIG

On Airflow **3.2.1+** this override is unnecessary — the stock ``airflow_local_settings.py``
already contains an ``elif OPENSEARCH_HOST:`` branch, so configuring the ``[opensearch]``
section in ``airflow.cfg`` is sufficient.

.. note::

    Reading task logs from OpenSearch works on Airflow 3.0.0 – 3.2.0 with the override
    above. The ``write_to_os = True`` direct-write path depends on the remote-log-IO
    plumbing that is only fully wired through the supervisor in Airflow **3.2.1+**. On
    3.0.0 – 3.2.0, prefer shipping logs with fluentd / logstash rather than enabling
    ``write_to_os``.

.. _write-logs-elasticsearch-tls:

Writing logs to Opensearch over TLS
''''''''''''''''''''''''''''''''''''''

To add custom configurations to Opensearch (e.g. turning on ``ssl_verify``, adding a custom self-signed
cert, etc.) use the ``opensearch_configs`` setting in your ``airflow.cfg``

Note that these configurations also apply when you enable writing logs to OpenSearch directly.

.. code-block:: ini

    [logging]
    remote_logging = True

    [opensearch_configs]
    use_ssl = True
    verify_certs = True
    ssl_assert_hostname = True
    ca_certs=/path/to/CA_certs
