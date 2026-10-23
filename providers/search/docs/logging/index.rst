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

.. _write-logs-search:

Writing logs to Elasticsearch or OpenSearch
-------------------------------------------

The provider stores each task log line as one document in Elasticsearch (8.x or later) or
OpenSearch (2.x or later) and reads them back for the Airflow UI. The same configuration works
for both engines.

There are two ways for the lines to reach the cluster:

* Airflow indexes them itself when the task finishes. Set ``write_to_search = True``.
* A log shipper such as Fluent Bit or Logstash collects them. Set ``write_stdout = True`` to
  print them to the worker's stdout as JSON lines, with ``log_id`` and ``offset`` already set.

Configuration
'''''''''''''

On Airflow 3.3.0 or later, set the ``search://`` scheme in ``[logging] remote_base_log_folder``:

.. code-block:: ini

    [logging]
    remote_logging = True
    remote_base_log_folder = search://
    delete_local_logs = False

    [search]
    host = https://search.example.com:9200
    username = airflow
    password = <password>
    write_to_search = True
    target_index = airflow-logs
    index_patterns = airflow-logs

Elastic Cloud deployments can set ``cloud_id`` instead of ``host``, and Elasticsearch users can
authenticate with ``api_key`` instead of a username and password. See
:doc:`../configurations-ref` for every option.

Airflow 3.0 to 3.2
''''''''''''''''''

Before 3.3.0, Airflow does not pick the remote log handler from the URL scheme. Point
``[logging] logging_config_class`` at a module that sets ``REMOTE_TASK_LOG``:

.. code-block:: python

    # config/search_log_settings.py
    from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
    from airflow.providers.search.log.search_task_handler import SearchRemoteLogIO

    REMOTE_TASK_LOG = SearchRemoteLogIO.from_config()

.. code-block:: ini

    [logging]
    remote_logging = True
    logging_config_class = config.search_log_settings.DEFAULT_LOGGING_CONFIG

Linking to Kibana or OpenSearch Dashboards
''''''''''''''''''''''''''''''''''''''''''

To show a link to the log in your search UI next to each task try, also replace the ``task``
handler with :class:`~airflow.providers.search.log.search_task_handler.SearchTaskHandler`:

.. code-block:: python

    from airflow.config_templates.airflow_local_settings import BASE_LOG_FOLDER, DEFAULT_LOGGING_CONFIG

    DEFAULT_LOGGING_CONFIG["handlers"]["task"] = {
        "class": "airflow.providers.search.log.search_task_handler.SearchTaskHandler",
        "formatter": "airflow",
        "base_log_folder": BASE_LOG_FOLDER,
        "frontend": "https://kibana.example.com/app/discover#/?_a=(query:(query:'log_id:\"{log_id}\"'))",
    }

``{log_id}`` in ``frontend`` is replaced with the URL-encoded ``log_id`` of the task try.

Index mapping
'''''''''''''

Reads filter on ``log_id`` with a ``match_phrase`` query and sort on the ``offset`` field. Map
``offset`` as ``long`` so the sort is numeric. Dynamic mapping does this when Airflow creates the
index by writing to it.
