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

You can choose to have all task logs from workers output to the highest parent level process, instead of the standard file locations. This allows for some additional flexibility in container environments like Kubernetes, where container stdout is already being logged to the host nodes. From there a log shipping tool can be used to forward them along to Elasticsearch. To use this feature, set the ``write_stdout`` option in ``airflow.cfg``.
You can also choose to have the logs output in a JSON format, using the ``json_format`` option. Airflow uses the standard Python logging module and JSON fields are directly extracted from the LogRecord object. To use this feature, set the ``json_fields`` option in ``airflow.cfg``. Add the fields to the comma-delimited string that you want collected for the logs. These fields are from the LogRecord object in the ``logging`` module. `Documentation on different attributes can be found here <https://docs.python.org/3/library/logging.html#logrecord-objects/>`_.

First, to use the handler, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>
    log_id_template = {dag_id}-{task_id}-{run_id}-{try_number}
    end_of_log_mark = end_of_log
    write_stdout =
    json_fields =

To output task logs to stdout in JSON format, the following config could be used:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch]
    host = <host>:<port>
    log_id_template = {dag_id}-{task_id}-{run_id}-{try_number}
    end_of_log_mark = end_of_log
    write_stdout = True
    json_format = True
    json_fields = asctime, filename, lineno, levelname, message

.. _write-logs-elasticsearch-tls:

Writing logs to Elasticsearch over TLS
''''''''''''''''''''''''''''''''''''''

To add custom configurations to ElasticSearch (e.g. turning on ``ssl_verify``, adding a custom self-signed
cert, etc.) use the ``elasticsearch_configs`` setting in your ``airflow.cfg``

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
    # Users must supply an Airflow connection id that provides access to the storage
    # location. If remote_logging is set to true, see UPDATING.md for additional
    # configuration requirements.
    remote_logging = True

    [elasticsearch_configs]
    use_ssl=True
    verify_certs=True
    ca_certs=/path/to/CA_certs

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
