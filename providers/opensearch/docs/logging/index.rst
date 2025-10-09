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

*Added in provider version 1.5.0*
Available only with Airflow>=3.0

Airflow can be configured to read task logs from Opensearch and optionally write logs to stdout in standard or json format. These logs can later be collected and forwarded to the cluster using tools like fluentd, logstash or others.

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

.. _write-logs-elasticsearch-tls:

Writing logs to Opensearch over TLS
''''''''''''''''''''''''''''''''''''''

To add custom configurations to Opensearch (e.g. turning on ``ssl_verify``, adding a custom self-signed
cert, etc.) use the ``opensearch_configs`` setting in your ``airflow.cfg``

.. code-block:: ini

    [logging]
    remote_logging = True

    [opensearch_configs]
    use_ssl = True
    verify_certs = True
    ssl_assert_hostname = True
    ca_certs=/path/to/CA_certs
