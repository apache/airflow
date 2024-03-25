
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

.. _guides/user:openlineage:

Using OpenLineage integration
-----------------------------

OpenLineage is an open framework for data lineage collection and analysis. At its core is an extensible specification that systems can use to interoperate with lineage metadata.
`Check out OpenLineage docs <https://openlineage.io/docs/>`_.

**No change to user DAG files is required to use OpenLineage**. Basic configuration is needed so that OpenLineage knows where to send events.

Quickstart
==========

.. note::

    OpenLineage Provider offers a diverse range of data transport options (http, kafka, file etc.),
    including the flexibility to create a custom solution. Configuration can be managed through several approaches
    and there is an extensive array of settings available for users to fine-tune and enhance their use of OpenLineage.
    For a comprehensive explanation of these features, please refer to the subsequent sections of this document.

This example is a basic demonstration of OpenLineage setup.

1. Install provider package or add it to ``requirements.txt`` file.

   .. code-block:: ini

      pip install apache-airflow-providers-openlineage

2. Provide a ``Transport`` configuration so that OpenLineage knows where to send the events. Within ``airflow.cfg`` file

   .. code-block:: ini

      [openlineage]
      transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}

   or with ``AIRFLOW__OPENLINEAGE__TRANSPORT`` environment variable

   .. code-block:: ini

      AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}'

3. **That's it !**  OpenLineage events should be sent to the configured backend when DAGs are run.

Usage
=====

When enabled and configured, the integration requires no further action from the user. It will automatically:

- Collect task input / output metadata (source, schema, etc.).
- Collect task run-level metadata (execution time, state, parameters, etc.)
- Collect task job-level metadata (owners, type, description, etc.)
- Collect task-specific metadata (bigquery job id, python source code, etc.) - depending on the Operator

All this data will be sent as OpenLineage events to the configured backend as described in :ref:`job_hierarchy:openlineage`.

Transport setup
===============

Primary, and recommended method of configuring OpenLineage Airflow Provider is Airflow configuration (``airflow.cfg`` file).
All possible configuration options, with example values, can be found in :ref:`the configuration section <configuration:openlineage>`.

At minimum, one thing that needs to be set up in every case is ``Transport`` - where do you wish for
your events to end up - for example `Marquez <https://marquezproject.ai/>`_.

Transport as JSON string
^^^^^^^^^^^^^^^^^^^^^^^^
The ``transport`` option in Airflow configuration is used for that purpose.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}

``AIRFLOW__OPENLINEAGE__TRANSPORT`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}'


If you want to look at OpenLineage events without sending them anywhere, you can set up ``ConsoleTransport`` - the events will end up in task logs.

.. code-block:: ini

    [openlineage]
    transport = {"type": "console"}

.. note::
  For full list of built-in transport types, specific transport's options or instructions on how to implement your custom transport, refer to
  `Python client documentation <https://openlineage.io/docs/client/python#built-in-transport-types>`_.

Transport as config file
^^^^^^^^^^^^^^^^^^^^^^^^
You can also configure OpenLineage ``Transport`` using a YAML file (f.e. ``openlineage.yml``).
Provide the path to the YAML file as ``config_path`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    config_path = '/path/to/openlineage.yml'

``AIRFLOW__OPENLINEAGE__CONFIG_PATH`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__CONFIG_PATH='/path/to/openlineage.yml'

Example content of config YAML file:

.. code-block:: ini

  transport:
    type: http
    url: https://backend:5000
    endpoint: events/receive
    auth:
      type: api_key
      apiKey: f048521b-dfe8-47cd-9c65-0cb07d57591e

.. note::

    Detailed description of that configuration method, together with example config files,
    can be found `in Python client documentation <https://openlineage.io/docs/client/python#built-in-transport-types>`_.

Configuration precedence
^^^^^^^^^^^^^^^^^^^^^^^^

As there are multiple possible ways of configuring OpenLineage, it's important to keep in mind the precedence of different configurations.
OpenLineage Airflow Provider looks for the configuration in the following order:

1. Check ``config_path`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__CONFIG_PATH environment variable)
2. Check ``transport`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__TRANSPORT environment variable)
3. If all the above options are missing, the OpenLineage Python client used underneath looks for configuration in the order described in `this <https://openlineage.io/docs/client/python#configuration>`_ documentation. Please note that **using Airflow configuration is encouraged** and is the only future proof solution.

Backwards compatibility
^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

  Below variables **should not** be used and can be removed in the future. Consider using Airflow configuration (described above) for a future proof solution.

For backwards compatibility with ``openlineage-airflow`` package, some environment variables are still available:

- ``OPENLINEAGE_DISABLED`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLED``.
- ``OPENLINEAGE_CONFIG`` is an equivalent of ``AIRFLOW__OPENLINEAGE__CONFIG_PATH``.
- ``OPENLINEAGE_NAMESPACE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__NAMESPACE``.
- ``OPENLINEAGE_EXTRACTORS`` is an equivalent of setting ``AIRFLOW__OPENLINEAGE__EXTRACTORS``.
- ``OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE``.
- ``OPENLINEAGE_URL`` can be used to set up simple http transport. This method has some limitations and may require using other environment variables to achieve desired output. See `docs <https://openlineage.io/docs/client/python#http-transport-configuration-with-environment-variables>`_.


Additional Options
==================

Namespace
^^^^^^^^^

It's very useful to set up OpenLineage namespace for this particular instance.
That way, if you use multiple OpenLineage producers, events coming from them will be logically separated.
If not set, it's using ``default`` namespace. Provide the name of the namespace as ``namespace`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    namespace = 'my-team-airflow-instance'

``AIRFLOW__OPENLINEAGE__NAMESPACE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__NAMESPACE='my-team-airflow-instance'


Disable
^^^^^^^
You can disable sending OpenLineage events without uninstalling OpenLineage provider by setting
``disabled`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disabled = true

``AIRFLOW__OPENLINEAGE__DISABLED`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLED=true


Disable source code
^^^^^^^^^^^^^^^^^^^

Several Operators (f.e. Python, Bash) will by default include their source code in their OpenLineage events.
To prevent that, set ``disable_source_code`` option to ``true`` in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disable_source_code = true

``AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE=true


Disabled for Operators
^^^^^^^^^^^^^^^^^^^^^^

You can easily exclude some Operators from emitting OpenLineage events by passing a string of semicolon separated
full import paths of Airflow Operators to disable as ``disabled_for_operators`` field in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    disabled_for_operators = 'airflow.operators.bash.BashOperator;airflow.operators.python.PythonOperator'

``AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS='airflow.operators.bash.BashOperator;airflow.operators.python.PythonOperator'

Custom Extractors
^^^^^^^^^^^^^^^^^

If you use :ref:`custom Extractors <custom_extractors:openlineage>` feature, register the extractors by passing
a string of semicolon separated Airflow Operators full import paths to ``extractors`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    extractors = full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass

``AIRFLOW__OPENLINEAGE__EXTRACTORS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__EXTRACTORS='full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass'


Troubleshooting
===============

See :ref:`local_troubleshooting:openlineage` for details on how to troubleshoot OpenLineage locally.


Adding support for custom Operators
===================================

If you want to add OpenLineage coverage for particular Operator, take a look at :ref:`guides/developer:openlineage`


Where can I learn more?
=======================

- Check out `OpenLineage website <https://openlineage.io>`_.
- Visit our `GitHub repository <https://github.com/OpenLineage/OpenLineage>`_.
- Watch multiple `talks <https://openlineage.io/resources#conference-talks>`_ about OpenLineage.

Feedback
========

You can reach out to us on `slack <http://bit.ly/OpenLineageSlack>`_ and leave us feedback!


How to contribute
=================

We welcome your contributions! OpenLineage is an Open Source project under active development, and we'd love your help!

Sounds fun? Check out our `new contributor guide <https://github.com/OpenLineage/OpenLineage/blob/main/CONTRIBUTING.md>`_ to get started.
