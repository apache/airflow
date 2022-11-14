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

.. _howto/decorators:snowpark:

``@task.snowpark``
==============

Use the :func:`@task.decorator <airflow.providers.snowflake.decorators.snowpark.snowpark_task>` to execute
`Snowpark <https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html>`__ notebooks in a `Snowflake <https://docs.snowflake.com/en/>`__ database.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this decorator, you must do a few things:

  * Make sure you are using Python 3.8 as it is required for the `snowflake-snowpark-python <https://pypi.org/project/snowflake-snowpark-python/>`__ package
  * Install provider package via **pip**.

    .. code-block:: bash

      pip install 'apache-airflow-providers-snowflake'

    Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

  * :doc:`Setup a Snowflake Connection </connections/snowflake>`.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to specify connection used. If not specified, ``snowflake_default`` will be used.

An example usage of the ``@task.snowpark`` is as follows:

.. exampleinclude:: /../../tests/system/providers/snowflake/example_snowflake_snowpark.py
    :language: python
    :start-after: [START howto_decorator_snowpark]
    :end-before: [END howto_decorator_snowpark]

You can also create stored procedures or user-defined function, but then the procedure should be defined as an internal function for your task function.

.. exampleinclude:: /../../tests/system/providers/snowflake/example_snowflake_snowpark.py
    :language: python
    :start-after: [START howto_decorator_snowpark_sproc]
    :end-before: [END howto_decorator_snowpark_sproc]

.. note::

  Parameters that can be passed onto the decorators will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).
