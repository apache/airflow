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
==================

Use the :func:`@task.snowpark <airflow.providers.snowflake.decorators.snowpark.snowpark_task>` to run
`Snowpark Python <https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html>`__ code in a `Snowflake <https://docs.snowflake.com/en/>`__ database.

.. warning::

    - Snowpark does not support Python 3.12 yet.
    - Currently, this decorator does not support `Snowpark pandas API <https://docs.snowflake.com/en/developer-guide/snowpark/python/pandas-on-snowflake>`__ because conflicting pandas version is used in Airflow.
      Consider using Snowpark pandas API with other Snowpark decorators or operators.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this decorator, you must do a few things:

  * Install provider package via **pip**.

    .. code-block:: bash

      pip install 'apache-airflow-providers-snowflake'

    Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

  * :doc:`Setup a Snowflake Connection </connections/snowflake>`.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to specify connection used. If not specified, ``snowflake_default`` will be used.

An example usage of the ``@task.snowpark`` is as follows:

.. exampleinclude:: /../../tests/system/providers/snowflake/example_snowpark_decorator.py
    :language: python
    :start-after: [START howto_decorator_snowpark]
    :end-before: [END howto_decorator_snowpark]


As the example demonstrates, there are two ways to use the Snowpark session object in your Python function:

  * Pass the Snowpark session object to the function as a keyword argument named ``session``. The Snowpark session will be automatically injected into the function, allowing you to use it as you normally would.

  * Use `get_active_session <https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/1.3.0/api/snowflake.snowpark.context.get_active_session>`__
    function from Snowpark to retrieve the Snowpark session object inside the function.

.. note::

  Parameters that can be passed onto the decorators will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).
