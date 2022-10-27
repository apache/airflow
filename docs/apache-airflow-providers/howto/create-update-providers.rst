
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

Community Providers
===================

How-to creating a new community provider
----------------------------------------

This document gathers the necessary steps to create a new community provider and also guidelines for updating
the existing ones. You should be aware that providers may have distinctions that may not be covered in
this guide. The sequence described was designed to meet the most linear flow possible in order to develop a
new provider.

Another recommendation that will help you is to look for a provider that works similar to yours. That way it will
help you to set up tests and other dependencies.

First, you need to set up your local development environment. See `Contribution Quick Start <https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst>`_
if you did not set up your local environment yet. We recommend using ``breeze`` to develop locally. This way you
easily be able to have an environment more similar to the one executed by GitHub CI workflow.

  .. code-block:: bash

      ./breeze

Using the code above you will set up Docker containers. These containers your local code to internal volumes.
In this way, the changes made in your IDE are already applied to the code inside the container and tests can
be carried out quickly.

In this how-to guide our example provider name will be ``<NEW_PROVIDER>``.
When you see this placeholder you must change for your provider name.


Initial Code and Unit Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most likely you have developed a version of the provider using some local customization and now you need to
transfer this code to the Airflow project. Below is described all the initial code structure that
the provider may need. Understand that not all providers will need all the components described in this structure.
If you still have doubts about building your provider, we recommend that you read the initial provider guide and
open a issue on GitHub so the community can help you.

  .. code-block:: bash

      airflow/
      ├── providers/<NEW_PROVIDER>/
      │   ├── __init__.py
      │   ├── example_dags/
      │   │   ├── __init__.py
      │   │   └── example_<NEW_PROVIDER>.py
      │   ├── hooks/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   ├── operators/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   ├── sensors/
      │   │   ├── __init__.py
      │   │   └── <NEW_PROVIDER>.py
      │   └── transfers/
      │       ├── __init__.py
      │       └── <NEW_PROVIDER>.py
      └── tests/providers/<NEW_PROVIDER>/
          ├── __init__.py
          ├── hooks/
          │   ├── __init__.py
          │   └── test_<NEW_PROVIDER>.py
          ├── operators/
          │   ├── __init__.py
          │   ├── test_<NEW_PROVIDER>.py
          │   └── test_<NEW_PROVIDER>_system.py
          ├── sensors/
          │   ├── __init__.py
          │   └── test_<NEW_PROVIDER>.py
          └── transfers/
              ├── __init__.py
              └── test_<NEW_PROVIDER>.py

Considering that you have already transferred your provider's code to the above structure, it will now be necessary
to create unit tests for each component you created. The example below I have already set up an environment using
breeze and I'll run unit tests for my Hook.

  .. code-block:: bash

      root@fafd8d630e46:/opt/airflow# python -m pytest tests/providers/<NEW_PROVIDER>/hook/<NEW_PROVIDER>.py

Integration tests
^^^^^^^^^^^^^^^^^

See `Airflow Integration Tests <https://github.com/apache/airflow/blob/main/TESTING.rst#airflow-integration-tests>`_


Documentation
^^^^^^^^^^^^^

An important part of building a new provider is the documentation.
Some steps for documentation occurs automatically by ``pre-commit`` see `Installing pre-commit guide <https://github.com/apache/airflow/blob/main/CONTRIBUTORS_QUICK_START.rst#pre-commit>`_

  .. code-block:: bash

      airflow/
      ├── INSTALL
      ├── CONTRIBUTING.rst
      ├── setup.py
      ├── docs/
      │   ├── spelling_wordlist.txt
      │   ├── apache-airflow/
      │   │   └── extra-packages-ref.rst
      │   ├── integration-logos/<NEW_PROVIDER>/
      │   │   └── <NEW_PROVIDER>.png
      │   └── apache-airflow-providers-<NEW_PROVIDER>/
      │       ├── index.rst
      │       ├── commits.rst
      │       ├── connections.rst
      │       └── operators/
      │           └── <NEW_PROVIDER>.rst
      └── providers/
          └── <NEW_PROVIDER>/
              ├── provider.yaml
              └── CHANGELOG.rst


Files automatically updated by pre-commit:

- ``INSTALL`` in provider

Files automatically created when the provider is released:

- ``docs/apache-airflow-providers-<NEW_PROVIDER>/commits.rst``
- ``/airflow/providers/<NEW_PROVIDER>/CHANGELOG``

There is a chance that your provider's name is not a common English word.
In this case is necessary to add it to the file ``docs/spelling_wordlist.txt``. This file begin with capitalized words and
lowercase in the second block.

  .. code-block:: bash

    Namespace
    Neo4j
    Nextdoor
    <NEW_PROVIDER> (new line)
    Nones
    NotFound
    Nullable
    ...
    neo4j
    neq
    networkUri
    <NEW_PROVIDER> (new line)
    nginx
    nobr
    nodash

Add your provider dependencies into ``provider.yaml`` under ``dependencies`` key..
If your provider doesn't have any dependency add a empty list.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/connections.rst``:

- add information how to configure connection for your provider.

In the ``docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst``:

- add information how to use the Operator. It's important to add examples and additional information if your Operator has extra-parameters.

  .. code-block:: RST

      .. _howto/operator:NewProviderOperator:

      NewProviderOperator
      ===================

      Use the :class:`~airflow.providers.<NEW_PROVIDER>.operators.NewProviderOperator` to do something
      amazing with Airflow!

      Using the Operator
      ^^^^^^^^^^^^^^^^^^

      The NewProviderOperator requires a ``connection_id`` and this other awesome parameter.
      You can see an example below:

      .. exampleinclude:: /../../airflow/providers/<NEW_PROVIDER>/example_dags/example_<NEW_PROVIDER>.py
          :language: python
          :start-after: [START howto_operator_<NEW_PROVIDER>]
          :end-before: [END howto_operator_<NEW_PROVIDER>]


In the ``docs/apache-airflow-providers-new_provider/index.rst``:

- add all information of the purpose of your provider. It is recommended to check with another provider to help you complete this document as best as possible.

In the ``airflow/providers/<NEW_PROVIDER>/provider.yaml`` add information of your provider:

  .. code-block:: yaml

      package-name: apache-airflow-providers-<NEW_PROVIDER>
      name: <NEW_PROVIDER>
      description: |
        `<NEW_PROVIDER> <https://example.io/>`__
      versions:
        - 1.0.0

      integrations:
        - integration-name: <NEW_PROVIDER>
          external-doc-url: https://www.example.io/
          logo: /integration-logos/<NEW_PROVIDER>/<NEW_PROVIDER>.png
          how-to-guide:
            - /docs/apache-airflow-providers-<NEW_PROVIDER>/operators/<NEW_PROVIDER>.rst
          tags: [service]

      operators:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.operators.<NEW_PROVIDER>

      hooks:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>

      sensors:
        - integration-name: <NEW_PROVIDER>
          python-modules:
            - airflow.providers.<NEW_PROVIDER>.sensors.<NEW_PROVIDER>

      connection-types:
        - hook-class-name: airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>.NewProviderHook
        - connection-type: provider-connection-type

      hook-class-names:  # deprecated in Airflow 2.2.0
        - airflow.providers.<NEW_PROVIDER>.hooks.<NEW_PROVIDER>.NewProviderHook

.. note:: Defining your own connection types

    You only need to add ``connection-types`` in case you have some hooks that have customized UI behavior. However
    it is only supported for Airflow 2.2.0. If your providers are also targeting Airflow below 2.2.0 you should
    provide the deprecated ``hook-class-names`` array. The ``connection-types`` array allows for optimization
    of importing of individual connections and while Airflow 2.2.0 is able to handle both definition, the
    ``connection-types`` is recommended.

    For more information see `Custom connection types <http://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#custom-connection-types>`_


After changing and creating these files you can build the documentation locally. The two commands below will
serve to accomplish this. The first will build your provider's documentation. The second will ensure that the
main Airflow documentation that involves some steps with the providers is also working.

  .. code-block:: bash

    breeze build-docs --package-filter apache-airflow-providers-<NEW_PROVIDER>
    breeze build-docs --package-filter apache-airflow

Optional provider features
--------------------------

  .. note::

    This feature is available in Airflow 2.3+.

Some providers might provide optional features, which are only available when some packages or libraries
are installed. Such features will typically result in ``ImportErrors`` however those import errors
should be silently ignored rather than pollute the logs of Airflow with false warnings. False warnings
are a very bad pattern, as they tend to turn into blind spots, so avoiding false warnings is encouraged.
However until Airflow 2.3, Airflow had no mechanism to selectively ignore "known" ImportErrors. So
Airflow 2.1 and 2.2 silently ignored all ImportErrors coming from providers with actually lead to
ignoring even important import errors - without giving the clue to Airflow users that there is something
missing in provider dependencies.

In Airflow 2.3, new exception :class:`~airflow.exceptions.OptionalProviderFeatureException` has been
introduced and Providers can use the exception to signal that the ImportError (or any other error) should
be ignored by Airflow ProvidersManager. However this Exception is only available in Airflow 2.3 so if
providers would like to remain compatible with 2.2, they should continue throwing
the ImportError exception.

Example code (from Plyvel Hook, part of the Google Provider) explains how such conditional error handling
should be implemented to keep compatibility with 2.2

  .. code-block:: python

    try:
        import plyvel
        from plyvel import DB

        from airflow.exceptions import AirflowException
        from airflow.hooks.base import BaseHook

    except ImportError as e:
        # Plyvel is an optional feature and if imports are missing, it should be silently ignored
        # As of Airflow 2.3  and above the operator can throw OptionalProviderFeatureException
        try:
            from airflow.exceptions import AirflowOptionalProviderFeatureException
        except ImportError:
            # However, in order to keep backwards-compatibility with Airflow 2.1 and 2.2, if the
            # 2.3 exception cannot be imported, the original ImportError should be raised.
            # This try/except can be removed when the provider depends on Airflow >= 2.3.0
            raise e
        raise AirflowOptionalProviderFeatureException(e)


Using Providers with dynamic task mapping
-----------------------------------------

Airflow 2.3 added `Dynamic Task Mapping <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42+Dynamic+Task+Mapping>`_
and it added the possibility of assigning a unique key to each task. Which means that when such dynamically
mapped task wants to retrieve a value from XCom (for example in case an extra link should calculated)
it should always check if the ti_key value passed is not None an only then retrieve the XCom value using
XCom.get_value. This allows to keep backwards compatibility with earlier versions of Airflow.

Typical code to access XCom Value in providers that want to keep backwards compatibility should look similar to
this (note the ``if ti_key is not None:`` condition).

  .. code-block:: python

    def get_link(
        self,
        operator: BaseOperator,
        dttm: datetime | None = None,
        ti_key: "TaskInstanceKey" | None = None,
    ):
        if ti_key is not None:
            job_ids = XCom.get_value(key="job_id", ti_key=ti_key)
        else:
            assert dttm is not None
            job_ids = XCom.get_one(
                key="job_id",
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)


Having sensors return XCOM values
---------------------------------
In Airflow 2.3, sensor operators will be able to return XCOM values. This is achieved by returning an instance of the ``PokeReturnValue`` object at the end of the ``poke()`` method:

  .. code-block:: python

    from airflow.sensors.base import PokeReturnValue


    class SensorWithXcomValue(BaseSensorOperator):
        def poke(self, context: Context) -> Union[bool, PokeReturnValue]:
            # ...
            is_done = ...  # set to true if the sensor should stop poking.
            xcom_value = ...  # return value of the sensor operator to be pushed to XCOM.
            return PokeReturnValue(is_done, xcom_value)


To implement a sensor operator that pushes a XCOM value and supports both version 2.3 and pre-2.3, you need to explicitly push the XCOM value if the version is pre-2.3.

  .. code-block:: python

    try:
        from airflow.sensors.base import PokeReturnValue
    except ImportError:
        PokeReturnValue = None


    class SensorWithXcomValue(BaseSensorOperator):
        def poke(self, context: Context) -> bool:
            # ...
            is_done = ...  # set to true if the sensor should stop poking.
            xcom_value = ...  # return value of the sensor operator to be pushed to XCOM.
            if PokeReturnValue is not None:
                return PokeReturnValue(is_done, xcom_value)
            else:
                if is_done:
                    context["ti"].xcom_push(key="xcom_key", value=xcom_value)
                return is_done


How-to Update a community provider
----------------------------------

See `Provider packages versioning <https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md#provider-packages-versioning>`_
