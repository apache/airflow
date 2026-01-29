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


.. _configuration:openlineage:

.. include:: /../../../devel-common/src/sphinx_exts/includes/providers-configurations-ref.rst
.. include:: /../../../devel-common/src/sphinx_exts/includes/sections-and-options.rst


Highlighted configurations
===========================

.. _configuration_transport:openlineage:

Transport setup
----------------

At minimum, one thing that needs to be set up for OpenLineage to function is ``Transport`` - where do you wish for
your events to end up - for example `Marquez <https://marquezproject.ai/>`_.

Transport as JSON string
^^^^^^^^^^^^^^^^^^^^^^^^
The ``transport`` option in OpenLineage section of Airflow configuration is used for that purpose.

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
  `Python client documentation <https://openlineage.io/docs/client/python/configuration#transports>`_.

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

    Detailed description, together with example config files, can be found `in Python client documentation <https://openlineage.io/docs/client/python/configuration#transports>`_.


Configuration precedence
^^^^^^^^^^^^^^^^^^^^^^^^^

Primary, and recommended method of configuring OpenLineage Airflow Provider is Airflow configuration.
As there are multiple possible ways of configuring OpenLineage, it's important to keep in mind the precedence of different configurations.
OpenLineage Airflow Provider looks for the configuration in the following order:

1. Check ``config_path`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__CONFIG_PATH environment variable)
2. Check ``transport`` in ``airflow.cfg`` under ``openlineage`` section (or AIRFLOW__OPENLINEAGE__TRANSPORT environment variable)
3. If all the above options are missing, the OpenLineage Python client used underneath looks for configuration in the order described in `this <https://openlineage.io/docs/client/python/configuration>`_ documentation. Please note that **using Airflow configuration is encouraged** and is the only future proof solution.


.. _configuration_selective_enable:openlineage:

Enabling OpenLineage on Dag/task level
---------------------------------------

One can selectively enable OpenLineage for specific Dags and tasks by using the ``selective_enable`` policy.
To enable this policy, set the ``selective_enable`` option to True in the [openlineage] section of your Airflow configuration file:

.. code-block:: ini

    [openlineage]
    selective_enable = True

``AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE=true


While ``selective_enable`` enables selective control, the ``disabled`` option still has precedence.
If you set ``disabled`` to True in the configuration, OpenLineage will be disabled for all Dags and tasks regardless of the ``selective_enable`` setting.

Once the ``selective_enable`` policy is enabled, you can choose to enable OpenLineage
for individual Dags and tasks using the ``enable_lineage`` and ``disable_lineage`` functions.

1. Enabling Lineage on a Dag:

.. code-block:: python

    from airflow.providers.openlineage.utils.selective_enable import disable_lineage, enable_lineage

    with enable_lineage(Dag(...)):
        # Tasks within this Dag will have lineage tracking enabled
        MyOperator(...)

        AnotherOperator(...)

2. Enabling Lineage on a Task:

While enabling lineage on a Dag implicitly enables it for all tasks within that Dag, you can still selectively disable it for specific tasks:

.. code-block:: python

    from airflow.providers.openlineage.utils.selective_enable import disable_lineage, enable_lineage

    with DAG(...) as dag:
        t1 = MyOperator(...)
        t2 = AnotherOperator(...)

    # Enable lineage for the entire Dag
    enable_lineage(dag)

    # Disable lineage for task t1
    disable_lineage(t1)

Enabling lineage on the Dag level automatically enables it for all tasks within that Dag unless explicitly disabled per task.

Enabling lineage on the task level implicitly enables lineage on its Dag.
This is because each emitting task sends a `ParentRunFacet <https://openlineage.io/docs/spec/facets/run-facets/parent_run>`_,
which requires the Dag-level lineage to be enabled in some OpenLineage backend systems.
Disabling Dag-level lineage while enabling task-level lineage might cause errors or inconsistencies.


.. _configuration_custom_facets:openlineage:

Custom Facets
--------------
To learn more about facets in OpenLineage, please refer to `facet documentation <https://openlineage.io/docs/spec/facets/>`_.

The OpenLineage spec might not contain all the facets you need to write your extractor,
in which case you will have to make your own `custom facets <https://openlineage.io/docs/spec/facets/custom-facets>`_.

You can also inject your own custom facets in the lineage event's run facet using the ``custom_run_facets`` Airflow configuration.

Steps to be taken,

1. Write a function that returns the custom facets. You can write as many custom facet functions as needed.
2. Register the functions using the ``custom_run_facets`` Airflow configuration.

Airflow OpenLineage listener will automatically execute these functions during the lineage event generation and append their return values to the run facet in the lineage event.

Writing a custom facet function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Input arguments:** The function should accept two input arguments: ``TaskInstance`` and ``TaskInstanceState``.
- **Function body:** Perform the logic needed to generate the custom facets. The custom facets must inherit from the ``RunFacet`` for the ``_producer`` and ``_schemaURL`` to be automatically added for the facet.
- **Return value:** The custom facets to be added to the lineage event. Return type should be ``dict[str, RunFacet]`` or ``None``. You may choose to return ``None``, if you do not want to add custom facets for certain criteria.

**Example custom facet function**

.. code-block:: python

    import attrs
    from airflow.models.taskinstance import TaskInstance, TaskInstanceState
    from airflow.providers.common.compat.openlineage.facet import RunFacet


    @attrs.define
    class MyCustomRunFacet(RunFacet):
        """Define a custom facet."""

        name: str
        jobState: str
        uniqueName: str
        displayName: str
        dagId: str
        taskId: str
        cluster: str
        custom_metadata: dict


    def get_my_custom_facet(
        task_instance: TaskInstance, ti_state: TaskInstanceState
    ) -> dict[str, RunFacet] | None:
        operator_name = task_instance.task.operator_name
        custom_metadata = {}
        if operator_name == "BashOperator":
            return None
        if ti_state == TaskInstanceState.FAILED:
            custom_metadata["custom_key_failed"] = "custom_value"
        job_unique_name = f"TEST.{task_instance.dag_id}.{task_instance.task_id}"
        return {
            "additional_run_facet": MyCustomRunFacet(
                name="test-lineage-namespace",
                jobState=task_instance.state,
                uniqueName=job_unique_name,
                displayName=f"{task_instance.dag_id}.{task_instance.task_id}",
                dagId=task_instance.dag_id,
                taskId=task_instance.task_id,
                cluster="TEST",
                custom_metadata=custom_metadata,
            )
        }

Register the custom facet functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``custom_run_facets`` Airflow configuration to register the custom run facet functions by passing
a string of semicolon separated full import path to the functions.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000", "endpoint": "api/v1/lineage"}
    custom_run_facets = full.path.to.get_my_custom_facet;full.path.to.another_custom_facet_function

``AIRFLOW__OPENLINEAGE__CUSTOM_RUN_FACETS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__CUSTOM_RUN_FACETS='full.path.to.get_my_custom_facet;full.path.to.another_custom_facet_function'

.. note::

    - The custom facet functions are executed both at the START and COMPLETE/FAIL of the TaskInstance and added to the corresponding OpenLineage event.
    - When creating conditions on TaskInstance state, you should use second argument provided (``TaskInstanceState``) that will contain the state the task should be in. This may vary from ti.current_state() as the OpenLineage listener may get called before the TaskInstance's state is updated in Airflow database.
    - When path to a single function is registered more than once, it will still be executed only once.
    - When duplicate custom facet keys are returned by multiple functions registered, the result of random function result will be added to the lineage event. Please avoid using duplicate facet keys as it can produce unexpected behaviour.


.. _configuration_backwards_compatibility:openlineage:

Backwards compatibility
------------------------

.. warning::

  Below variables **should not** be used and can be removed in the future. Consider using Airflow configuration (described above) for a future proof solution.

For backwards compatibility with ``openlineage-airflow`` package, some environment variables are still available:

- ``OPENLINEAGE_DISABLED`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLED``.
- ``OPENLINEAGE_CONFIG`` is an equivalent of ``AIRFLOW__OPENLINEAGE__CONFIG_PATH``.
- ``OPENLINEAGE_NAMESPACE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__NAMESPACE``.
- ``OPENLINEAGE_EXTRACTORS`` is an equivalent of setting ``AIRFLOW__OPENLINEAGE__EXTRACTORS``.
- ``OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE`` is an equivalent of ``AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE``.
- ``OPENLINEAGE_URL`` can be used to set up simple http transport. This method has some limitations and may require using other environment variables to achieve desired output. See `docs <https://openlineage.io/docs/client/python/configuration#transports>`_.
