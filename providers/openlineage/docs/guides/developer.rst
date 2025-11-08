
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


.. _guides/developer:openlineage:


Implementing OpenLineage in Operators
-------------------------------------

OpenLineage makes adding lineage to your data pipelines easy through support of direct modification of Airflow Operators.
When it's possible to modify the Operator adding lineage extraction can be as easy as adding a single method to it.
See :ref:`openlineage_methods:openlineage` for more details.

There might be some Operators that you can not modify (f.e. third party providers), but still want the lineage to be extracted from them.
To handle this situation, OpenLineage allows you to provide custom Extractor for any Operator.
See :ref:`custom_extractors:openlineage` for more details.

.. _extraction_precedence:openlineage:

Extraction precedence
=====================

As there are multiple possible ways of implementing OpenLineage support for the Operator,
it's important to keep in mind the order in which OpenLineage looks for lineage data:

1. **Extractor** - check if there is a custom Extractor specified for Operator class name. Any custom Extractor registered by the user will take precedence over default Extractors defined in Airflow Provider source code (f.e. BashExtractor).
2. **OpenLineage methods** - if there is no Extractor explicitly specified for Operator class name, DefaultExtractor is used, that looks for OpenLineage methods in Operator.
3. **Inlets and Outlets** - if there are no OpenLineage methods defined in the Operator, inlets and outlets are checked.

If all the above options are missing, no lineage data is extracted from the Operator. You will still receive OpenLineage events
enriched with things like general Airflow facets, proper event time and type, but the inputs/outputs will be empty
and Operator-specific facets will be missing.

.. _openlineage_methods:openlineage:

OpenLineage methods
===================

This approach is recommended when dealing with your own Operators, where you can directly implement OpenLineage methods.
When dealing with Operators that you can not modify (f.e. third party providers), but still want the lineage to be extracted from them, see :ref:`custom_extractors:openlineage`.

OpenLineage defines a few methods for implementation in Operators. Those are referred to as OpenLineage methods.

.. code-block:: python

  def get_openlineage_facets_on_start() -> OperatorLineage: ...


  def get_openlineage_facets_on_complete(ti: TaskInstance) -> OperatorLineage: ...


  def get_openlineage_facets_on_failure(ti: TaskInstance) -> OperatorLineage: ...

OpenLineage methods get called respectively when task instance changes state to:

- RUNNING -> ``get_openlineage_facets_on_start()``
- SUCCESS -> ``get_openlineage_facets_on_complete()``
- FAILED -> ``get_openlineage_facets_on_failure()``

At least one of the following methods must be implemented: ``get_openlineage_facets_on_start()`` or ``get_openlineage_facets_on_complete()``.
For more details on what methods are called when others are missing, see :ref:`ol-methods-best-practices:openlineage`.

Instead of returning complete OpenLineage event, the provider defines ``OperatorLineage`` structure to be returned by Operators:

.. code-block:: python

  @define
  class OperatorLineage:
      inputs: list[Dataset] = Factory(list)
      outputs: list[Dataset] = Factory(list)
      run_facets: dict[str, RunFacet] = Factory(dict)
      job_facets: dict[str, BaseFacet] = Factory(dict)

OpenLineage integration itself takes care to enrich it with things like general Airflow facets, proper event time and type, creating proper OpenLineage RunEvent.

.. _ol-methods-best-practices:openlineage:

How to properly implement OpenLineage methods?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are a couple of things worth noting when implementing OpenLineage in Operators.

First, do not import OpenLineage-related objects on top-level, but in OL method itself.
This allows users to use your provider even if they do not have OpenLineage provider installed.

Second important point is to make sure your provider returns OpenLineage-compliant dataset names.
It allows OpenLineage consumers to properly match information about datasets gathered from different sources.
The naming convention is described in the `OpenLineage naming docs <https://openlineage.io/docs/spec/naming>`__.

Third, OpenLineage implementation should not waste time of users that do not use it.
This means not doing heavy processing or network calls in the ``execute`` method that aren't used by it.
Better option is to save relevant information in Operator attributes - and then use it
in OpenLineage method.
Good example is ``BigQueryExecuteQueryOperator``. It saves ``job_ids`` of queries that were executed.
``get_openlineage_facets_on_complete`` then can call BigQuery API, asking for lineage of those tables, and transform it to OpenLineage format.

Fourth, it's not necessary to implement all the methods. If all the datasets are known before ``execute`` is
called, and there's no relevant runtime data, there might be no point to implementing ``get_openlineage_facets_on_complete``
- the ``get_openlineage_facets_on_start`` method can provide all the data. And in reverse, if everything is unknown
before execute, there might be no point in writing ``_on_start`` method.
Similarly, if there's no relevant failure data - or the failure conditions are unknown,
implementing ``get_openlineage_facets_on_failure`` is probably not worth it. In general:
if there's no ``on_failure`` method, the ``on_complete`` method gets called instead.
If there's no ``on_failure`` and ``on_complete`` method, the ``on_start`` gets called instead (both at the task start and task completion).
If there's no ``on_start`` method the lineage information will not be included in START event, and the ``on_complete`` method will be called upon task completion.

How to test OpenLineage methods?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unit testing OpenLineage integration in Operators is very similar to testing Operators itself.
Objective of those tests is making sure the ``get_openlineage_*`` methods return proper ``OperatorLineage``
data structure with relevant fields filled. It's recommended to mock any external calls.
Authors of tests need to remember the condition of calling different OL methods is different.
``get_openlineage_facets_on_start`` is called before ``execute``, and as such, must not depend on values
that are set there.

See :ref:`troubleshooting:openlineage` for details on how to troubleshoot OpenLineage locally.

There is no existing framework for system testing OpenLineage integration, but the easiest way it can be achieved is
by comparing emitted events (f.e. with ``FileTransport``) against expected ones.
Objective of author of OpenLineage system test is to provide expected dictionary of event keys.
Event keys identify event send from particular Operator and method: they have structure ``<dag_id>.<task_id>.event.<event_type>``;
it's always possible to identify particular event send from particular task this way.
The provided event structure does not have to contain all the fields that are in the resulting event.
Only the fields provided by test author can be compared; this allows to check only for fields particular
test cares about. It also allows to skip fields that are (semi) randomly generated, like ``runId`` or ``eventTime``,
or just always the same in context of OpenLineage in Airflow, like ``producer``.

Example
^^^^^^^

Here's example of properly implemented ``get_openlineage_facets_on_complete`` method, for `GcsToGcsOperator <https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/transfers/gcs_to_gcs.py>`_.
As there is some processing made in ``execute`` method, and there is no relevant failure data, implementing this single method is enough.

.. code-block::  python

    def get_openlineage_facets_on_complete(self, task_instance):
        """
        Implementing _on_complete because execute method does preprocessing on internals.
        This means we won't have to normalize self.source_object and self.source_objects,
        destination bucket and so on.
        """
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        return OperatorLineage(
            inputs=[
                Dataset(namespace=f"gs://{self.source_bucket}", name=source)
                for source in sorted(self.resolved_source_objects)
            ],
            outputs=[
                Dataset(namespace=f"gs://{self.destination_bucket}", name=target)
                for target in sorted(self.resolved_target_objects)
            ],
        )

For more examples of implemented OpenLineage methods, check out the source code of :ref:`supported_classes:openlineage`.

.. _custom_extractors:openlineage:

Custom Extractors
=================

This approach is recommended when dealing with Operators that you can not modify (f.e. third party providers), but still want the lineage to be extracted from them.
If you want to extract lineage from your own Operators, you may prefer directly implementing OpenLineage methods as described in :ref:`openlineage_methods:openlineage`.

This approach works by detecting which Airflow Operators your Dag is using, and extracting lineage data from them using corresponding Extractors class.

Interface
^^^^^^^^^

Custom Extractors have to derive from :class:`BaseExtractor <airflow.providers.openlineage.extractors.base.BaseExtractor>`
and implement at least two methods: ``_execute_extraction`` and ``get_operator_classnames``.

BaseExtractor defines three more methods: ``extract``, ``extract_on_complete`` and ``extract_on_failure``,
that are called and used to provide actual lineage data.
The difference is that ``extract`` is called before Operator's ``execute`` method, while ``extract_on_complete`` and
``extract_on_failure`` are called after - when the task either succeeds or fails, respectively.
By default, ``extract`` calls ``_execute_extraction`` method implemented in custom Extractor.
When the task succeeds, ``extract_on_complete`` is called and if not overwritten, by default, it delegates to ``extract``.
When the task fails, ``extract_on_failure`` is called and if not overwritten, by default, it delegates to ``extract_on_complete``.
If you want to provide some additional information available after the task execution, you can
override ``extract_on_complete`` and ``extract_on_failure`` methods.
This is useful for extracting data the Operator sets as it's own properties during or after execution.
Good example is an SQL operator that sets ``query_ids`` after execution.

The ``get_operator_classnames`` is a classmethod that is used to provide list of Operators that your Extractor can get lineage from.

For example:

.. code-block::  python

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
      return ['CustomPostgresOperator']

If the name of the Operator matches one of the names on the list, the Extractor will be instantiated - with Operator
provided in the Extractor's ``self.operator`` property - and both ``extract`` and ``extract_on_complete``/``extract_on_failure`` methods will be called.

Both methods return ``OperatorLineage`` structure:

.. code-block::  python

    @define
    class OperatorLineage:
        """Structure returned from lineage extraction."""

        inputs: list[Dataset] = Factory(list)
        outputs: list[Dataset] = Factory(list)
        run_facets: dict[str, RunFacet] = Factory(dict)
        job_facets: dict[str, BaseFacet] = Factory(dict)


Inputs and outputs are lists of plain OpenLineage datasets (`openlineage.client.event_v2.Dataset`).

``run_facets`` and ``job_facets`` are dictionaries of optional RunFacets and JobFacets that would be attached to the job - for example,
you might want to attach ``SqlJobFacet`` if your Operator is executing SQL.

To learn more about facets in OpenLineage see :ref:`custom_facets:openlineage`.

Registering Custom Extractor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OpenLineage integration does not know that you've provided an Extractor unless you'll register it.

It can be done by using ``extractors`` option in Airflow configuration.

.. code-block:: ini

    [openlineage]
    transport = {"type": "http", "url": "http://example.com:5000"}
    extractors = full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass

``AIRFLOW__OPENLINEAGE__EXTRACTORS`` environment variable is an equivalent.

.. code-block:: ini

  AIRFLOW__OPENLINEAGE__EXTRACTORS='full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass'

Optionally, you can separate them with whitespace. It's useful if you're providing them as part of some YAML file.

.. code-block:: ini

    AIRFLOW__OPENLINEAGE__EXTRACTORS: >-
      full.path.to.FirstExtractor;
      full.path.to.SecondExtractor


Remember to make sure that the path is importable for scheduler and worker.

Debugging Custom Extractor
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two common problems associated with custom Extractors.

First, is wrong path provided to ``extractors`` option in Airflow configuration. The path needs to be exactly the same as one you'd use from your code.
If the path is wrong or non-importable from worker, plugin will fail to load the Extractors and proper OpenLineage events for that Operator won't be emitted.

Second one, and maybe more insidious, are imports from Airflow. Due to the fact that OpenLineage code gets instantiated when Airflow worker itself starts,
any import from Airflow can be unnoticeably cyclical. This causes OpenLineage extraction to fail.

To avoid this issue, import from Airflow only locally - in ``_execute_extraction`` or ``extract_on_complete``/``extract_on_failure`` methods.
If you need imports for type checking, guard them behind typing.TYPE_CHECKING.


Testing Custom Extractor
^^^^^^^^^^^^^^^^^^^^^^^^
As all code, custom Extractors should be tested. This section will provide some information about the most important
data structures to write tests for and some notes on troubleshooting. We assume prior knowledge of writing custom Extractors.
To learn more about how Operators and Extractors work together under the hood, check out :ref:`custom_extractors:openlineage`.

When testing an Extractor, we want to firstly verify if ``OperatorLineage`` object is being created,
specifically verifying that the object is being built with the correct input and output datasets and relevant facets.
This is done in OpenLineage via pytest, with appropriate mocking and patching for connections and objects.
Check out `example tests <https://github.com/apache/airflow/blob/main/providers/openlineage/tests/unit/openlineage/extractors/test_base.py>`_.

Testing each facet is also important, as data or graphs in the UI can render incorrectly if the facets are wrong.
For example, if the facet name is created incorrectly in the Extractor, then the Operator's task will not show up in the lineage graph,
creating a gap in pipeline observability.

Even with unit tests, an Extractor may still not be operating as expected.
The easiest way to tell if data isn't coming through correctly is if the UI elements are not showing up correctly in the Lineage tab.

See :ref:`troubleshooting:openlineage` for details on how to troubleshoot OpenLineage locally.

Example
^^^^^^^

This is an example of a simple Extractor for an Operator that executes export Query in BigQuery and saves the result to S3 file.
Some information is known before Operator's ``execute`` method is called, and we can already extract some lineage in ``_execute_extraction`` method.
After Operator's ``execute`` method is called, in ``extract_on_complete``, we can simply attach some additional Facets
f.e. with Bigquery Job ID to what we've prepared earlier. We can also implement ``extract_on_failure`` method, if there is
a need to include some information only when task fails. This way, we get all possible information from the Operator.

Please note that this is just an example. There are some OpenLineage built-in features that can facilitate different processes,
like extracting column level lineage and inputs/outputs from SQL query with SQL parser.

.. code-block:: python

    from airflow.models.baseoperator import BaseOperator
    from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
    from airflow.providers.common.compat.openlineage.facet import (
        Dataset,
        ExternalQueryRunFacet,
        ErrorMessageRunFacet,
        SQLJobFacet,
    )


    class ExampleOperator(BaseOperator):
        def __init__(self, query, bq_table_reference, s3_path) -> None:
            self.bq_table_reference = bq_table_reference
            self.s3_path = s3_path
            self.s3_file_name = s3_file_name
            self._job_id = None

        def execute(self, context) -> Any:
            self._job_id, self._error_message = run_query(query=self.query)


    class ExampleExtractor(BaseExtractor):
        @classmethod
        def get_operator_classnames(cls):
            return ["ExampleOperator"]

        def _execute_extraction(self) -> OperatorLineage:
            """Define what we know before Operator's extract is called."""
            return OperatorLineage(
                inputs=[Dataset(namespace="bigquery", name=self.operator.bq_table_reference)],
                outputs=[Dataset(namespace=self.operator.s3_path, name=self.operator.s3_file_name)],
                job_facets={
                    "sql": SQLJobFacet(
                        query="EXPORT INTO ... OPTIONS(FORMAT=csv, SEP=';' ...) AS SELECT * FROM ... "
                    )
                },
            )

        def extract_on_complete(self, task_instance) -> OperatorLineage:
            """Add what we received after Operator's extract call."""
            lineage_metadata = self.extract()
            lineage_metadata.run_facets = {
                "parent": ExternalQueryRunFacet(externalQueryId=task_instance.task._job_id, source="bigquery")
            }
            return lineage_metadata

        def extract_on_failure(self, task_instance) -> OperatorLineage:
            """Add any failure-specific information."""
            lineage_metadata = self.extract_on_complete(task_instance)
            lineage_metadata.run_facets = {
                "error": ErrorMessageRunFacet(
                    message=task_instance.task._error_message, programmingLanguage="python"
                )
            }
            return lineage_metadata

For more examples of OpenLineage Extractors, check out the source code of
`BashExtractor <https://github.com/apache/airflow/blob/main/providers/openlineage/src/airflow/providers/openlineage/extractors/bash.py>`_ or
`PythonExtractor <https://github.com/apache/airflow/blob/main/providers/openlineage/src/airflow/providers/openlineage/extractors/python.py>`_.

.. _custom_facets:openlineage:

Custom Facets
=============
To learn more about facets in OpenLineage, please refer to `facet documentation <https://openlineage.io/docs/spec/facets/>`_.
Also check out `available facets <https://github.com/OpenLineage/OpenLineage/blob/main/client/python/src/openlineage/client/facet.py>`_
and a blog post about `extending with facets <https://openlineage.io/blog/extending-with-facets/>`_.

The OpenLineage spec might not contain all the facets you need to write your extractor,
in which case you will have to make your own `custom facets <https://openlineage.io/docs/spec/facets/custom-facets>`_.

You can also inject your own custom facets in the lineage event's run facet using the ``custom_run_facets`` Airflow configuration.

Steps to be taken,

1. Write a function that returns the custom facets. You can write as many custom facet functions as needed.
2. Register the functions using the ``custom_run_facets`` Airflow configuration.

Airflow OpenLineage listener will automatically execute these functions during the lineage event generation and append their return values to the run facet in the lineage event.

Writing a custom facet function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _job_hierarchy:openlineage:

Job Hierarchy
=============

Apache Airflow features an inherent job hierarchy: Dags, large and independently schedulable units, comprise smaller, executable tasks.

OpenLineage reflects this structure in its Job Hierarchy model.

- Upon Dag scheduling, a START event is emitted.
- Subsequently, following Airflow's task order, each task triggers:

  - START events at TaskInstance start.
  - COMPLETE/FAILED events upon completion.

- Finally, upon Dag termination, a completion event (COMPLETE or FAILED) is emitted.

TaskInstance events' ParentRunFacet references the originating Dag run.

.. _troubleshooting:openlineage:

Troubleshooting
=====================

When testing code locally, `Marquez <https://marquezproject.ai/docs/quickstart>`_ can be used to inspect the data being emitted—or not being emitted.
Using Marquez will allow you to figure out if the error is being caused by the Extractor or the API.
If data is being emitted from the Extractor as expected but isn't making it to the UI,
then the Extractor is fine and an issue should be opened up in OpenLineage. However, if data is not being emitted properly,
it is likely that more unit tests are needed to cover Extractor behavior.
Marquez can help you pinpoint which facets are not being formed properly so you know where to add test coverage.

Debug settings
^^^^^^^^^^^^^^
For debugging purposes, ensure that both `Airflow logging level <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#logging-level>`_
and `OpenLineage client logging level <https://openlineage.io/docs/client/python#environment-variables>`_ is set to ``DEBUG``.
The latest provider auto-syncs Airflow's logging level with the OpenLineage client, removing the need for manual configuration.

For DebugFacet, containing additional information (e.g., list of all packages installed), to be appended to all OL events
enable :ref:`debug_mode <options:debug_mode>` for OpenLineage integration.

Keep in mind that enabling these settings will increase the detail in Airflow logs (which will increase their size) and
add extra information to OpenLineage events. It's recommended to use them temporarily, primarily for debugging purposes.

When seeking help with debugging, always try to provide the following:

-    Airflow scheduler logs with the logging level set to DEBUG
-    Airflow worker logs (task logs) with the logging level set to DEBUG
-    OpenLineage events with debug_mode enabled
-    Information about Airflow version and OpenLineage provider version
-    Information about any custom modifications made to the deployment environment where the Airflow is running


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
