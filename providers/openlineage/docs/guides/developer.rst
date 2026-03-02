
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

To learn more about facets in OpenLineage see :ref:`configuration_custom_facets:openlineage`.

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


.. _inlets_outlets:openlineage:

Manually annotated lineage
==========================

This approach is rarely recommended, only in very specific cases, when it's impossible to extract some lineage information from the Operator itself.
If you want to extract lineage from your own Operators, you may prefer directly implementing OpenLineage methods as described in :ref:`openlineage_methods:openlineage`.
When dealing with Operators that you can not modify (f.e. third party providers), but still want the lineage to be extracted from them, see :ref:`custom_extractors:openlineage`.

Airflow allows Operators to track lineage by specifying the input and outputs of the Operators via
`inlets and outlets <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html>`_.
By default, OpenLineage uses inlets and outlets as input and output datasets when it cannot find any successfully
extracted datasets from OpenLineage methods or Extractors. When Airflow Assets are used as inlets and outlets,
OpenLineage attempts to convert them into OpenLineage Datasets and includes them as input and output datasets in
the resulting event. If this conversion is not possible, the inlets and outlets information is still available in the
AirflowRunFacet, under task.inlets and task.outlets. When OpenLineage Datasets are used directly as inlets and outlets,
no conversion is required. However, this usage is specific to OpenLineage only: Airflow ignores OpenLineage Datasets
provided in inlets and outlets, and they are not treated as Airflow Assets. This mechanism is supported solely for
OpenLineage's purposes and does not replace or affect Airflow Assets.


Example
^^^^^^^

An Operator inside the Airflow DAG can be annotated with inlets and outlets like in the below example:

.. code-block:: python

    """Example DAG demonstrating the usage of the extraction via Inlets and Outlets."""

    import pendulum

    from airflow import DAG
    from openlineage.client.event_v2 import Dataset

    t1 = Dataset(namespace="postgres:my-host.com:1234", name="db.sch.t1")
    t2 = Dataset(namespace="mysql:another-host.com:5678", name="db.sch.t2")
    f1 = File(url="s3://bucket/dir/file1")

    with DAG(
        dag_id="example_operator",
        schedule="@once",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    ) as dag:
        task1 = BashOperator(
            task_id="task_1_with_inlet_outlet",
            bash_command="exit 0;",
            inlets=[t1, t2],
            outlets=[f1],
        )


.. _extraction_helpers:openlineage:

Helper functions
=================

Some providers expose helper functions that simplify OpenLineage event emission for SQL queries executed within custom operators.
These functions are particularly useful when executing multiple queries in a single task, as they allow you to treat each SQL query
as a separate child job of the Airflow task, creating a more granular lineage graph.

The helper functions automatically:

- Create START and COMPLETE/FAIL OpenLineage events for each query
- Link child query jobs to the parent Airflow task using ParentRunFacet
- Optionally retrieve additional metadata (execution times, query text, error messages) from the database
- Handle event serialization and emission to the OpenLineage backend

Currently available helper functions:

- ``emit_openlineage_events_for_snowflake_queries`` - For Snowflake queries
- ``emit_openlineage_events_for_databricks_queries`` - For Databricks SQL queries


Example
^^^^^^^

When using Airflow hooks (e.g., ``SnowflakeHook``, ``DatabricksSqlHook``), the helper functions can automatically
retrieve connection information to build the namespace and extract query IDs from the hook if the hook's ``query_ids``
attribute is populated. Some hooks (like ``DatabricksHook``) may not automatically track query IDs,
in which case you'll need to provide them explicitly.


.. code-block:: python

    from airflow import task
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from airflow.providers.snowflake.utils.openlineage import emit_openlineage_events_for_snowflake_queries
    from airflow.utils.context import get_current_context


    @task
    def execute_queries():
        context = get_current_context()
        task_instance = context["ti"]
        hook = SnowflakeHook(snowflake_conn_id="snowflake")

        # Execute queries - hook.run() automatically tracks query_ids
        hook.run("SELECT * FROM table1; INSERT INTO table2 SELECT * FROM table1;")

        # Emit OpenLineage events for all executed queries
        emit_openlineage_events_for_snowflake_queries(
            task_instance=task_instance,
            hook=hook,
            query_for_extra_metadata=True,  # Fetch query text, execution times, etc.
        )


When executing queries using raw SDKs (e.g., ``snowflake-connector-python``, ``databricks-sql-connector``) or other methods
that don't use Airflow hooks, you need to manually track query IDs and provide them to the helper function along with
the namespace. In this case, the function cannot retrieve additional metadata from the database.

.. code-block:: python

    from databricks import sql
    from airflow import task
    from airflow.providers.databricks.utils.openlineage import emit_openlineage_events_for_databricks_queries
    from airflow.utils.context import get_current_context


    @task
    def execute_queries():
        context = get_current_context()
        task_instance = context["ti"]
        query_ids = []

        # Connect using raw Databricks SQL connector
        connection = sql.connect(
            server_hostname="workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/warehouse_id",
            access_token="token",
        )
        cursor = connection.cursor()

        try:
            # Execute queries and capture query IDs
            result = cursor.execute("SELECT * FROM table1")
            query_ids.append(result.command_id)  # Get query ID from result

            result = cursor.execute("INSERT INTO table2 SELECT * FROM table1")
            query_ids.append(result.command_id)

            connection.commit()
        finally:
            cursor.close()
            connection.close()

        # Emit OpenLineage events - must provide query_ids and namespace explicitly
        emit_openlineage_events_for_databricks_queries(
            task_instance=task_instance,
            query_ids=query_ids,
            query_source_namespace="databricks://workspace.cloud.databricks.com",
            query_for_extra_metadata=False,  # Cannot fetch metadata without hook
        )

Troubleshooting
===============

See :ref:`troubleshooting:openlineage`.
