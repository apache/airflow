
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

OpenLineage defines a few methods for implementation in Operators. Those are referred to as OpenLineage methods.

.. code-block:: python

  def get_openlineage_facets_on_start() -> OperatorLineage:
      ...


  def get_openlineage_facets_on_complete(ti: TaskInstance) -> OperatorLineage:
      ...


  def get_openlineage_facets_on_failure(ti: TaskInstance) -> OperatorLineage:
      ...

OpenLineage methods get called respectively when task instance changes state to RUNNING, SUCCESS and FAILED.
If there's no ``on_complete`` or ``on_failure`` method, the ``on_start`` gets called instead.

Instead of returning complete OpenLineage event, the provider defines ``OperatorLineage`` structure to be returned by Operators:

.. code-block:: python

  @define
  class OperatorLineage:
      inputs: list[Dataset] = Factory(list)
      outputs: list[Dataset] = Factory(list)
      run_facets: dict[str, BaseFacet] = Factory(dict)
      job_facets: dict[str, BaseFacet] = Factory(dict)

OpenLineage integration itself takes care to enrich it with things like general Airflow facets, proper event time and type, creating proper OpenLineage RunEvent.

How to properly implement OpenLineage methods?
==============================================

There are a couple of things worth noting when implementing OpenLineage in operators.

First, do not import OpenLineage methods on top-level, but in OL method itself.
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
implementing ``get_openlineage_facets_on_failure`` is probably not worth it.

Here's example of properly implemented ``get_openlineage_facets_on_complete`` method, for ``GcsToGcsOperator``.

.. code-block::

    def get_openlineage_facets_on_complete(self, task_instance):
        """
        Implementing _on_complete because execute method does preprocessing on internals.
        This means we won't have to normalize self.source_object and self.source_objects,
        destination bucket and so on.
        """
        from openlineage.client.run import Dataset
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


How to add tests to OpenLineage integration?
============================================

Unit testing OpenLineage integration in operators is very similar to testing operators itself.
Objective of those tests is making sure the ``get_openlineage_*`` methods return proper ``OperatorLineage``
data structure with relevant fields filled. It's recommended to mock any external calls.
Authors of tests need to remember the condition of calling different OL methods is different.
``get_openlineage_facets_on_start`` is called before ``execute``, and as such, must not depend on values
that are set there.

System testing OpenLineage integration relies on the existing system test framework.
There is special ``VariableTransport`` that gathers OpenLineage events in Airflow database,
and ``OpenLineageTestOperator`` that compares those events to expected ones. Objective of author
of OpenLineage system test is to provide expected dictionary of event keys and events to ``OpenLineageTestOperator``.

Event keys identify event send from particular operator and method: they have structure ``<dag_id>.<task_id>.event.<event_type>``;
it's always possible to identify particular event send from particular task this way.

The provided event structure does not have to contain all the fields that are in the resulting event.
Only the fields provided by test author are compared; this allows to check only for fields particular
test cares about. It also allows to skip fields that are (semi) randomly generated, like ``runId`` or ``eventTime``,
or just always the same in context of OpenLineage in Airflow, like ``producer``.
