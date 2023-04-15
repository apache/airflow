
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


Implementing OpenLineage in Operators
-------------------------------------

OpenLineage defines few methods for implementation for Operators.

.. code-block:: python

  def get_openlineage_facets_on_start() -> OperatorLineage:
      ...


  def get_openlineage_facets_on_complete(ti: TaskInstance) -> OperatorLineage:
      ...


  def get_openlineage_facets_on_failure(ti: TaskInstance) -> OperatorLineage:
      ...

Those get called respectively when task instance changes state to RUNNING, SUCCESS and FAILED.
It's required to implement ``on_start`` method.
If there's no ``on_complete`` or ``on_failure`` method, the ``on_start`` gets called instead.

Instead of returning complete OpenLineage event, the provider defines ``OperatorLineage`` structure to be returned by Operators:

.. code-block:: python

  @define
  class OperatorLineage:
      inputs: list[Dataset] = Factory(list)
      outputs: list[Dataset] = Factory(list)
      run_facets: dict[str, BaseFacet] = Factory(dict)
      job_facets: dict[str, BaseFacet] = Factory(dict)

OpenLineage integration takes care to enrich it with things like general Airflow facets, proper event time and type.
