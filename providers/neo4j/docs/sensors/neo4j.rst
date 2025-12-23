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



.. _howto/sensor:neo4j:

Neo4jSensor
============

The :class:`~airflow.providers.neo4j.sensors.neo4j.Neo4jSensor` executes a Cypher query
in a Neo4j database until the returned value satisfies a condition.

The sensor runs the query repeatedly at the defined ``poke_interval`` until:

* A callable provided in ``failure`` evaluates to ``True``, which raises an exception.
* A callable provided in ``success`` evaluates to ``True``, which marks the sensor as successful.
* Otherwise, the truthiness of the selected value determines success.

The sensor uses :class:`~airflow.providers.neo4j.hooks.neo4j.Neo4jHook` and the
`Neo4j Python driver <https://neo4j.com/developer/python/>`_ for communication
with the database.

Prerequisites
-------------

To use the Neo4j sensor:

* A Neo4j instance must be reachable from the Airflow environment.
* A valid Neo4j connection must be configured in Airflow (for example
  ``neo4j_default``), as described in :ref:`howto/connection:neo4j`.
* The ``neo4j`` provider package must be installed in your Airflow environment.

Basic Usage
-----------

The simplest use case is to run a Cypher query and rely on the first value of the
first row to determine success. Any truthy value will mark the sensor as successful.

Example: Wait for at least one ``Person`` node to exist:

.. code-block:: python

   from airflow import DAG
   from airflow.utils.dates import days_ago
   from airflow.providers.neo4j.sensors.neo4j import Neo4jSensor

   with DAG(
       dag_id="example_neo4j_sensor_basic",
       start_date=days_ago(1),
       schedule=None,
       catchup=False,
   ):
       wait_person_exists = Neo4jSensor(
           task_id="wait_person_exists",
           neo4j_conn_id="neo4j_default",
           cypher="MATCH (p:Person) RETURN count(p) > 0",
       )

In this example, the Cypher query returns a single boolean value. When it becomes
``True``, the sensor succeeds.

Templating
----------

The following fields of :class:`~airflow.providers.neo4j.sensors.neo4j.Neo4jSensor`
are templated:

* ``cypher``
* ``parameters``

This allows you to build dynamic queries using Jinja templates based on the
execution context.

Example: Use execution date in the query:

.. code-block:: python

   wait_events_for_date = Neo4jSensor(
       task_id="wait_events_for_date",
       neo4j_conn_id="neo4j_default",
       cypher="""
           MATCH (e:Event)
           WHERE e.date = $event_date
           RETURN count(e) > 0
       """,
       parameters={"event_date": "{{ ds }}"},
   )

Advanced Usage
--------------

Custom success and failure conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can provide callables for ``success`` and ``failure`` to implement more complex
logic. Each callable receives the selected value and must return a boolean.

Note: ``failure`` condition takes priority over the ``success`` condition.

* ``success(value)`` – if provided, the sensor succeeds when this returns ``True``.
* ``failure(value)`` – if provided and returns ``True``, the sensor raises
  :class:`~airflow.exceptions.AirflowException`.

Example: Wait until a count reaches at least 10, and fail if it ever exceeds 1,000:

.. code-block:: python

   def success_when_at_least_10(value):
       return value >= 10


   def fail_when_too_large(value):
       return value > 1000


   wait_count_in_range = Neo4jSensor(
       task_id="wait_count_in_range",
       neo4j_conn_id="neo4j_default",
       cypher="MATCH (n:Item) RETURN count(n)",
       success=success_when_at_least_10,
       failure=fail_when_too_large,
   )

Using a custom selector
~~~~~~~~~~~~~~~~~~~~~~~

By default, the sensor applies ``operator.itemgetter(0)`` to the first row of the
result set, effectively selecting the first value of the first row. You can override
this with a custom ``selector`` callable.

The ``selector`` receives the first row as a tuple and must return a single value
to be used by ``success`` / ``failure`` or for truthiness evaluation.

Example: Select a specific column from the row:

.. code-block:: python

   from operator import itemgetter

   # Assume the query returns rows of the form (count, status)
   wait_status_ok = Neo4jSensor(
       task_id="wait_status_ok",
       neo4j_conn_id="neo4j_default",
       cypher="MATCH (s:ServiceStatus) RETURN s.count, s.status ORDER BY s.timestamp DESC LIMIT 1",
       selector=itemgetter(1),  # pick the 'status' column
       success=lambda status: status == "OK",
   )

Handling empty results
~~~~~~~~~~~~~~~~~~~~~~

If the query returns no rows:

* When ``fail_on_empty=False`` (default), the sensor simply returns ``False`` and
  will be re-scheduled for the next poke.
* When ``fail_on_empty=True``, the sensor raises
  :class:`~airflow.exceptions.AirflowException`.

Example: Fail immediately if there are no results:

.. code-block:: python

   wait_non_empty = Neo4jSensor(
       task_id="wait_non_empty",
       neo4j_conn_id="neo4j_default",
       cypher="MATCH (o:Order) RETURN o.id LIMIT 1",
       fail_on_empty=True,
   )

Reference
---------

Parameters
~~~~~~~~~~

``neo4j_conn_id``
    Connection ID to use for connecting to Neo4j. Defaults to ``"neo4j_default"``.

``cypher``
    Cypher statement to execute. This field is templated.

``parameters``
    Dictionary of query parameters passed to the Cypher statement. This field
    is templated.

``success``
    Optional callable that receives the selected value and returns a boolean.
    When provided, the sensor succeeds only when this callable returns ``True``.

``failure``
    Optional callable that receives the selected value. If provided and it
    returns ``True``, the sensor raises :class:`~airflow.exceptions.AirflowException``.

``selector``
    Callable that receives the first row of the query result as a tuple and
    returns a single value to be evaluated. Defaults to selecting the first
    element of the row with ``operator.itemgetter(0)``.

``fail_on_empty``
    When set to ``True``, the sensor raises an exception if the query returns
    no rows. When ``False`` (default), the sensor simply returns ``False`` and
    will poke again later.

``**kwargs``
    Additional keyword arguments passed to
    :class:`~airflow.providers.common.compat.sdk.BaseSensorOperator`, such as
    ``poke_interval``, ``timeout``, or ``mode``.
