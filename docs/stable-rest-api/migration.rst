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

Migration Guide from Experimental API to Stable API v1
======================================================
This article provides guidelines for migrating from experimental REST API to the
stable REST API.

Introduction
------------
If your application is still using the experimental API, it is important to
consider migrating to the stable API so that your application continues to
work.

The stable API exposes many endpoints available through the webserver. Here are the
differences between the two endpoints that will help you migrate from the
experimental REST API to the stable REST API.

Base Endpoint
^^^^^^^^^^^^^
The base endpoint for the stable API v1 is ``/api/v1/``. You must change the
experimental base endpoint from ``/api/experimental/`` to ``/api/v1/``.

Create a dag_run from a given dag_id
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The endpoint for creating a dag_run from a given dag_id has changed from

.. http:post:: /api/experimental/dags/<DAG_ID>/dag_runs

to

.. http:post:: /api/v1/dags/{dag_id}/dagRuns


List dag_runs from a specific DAG ID
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The endpoint to get the list of dag_runs for a specific dag_id has changed from

.. http:get:: /api/experimental/dags/<DAG_ID>/dag_runs

to

.. http:get:: /api/v1/dags/{dag_id}/dagRuns

This endpoint also allows you to filter dag_runs with parameters such as ``start_date``, ``end_date``, ``execution_date`` etc in the query string.
Therefore the operation previously performed by this endpoint

.. http:get:: /api/experimental/dags/<string:dag_id>/dag_runs/<string:execution_date>

can now be handled with filter parameters in the query string.

Health endpoint
^^^^^^^^^^^^^^^
The operation previously performed in the experimental REST API endpoint to check
the health status has changed from

.. http:get:: /api/experimental/test

to

.. http:get:: /api/v1/health

Task information endpoint
^^^^^^^^^^^^^^^^^^^^^^^^^
The endpoint for getting task information has changed from

.. http:get:: /api/experimental/dags/<DAG_ID>/tasks/<TASK_ID>

to

.. http:get:: /api/v1//dags/{dag_id}/tasks/{task_id}

Task Instance
^^^^^^^^^^^^^
The endpoint for getting task instance's public instance variable
has changed from

.. http:get:: /api/experimental/dags/<DAG_ID>/dag_runs/<string:execution_date>/tasks/<TASK_ID>

to

.. http:get:: /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}


DAG
^^^
The endpoint for pausing a dag has changed from

.. http:get:: /api/experimental/dags/<DAG_ID>/paused/<string:paused>

to

.. http:patch:: /api/v1/dags/{dag_id}

while getting information about the paused state of a dag has changed from

.. http:get:: /api/experimental/dags/<DAG_ID>/paused

to

.. http:get:: /api/v1/dags/{dag_id}


Latest DAG Runs
^^^^^^^^^^^^^^^
The endpoint for getting the latest DagRun for each DAG formatted for the UI
have changed from

.. http:get:: /api/experimental/latest_runs

to

.. http:get:: /api/v1/dags/{dag_id}/dagRuns

Getting information about latest runs can be accomplished with the help of
filters in the query string of this endpoint. Please check the Stable API
reference documentation for more information

Get all Pools
^^^^^^^^^^^^^
The endpoint for getting all pools has changed from

.. http:get:: /api/experimental/pools

to

.. http:get:: /api/v1/pools

Get pool by a given name
^^^^^^^^^^^^^^^^^^^^^^^^
The endpoint to get pool by a given name has changed from

.. http:get:: /api/experimental/pools/<string:name>

to

.. http:get:: /api/v1/pools/{pool_name}

Create a Pool
^^^^^^^^^^^^^
The endpoint for creating a pool has changed from

.. http:post:: /api/experimental/pools

to

.. http:post:: /api/v1/pools

Delete a Pool
^^^^^^^^^^^^^
The endpoint for deleting a pool has changed from

.. http:delete:: /api/experimental/pools/<string:name>

to

.. http:delete:: /api/v1/pools/{pool_name}

DAG Lineage
^^^^^^^^^^^
The endpoint for returning the lineage of a dag have changed from

.. http:get:: /api/experimental/lineage/<DAG_ID>/<string:execution_date>/

to

.. http:get:: /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries
