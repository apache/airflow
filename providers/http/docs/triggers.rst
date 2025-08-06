
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

HTTP Event Trigger
==================

.. _howto/trigger:HttpEventTrigger:

The ``HttpEventTrigger`` is an event-based trigger that monitors whether responses
from an API meet the conditions set by the user in the ``response_check`` callable.

It is especially useful for **Airflow 3.0+** in combination with the ``AssetWatcher`` system,
enabling event-driven DAGs based on API responses.

How It Works
------------

1. Sends requests to an API.
2. Uses the callable at ``response_check_path`` to evaluate the API response.
3. If the callable returns ``True``, a ``TriggerEvent`` is emitted. This will trigger DAGs using this ``AssetWatcher`` for scheduling.

.. note::
   This trigger requires **Airflow >= 3.0** due to dependencies on ``AssetWatcher`` and event-driven scheduling infrastructure.

Usage Example with AssetWatcher
-------------------------------

Here's an example of using the HttpEventTrigger in an AssetWatcher to monitor the GitHub API for new Airflow releases.

.. exampleinclude:: /../../http/tests/system/http/example_http_event_trigger_dag.py
    :language: python
    :start-after: [START howto_http_event_trigger]
    :end-before: [END howto_http_event_trigger]

Parameters
----------

``http_conn_id``
    http connection id that has the base API url i.e https://www.google.com/ and optional authentication credentials.
    Default headers can also be specified in the Extra field in json format.

``auth_type``
    The auth type for the service

``method``
    the API method to be called

``endpoint``
    Endpoint to be called, i.e. ``resource/v1/query?``

``headers``
    Additional headers to be passed through as a dict

``data``
    Payload to be uploaded or request parameters

``extra_options``
    Additional kwargs to pass when creating a request.

``response_check_path``
    Path to callable that evaluates whether the API response passes the conditions set by the user to trigger DAGs


Important Notes
---------------

1. The ``response_check_path`` must contain the path to an asynchronous callable. Synchronous callables will raise an exception.
2. If no ``response_check_path`` is provided, the trigger will emit an event for every API response without any checks. This could trigger a high number of concurrent DAG runs, which could lock the Airflow database.
3. This trigger does not automatically record the previous API response.
4. The previous response may have to be persisted manually though ``Variable.set()`` in the ``response_check_path`` callable to prevent the trigger from emitting events repeatedly for the same API response.
