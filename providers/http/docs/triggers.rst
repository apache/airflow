
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
from an api meet the conditions set by the user in the ``response_check`` callable.

It is especially useful for **Airflow 3.0+** in combination with the ``AssetWatcher`` system,
enabling event-driven DAGs based on API responses.

How It Works
------------

1. Periodically sends requests to an API.
2. Uses the ``response_check`` callable to evaluate the API response.
3. If ``response_check`` returns ``True``, a ``TriggerEvent`` is emitted. This will trigger DAGs
using this ``AssetWatcher`` for scheduling.

.. note::
    This trigger requires **Airflow >= 3.0** due to dependencies on:
    - ``AssetWatcher``
    - Event-driven scheduling infrastructure
