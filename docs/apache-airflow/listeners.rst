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

Listeners
=========

Airflow gives you an option to be notified of events happening in Airflow
by writing listeners. Listeners are powered by `pluggy <https://pluggy.readthedocs.io/en/stable/>`__

Listener API is meant to be called across all dags, and all operators - in contrast to methods like
``on_success_callback``, ``pre_execute`` and related family which are meant to provide callbacks
for particular dag authors, or operator creators. There is no possibility to listen on events generated
by particular dag.

To include listener in your Airflow installation, include it as a part of an :doc:`Airflow Plugin </plugins>`

|experimental|

Interface
---------

To create a listener you will need to derive the
create python module, import ``airflow.listeners.hookimpl`` and implement the ``hookimpls`` for
events you want to be notified at.

Right now Airflow exposes TaskInstance state change events.
Their specification is defined as ``hookspec`` in ``airflow/listeners/spec.py`` file.
