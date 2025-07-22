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

airflow.sdk API Reference
=========================

This page documents the full public API exposed in Airflow 3.0+ via the Task SDK python module.

If something is not on this page it is best to assume that it is not part of the public API and use of it is entirely at your own risk
-- we won't go out of our way break usage of them, but we make no promises either.

Defining DAGs
-------------
.. autoapiclass:: airflow.sdk.DAG


Decorators
----------
.. autoapifunction:: airflow.sdk.dag
.. autoapifunction:: airflow.sdk.task

.. autoapifunction:: airflow.sdk.task_group

.. autoapifunction:: airflow.sdk.setup

.. autoapifunction:: airflow.sdk.teardown

.. autofunction:: airflow.sdk.task
.. autofunction:: airflow.sdk.setup
.. autofunction:: airflow.sdk.teardown
.. autofunction:: airflow.sdk.asset


Bases
-----
.. autoapiclass:: airflow.sdk.BaseOperator

.. autoapiclass:: airflow.sdk.BaseSensorOperator

.. autoapiclass:: airflow.sdk.BaseNotifier

.. autoapiclass:: airflow.sdk.BaseOperatorLink

.. autoapiclass:: airflow.sdk.BaseXCom

.. autoapiclass:: airflow.sdk.XComArg

.. autoapiclass:: airflow.sdk.PokeReturnValue

Connections & Variables
-----------------------
.. autoapiclass:: airflow.sdk.Connection

.. autoapiclass:: airflow.sdk.Variable

Tasks & Operators
-----------------
.. autoapiclass:: airflow.sdk.TaskGroup

.. autoapifunction:: airflow.sdk.get_current_context

.. autoapifunction:: airflow.sdk.get_parsing_context

.. autoapiclass:: airflow.sdk.Param

Setting Dependencies
~~~~~~~~~~~~~~~~~~~~
.. autoapifunction:: airflow.sdk.chain

.. autoapifunction:: airflow.sdk.chain_linear

.. autoapifunction:: airflow.sdk.cross_downstream

.. autoapifunction:: airflow.sdk.literal

Edges & Labels
~~~~~~~~~~~~~~
.. autoapiclass:: airflow.sdk.EdgeModifier

.. autoapiclass:: airflow.sdk.Label

Assets
------
.. autoapiclass:: airflow.sdk.Asset

.. autoapiclass:: airflow.sdk.AssetAlias

.. autoapiclass:: airflow.sdk.AssetAll

.. autoapiclass:: airflow.sdk.AssetAny

.. autoapiclass:: airflow.sdk.AssetWatcher

.. autoapiclass:: airflow.sdk.Metadata

I/O Helpers
-----------
.. autoapiclass:: airflow.sdk.ObjectStoragePath

Execution Time Components
-------------------------
.. rubric:: Context
.. autoapiclass:: airflow.sdk.Context
.. autoapimodule:: airflow.sdk.execution_time.context
   :members:
   :undoc-members:


Everything else
---------------

.. autoapimodule:: airflow.sdk
  :members:
  :special-members: __version__
  :exclude-members: BaseOperator, DAG, dag, asset, Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher, TaskGroup, XComArg, get_current_context, get_parsing_context
  :undoc-members:
  :imported-members:
  :no-index:
