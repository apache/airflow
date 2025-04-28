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

.. :py:module: airflow.sdk

Defining DAGs
-------------

.. autoapiclass:: airflow.sdk.DAG

.. autoapifunction:: airflow.sdk.dag

.. autoapiclass:: airflow.sdk.TaskGroup

.. autoapifunction:: airflow.sdk.get_parsing_context

.. autoapiclass:: airflow.sdk.definitions.context.AirflowParsingContext
   :undoc-members:
   :members:


Tasks and Operators
-------------------

.. autoapiclass:: airflow.sdk.BaseOperator

.. autoapiclass:: airflow.sdk.XComArg

.. autoapifunction:: airflow.sdk.get_current_context

Assets
------

.. autoapiclass:: airflow.sdk.Asset

.. autoapiclass:: airflow.sdk.AssetAlias

.. autoapiclass:: airflow.sdk.AssetAll

.. autoapiclass:: airflow.sdk.AssetAny

.. autoapiclass:: airflow.sdk.AssetWatcher


.. Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher

Everything else
---------------

.. autoapimodule:: airflow.sdk
  :members:
  :exclude-members: BaseOperator, DAG, dag, asset, Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher, TaskGroup, XComArg, get_current_context, get_parsing_context
  :undoc-members:
  :imported-members:
  :no-index:
