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

.. _dq:assets:

Assets and quality gating
============================

Quality rules can travel with the :class:`~airflow.sdk.Asset` they describe instead of being
scattered across every Dag that checks it, and a downstream consumer Dag can refuse to run when
the data it was triggered by did not meet a minimum quality bar.

Attaching a ruleset to an asset
----------------------------------

:func:`~airflow.providers.dq.assets.asset_quality` stores ruleset, connection, and table
configuration inside ``Asset.extra`` under the ``airflow.dq`` key, so it is serialized with the
Dag and needs no Airflow core changes:

.. exampleinclude:: /../src/airflow/providers/dq/example_dags/example_dq_require_quality.py
    :language: python
    :start-after: [START howto_asset_quality]
    :end-before: [END howto_asset_quality]

Pass the resulting asset to :class:`~airflow.providers.dq.operators.dq_check.DQCheckOperator`
via ``asset=`` (see :doc:`operators`) instead of ``table``/``ruleset``/``conn_id``: the operator
resolves all three from the asset's config, adds the asset to its own outlets, and attaches its
summary -- including the quality ``score`` used below -- to the asset event.

Gating a consumer Dag on quality
------------------------------------

:func:`~airflow.providers.dq.assets.require_quality` builds a ``@task.short_circuit`` task that
reads the ``score`` off the asset event that triggered the current run, and skips every
downstream task when that event has no quality summary at all, or its score is below
``min_score``:

.. exampleinclude:: /../src/airflow/providers/dq/example_dags/example_dq_require_quality.py
    :language: python
    :start-after: [START howto_require_quality]
    :end-before: [END howto_require_quality]

Put the gate first in a Dag scheduled by the asset, and chain everything else after it:

.. code-block:: python

    with DAG("orders_consumer", schedule=orders_asset) as consumer:
        gate = require_quality(orders_asset, min_score=0.95)
        gate >> process_orders()

``min_score`` must be between ``0`` and ``1``; the check considers only the *most recent*
triggering event for the asset when a run was triggered by several.
