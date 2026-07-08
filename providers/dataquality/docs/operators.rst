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

.. _howto/operator:DQCheckOperator:

``DQCheckOperator``
=====================

Use :class:`~airflow.providers.dataquality.operators.dq_check.DQCheckOperator` to run a
:doc:`ruleset <rules>` against a table and persist per-rule results to the configured results
store. Every rule is evaluated and recorded regardless of the task outcome -- a rule failing
doesn't stop the others from running, and the full set of results is always written before the
task decides whether to fail.

Basic usage
------------

Pass a connection, table, and ruleset directly:

.. exampleinclude:: /../tests/system/dataquality/example_dq_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dq_check]
    :end-before: [END howto_operator_dq_check]

Parameters
-----------

- ``ruleset`` -- a :class:`~airflow.providers.dataquality.rules.RuleSet`, its dict form, or a path to a
  YAML ruleset file (see :doc:`rules`). Optional when ``asset`` carries one.
- ``table`` -- the table to check. Optional when ``asset`` carries one (falling back to the
  asset's name).
- ``asset`` -- an :class:`~airflow.sdk.Asset` decorated with
  :func:`~airflow.providers.dataquality.assets.asset_quality`. Supplies defaults for ``ruleset``,
  ``table``, and ``conn_id`` (explicit arguments win) and is automatically added to the task's
  outlets so its asset events carry the check summary -- see :doc:`assets`.
- ``partition_clause`` -- predicate ANDed into every built-in check's ``WHERE`` clause, e.g.
  ``"ds = '{{ ds }}'"`` (templated).
- ``fail_on`` -- severity that fails the task:

  - ``error`` (default) -- only ``error``-severity rule failures fail the task; ``warn``
    failures are recorded but don't fail it.
  - ``warn`` -- any rule failure (``error`` or ``warn`` severity) fails the task.
  - ``never`` -- rule failures never fail the task; only an execution error (the check query
    itself failing) does.

- ``conn_id`` -- connection to the database to check, any ``common.sql`` ``DbApiHook``-
  compatible type.
- ``database`` -- optional database/schema, overriding the connection's default.

Regardless of ``fail_on``, an execution error -- the check query itself failing, as opposed to
a rule failing its condition -- always fails the task; there's no way to check data whose query
can't even run.

Persisting results
--------------------

Results are persisted to the backend configured under ``[dataquality] results_path`` (see
:doc:`configurations-ref`). When that's unset, checks still run and the task still passes or
fails normally -- only persisted data quality history is unavailable. There is no
per-operator override: every check in a deployment shares one results store, so history stays
available across tasks and Dags without stitching together several stores.

Checking an asset
--------------------

Attach a ruleset to an :class:`~airflow.sdk.Asset` with
:func:`~airflow.providers.dataquality.assets.asset_quality`, then pass the asset instead of ``table``/
``ruleset``/``conn_id``:

.. exampleinclude:: /../tests/system/dataquality/example_dq_check.py
    :language: python
    :start-after: [START howto_operator_dq_check_asset]
    :end-before: [END howto_operator_dq_check_asset]

The operator adds the asset to its own outlets automatically, and attaches the check's summary
(including its quality ``score``) to the asset event -- which is what makes
:func:`~airflow.providers.dataquality.assets.require_quality` (see :doc:`assets`) able to gate a
downstream consumer Dag on it.

``custom_sql`` checks
------------------------

Built-in checks are all single-column. For cross-column comparisons, joins, or anything the
catalog doesn't cover, use ``custom_sql``:

.. exampleinclude:: /../src/airflow/providers/dataquality/example_dags/example_dq_check_custom_sql.py
    :language: python
    :start-after: [START howto_operator_dq_check_custom_sql]
    :end-before: [END howto_operator_dq_check_custom_sql]

See :doc:`rules` for the full built-in check catalog and the ``custom_sql`` grammar.

Loading a ruleset from YAML
------------------------------

.. exampleinclude:: /../src/airflow/providers/dataquality/example_dags/example_dq_ruleset_from_yaml.py
    :language: python
    :start-after: [START howto_operator_dq_check_ruleset_from_yaml]
    :end-before: [END howto_operator_dq_check_ruleset_from_yaml]

TaskFlow decorator
--------------------

See :doc:`decorators` for the ``@task.dq_check`` equivalent, including runtime rule sets.
