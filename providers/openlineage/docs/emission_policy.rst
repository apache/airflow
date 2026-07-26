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


.. _emission_policy:openlineage:

Controlling what OpenLineage emits
==================================

This page is the one place to look when you want to control **which OpenLineage events are
emitted**, **how much metadata they contain**, and **who is allowed to override what**.

.. _emission_policy_kill_switch:openlineage:

Kill switch: ``disabled`` (turn OpenLineage off entirely)
---------------------------------------------------------

If you want to turn the OpenLineage integration off **completely** — no plugin loaded, no listener registered,
no events generated, no overhead — without uninstalling the provider, set:

.. code-block:: ini

    [openlineage]
    disabled = true

or the equivalent environment variable:

.. code-block:: bash

    AIRFLOW__OPENLINEAGE__DISABLED=true


.. note::

  Changing this variable may require Airflow restart, as the plugins (including OpenLineage) are loaded at startup.


This is the all-or-nothing kill switch — useful for incident response, for testing without OL
side-effects, or to keep the provider installed but inactive. When ``disabled = true`` no
``emission_policy`` rule is evaluated and no authoring override has any effect: the OL plugin
is not loaded.

For anything **less than all-or-nothing** — disabling specific DAGs, tasks, operator classes,
or specific facets; controlling source-code capture or hook-lineage extraction; locking
admin policy against per-Dag author overrides — use ``emission_policy`` instead, documented
below.


Two layers of control
---------------------

When OL is enabled, there are two layers of control, sitting on different sides of the wire:

1. **The Airflow OpenLineage provider** (this provider) controls what the *producer* generates
   in the first place — whether to run the extractor, whether to include source code, whether
   to emit task or DAG-run events at all, whether to inject the full ``AirflowRunFacet``, and
   so on. This is the ``emission_policy`` Airflow configuration, documented below.
2. **The OpenLineage Python client** controls what happens to events *after* they are built —
   dropping events by name pattern, applying transport-level rules. See
   `OpenLineage Python client – filters <https://openlineage.io/docs/client/python/configuration#filters>`_.

The provider runs first (skipping work) and the client runs second (reshaping what survives).

.. _emission_policy_unified:openlineage:

Unified ``emission_policy`` configuration
-----------------------------------------

The ``emission_policy`` option (under the ``[openlineage]`` section) accepts a JSON array of
rule objects. Each rule has this shape::

    {
      "scope":      {<scope keys>},          # required; use {} for "global"
      "match_mode": "exact" | "regex",       # optional; default "exact"
      "controls":   {<control flags>},       # required; must be non-empty
      "locked":     true | false             # optional; default false
    }

**Top-level keys** outside ``scope`` / ``match_mode`` / ``controls`` / ``locked`` are rejected
and the rule is skipped with a WARNING. The same applies to unknown keys inside ``scope`` or
``controls``. This catches typos like ``"dgg_id"`` immediately at config load.

**Scope keys** (all optional inside ``scope``):

- ``operator`` — fully-qualified operator class name; applies to task events for that operator type.
- ``dag_id`` — applies to task and/or DAG-level events for the named DAG (use ``emit_task_events``
  / ``emit_dag_events`` in ``controls`` to target one type selectively).
- ``task_id`` — only valid alongside ``dag_id``; targets a specific task.
- *(empty ``scope: {}``)* — global default; applies to every task and DAG event.

``scope`` must be one of: global (empty), operator-only, dag-only, or dag+task. ``task_id``
without ``dag_id``, or ``operator`` combined with ``dag_id`` / ``task_id``, is rejected with
a WARNING.

**``match_mode``** (top-level): ``"exact"`` (default) or ``"regex"``. When ``"regex"``, every
value inside ``scope`` (``dag_id``, ``task_id``, ``operator``) is treated as a ``re.fullmatch``
pattern.

**Control flag keys** (all optional inside ``controls``; the dict must be non-empty):

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Flag
     - Default
     - Description
   * - ``emit``
     - ``true``
     - Shorthand: disable **all** OpenLineage events in scope (both task and DAG-level).
   * - ``emit_task_events``
     - ``true``
     - Disable task-level events only; takes precedence over ``emit`` for task events within the
       same rule.
   * - ``emit_dag_events``
     - ``true``
     - Disable DAG-run-level events only; takes precedence over ``emit`` for DAG events within
       the same rule.
   * - ``extract_operator_metadata``
     - ``true``
     - Whether to run operator-specific extractor-based metadata collection (inputs/outputs). Task events only.
   * - ``include_source_code``
     - ``true``
     - Whether to include source code in Python/Bash operator events. Task events only.
       **No effect when ``extract_operator_metadata`` is ``false``** — the entire extraction
       pipeline is skipped.
   * - ``hook_lineage``
     - ``true``
     - Whether to use ``HookLineageCollector`` as a fallback when the extractor finds no
       inputs/outputs. Task events only.
       **No effect when ``extract_operator_metadata`` is ``false``** — the entire extraction
       pipeline (including hook lineage) is skipped.
   * - ``include_full_task_info``
     - ``false``
     - Whether to include the full serialized operator state in ``AirflowRunFacet``. When
       ``false``, only a curated subset of task attributes is sent. When ``true``, all
       serializable task parameters are included, which may significantly increase event size.
       Task events only.

``locked`` (top-level, default ``false``) is an admin-only floor lock: when ``true``, the
control fields carried by this rule's ``controls`` dict cannot be overridden by per-Dag /
per-task authoring flags (see :ref:`emission_policy_authoring:openlineage`). The rule still
participates in normal tier resolution against other conf rules — locking only blocks
authoring overrides.

``emit_task_events`` / ``emit_dag_events`` take precedence over ``emit`` within the same rule.
For example, ``{"scope": {"dag_id": "X"}, "controls": {"emit": false, "emit_task_events": true}}``
suppresses DAG-level events for X while leaving task events enabled.

**Priority** for task events (most specific tier wins; within a tier, the last matching rule wins):

1. ``dag_id`` + ``task_id``
2. ``dag_id``
3. ``operator``
4. Global (empty ``scope``)
5. Built-in defaults

**Priority** for DAG-level events:

1. ``dag_id`` rule (using ``emit_dag_events`` or ``emit``)
2. Global (empty ``scope``)
3. Built-in defaults

.. note::

    **Contradictory rules within the same tier.** When two rules in the same priority tier set the
    same flag to different values, the *last* rule in the list wins. A ``WARNING`` is logged to help
    you spot accidental conflicts. For example,
    ``[{"scope": {}, "controls": {"emit": false}}, {"scope": {}, "controls": {"emit": true}}]``
    (both global) results in ``emit=true`` with a warning. Ordering rules intentionally is cleaner
    than relying on last-wins.

.. code-block:: ini

    [openlineage]
    emission_policy = [
      {"scope": {"operator": "airflow.providers.http.operators.http.HttpOperator"}, "controls": {"emit": false}},
      {"scope": {"operator": "airflow.providers.standard.operators.python.PythonOperator"}, "controls": {"include_source_code": false}},
      {"scope": {"dag_id": "expensive_dag"}, "controls": {"extract_operator_metadata": false}},
      {"scope": {"dag_id": "expensive_dag", "task_id": "send_report"}, "controls": {"extract_operator_metadata": true}},
      {"scope": {"dag_id": "my_dag", "task_id": "sensitive_task"}, "controls": {"hook_lineage": false}},
      {"scope": {"dag_id": "reporting_dag"}, "controls": {"emit_dag_events": false}},
      {"scope": {"dag_id": "full_control_dag"}, "controls": {"emit": false}},
      {"scope": {"dag_id": "full_control_dag", "task_id": "critical_task"}, "controls": {"emit": true}},
      {"scope": {"dag_id": "^staging_.*"}, "match_mode": "regex", "controls": {"emit": false}},
      {"scope": {"dag_id": "audit_dag"}, "controls": {"include_full_task_info": true}},
      {"scope": {}, "controls": {"include_source_code": false}}
    ]


``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` environment variable is an equivalent.

In the example above:

- ``HttpOperator`` task events are suppressed entirely for all DAGs.
- ``PythonOperator`` tasks emit events but without source code (all DAGs).
- Tasks in ``expensive_dag`` skip dataset extraction — except ``send_report``, which overrides this
  with a more-specific task-scoped rule and extracts normally.
- ``sensitive_task`` in ``my_dag`` runs its extractor normally but skips the
  ``HookLineageCollector`` fallback.
- DAG-level events for ``reporting_dag`` are suppressed; task events are still emitted.
- Both task events and DAG-level events for ``full_control_dag`` are suppressed — except
  ``critical_task``, which re-enables task event emission via a task-scoped rule.
- All events for any DAG whose ID starts with ``staging_`` are suppressed (regex match).
- Full task info is included in all task events for ``audit_dag``.
- Source code is disabled globally for all operators (``PythonOperator`` was already covered above,
  but the global rule applies to ``BashOperator`` and any other operator with source code).

**Audit logging**: whenever a control flag is non-default (suppressed or enabled), an INFO-level log
is emitted identifying the field, the event context, and the exact rule that caused the change.
This provides a clear audit trail for operational troubleshooting.

.. _emission_policy_authoring:openlineage:

Per-Dag / per-task authoring overrides (``extend_global_openlineage_emission_policy``)
--------------------------------------------------------------------------------------

Dag authors can override most ``emission_policy`` flags directly in Dag code, without touching
Airflow configuration, via
:func:`~airflow.providers.openlineage.api.emission_policy.extend_global_openlineage_emission_policy`. This sits
**above** ``emission_policy`` conf rules in the resolution stack — the value provided in code
wins **unless** the matching conf rule is marked with ``"locked": true``.

.. code-block:: python

    from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy

    with DAG("my_dag", ...) as dag:
        extract = PythonOperator(task_id="extract", ...)
        sensitive = PythonOperator(task_id="sensitive", ...)

    # Disable all events for one task
    extend_global_openlineage_emission_policy(sensitive, emit=False)

    # Disable source-code capture for every task in the Dag, re-enable for one
    extend_global_openlineage_emission_policy(dag, include_source_code=False)
    extend_global_openlineage_emission_policy(extract, include_source_code=True)

    # Suppress DAG-run events but keep task events
    extend_global_openlineage_emission_policy(dag, emit_dag_events=False)

Key semantics:

- **Resolution priority**: authoring flags > unlocked conf rules > built-in defaults.
- **DAG-or-task only — no global authoring scope.** ``extend_global_openlineage_emission_policy`` only accepts
  a single DAG, operator, or :class:`XComArg`. There is no API for "apply to every DAG in the
  deployment." Deployment-wide changes are an admin concern and belong in ``emission_policy``
  with an empty ``"scope": {}`` (see the :ref:`unified configuration<emission_policy_unified:openlineage>` above).
  Passing any other object type raises :class:`TypeError`.
- **Locked conf rules win**: a rule with ``"locked": true`` blocks authoring overrides for the
  field(s) it carries. The override is silently ignored and an INFO-level log records the lock
  hit. This is a floor lock — *any* matching conf rule that locks a field is honored, even if
  another (more specific) conf rule wins the value resolution.
- **DAG-level calls only propagate to existing tasks** — flags set on a Dag are pushed to the
  tasks present on the Dag at the moment the call is made. Tasks defined later (e.g. inside
  ``with DAG(...) as dag:`` before the operators are declared) will not inherit them. Call
  ``extend_global_openlineage_emission_policy(dag, ...)`` *after* defining the tasks, or set flags per task.
- **No unset API**: pass an explicit boolean to override; passing ``None`` means "not provided"
  and leaves any previously-stored value intact.
- **Legacy options cannot be locked**. ``"locked": true`` is an ``emission_policy`` feature.
  If you rely only on legacy options (see :ref:`emission_policy_legacy:openlineage` below),
  Dag authors can override them via ``extend_global_openlineage_emission_policy``. To enforce a real lock,
  move the constraint into ``emission_policy`` and add ``"locked": true``.

See the docstring of
:func:`~airflow.providers.openlineage.api.emission_policy.extend_global_openlineage_emission_policy`
for the full list of supported flags and examples.

.. _emission_policy_legacy:openlineage:

Legacy configuration options (deprecated)
-----------------------------------------

.. deprecated:: 2.18.0

    The legacy options ``disabled_for_operators``, ``disable_source_code``,
    ``include_full_task_info``, and ``selective_enable`` are superseded by
    ``emission_policy``. They continue to work for now but will be removed in a future version
    — migrate by translating them into the equivalent rules shown below.

Any active legacy options are **automatically translated** into equivalent ``emission_policy`` rules.
The translated rules are **prepended** before your explicit rules, so your explicit rules win within each priority tier
(last-wins within a tier). A ``DeprecationWarning`` is issued listing every translated option — migrate
your configuration to ``emission_policy`` exclusively to silence it.

The translation mapping is:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Legacy option
     - Translated to
   * - ``disabled_for_operators = some.Operator``
     - ``{"scope": {"operator": "some.Operator"}, "controls": {"emit": false}}`` (operator-scoped, one per entry)
   * - ``disable_source_code = True``
     - ``{"scope": {}, "controls": {"include_source_code": false}}`` (global rule)
   * - ``include_full_task_info = True``
     - ``{"scope": {}, "controls": {"include_full_task_info": true}}`` (global rule)
   * - ``selective_enable = True``
     - ``{"scope": {}, "controls": {"emit": false}}`` (global baseline) +
       ``{"scope": {"dag_id": "...", "task_id": "..."}, "controls": {"emit": true}}``
       injected at runtime for each task that called ``enable_lineage()``

**Full translation example.** Given this configuration:

.. code-block:: ini

    [openlineage]
    emission_policy        = [{"scope": {"dag_id": "my_dag"}, "controls": {"include_source_code": true}}]
    disabled_for_operators = airflow.providers.standard.operators.python.PythonOperator
    disable_source_code    = True
    include_full_task_info = True
    selective_enable       = True

The effective rule set evaluated at runtime becomes (legacy rules first, your rules last):

.. code-block:: text

    Tier: operator
      {"scope": {"operator": "airflow.providers.standard.operators.python.PythonOperator"},
       "controls": {"emit": false}}
      ↳ from disabled_for_operators

    Tier: global
      {"scope": {}, "controls": {"include_source_code": false}}
      ↳ from disable_source_code

      {"scope": {}, "controls": {"include_full_task_info": true}}
      ↳ from include_full_task_info

      {"scope": {}, "controls": {"emit": false}}
      ↳ from selective_enable (global baseline — all tasks off by default)

    Tier: task  (injected at runtime for each opted-in task)
      {"scope": {"dag_id": "my_dag", "task_id": "opted_in_task"}, "controls": {"emit": true}}
      ↳ from selective_enable + enable_lineage(task)

    Your explicit emission_policy rules (appended last, win within tier):
    Tier: dag
      {"scope": {"dag_id": "my_dag"}, "controls": {"include_source_code": true}}
      ↳ from emission_policy

Result for ``PythonOperator`` task in ``my_dag`` (task opted in via ``enable_lineage``):

- ``emit`` — task-tier opt-in rule sets ``true``, overriding the global ``false`` baseline.
- ``include_source_code`` — your explicit dag-tier rule overrides the global
  ``include_source_code=false`` from the translated legacy rule, because dag tier beats global tier.
- ``include_full_task_info`` — global ``true`` from the translated legacy rule.

.. note::

    Because ``disabled_for_operators`` translates to an **operator-tier** rule, overriding it from
    ``emission_policy`` requires an operator-tier rule with a higher list position (i.e., appearing
    later in the same ``emission_policy`` list). A global
    ``{"scope": {}, "controls": {"emit": true}}`` rule will *not* override it — operator tier beats
    global tier. To re-enable a specific operator that is listed in ``disabled_for_operators``, add
    an explicit operator rule to ``emission_policy``:

    .. code-block:: ini

        [openlineage]
        emission_policy        = [{"scope": {"operator": "my.pkg.MyOperator"}, "controls": {"emit": true}}]
        disabled_for_operators = my.pkg.MyOperator

    The translated legacy rule ``{"scope": {"operator": "my.pkg.MyOperator"}, "controls": {"emit": false}}``
    is prepended, but the explicit ``{"scope": {"operator": "my.pkg.MyOperator"}, "controls": {"emit": true}}``
    follows it in the same operator tier — last-wins, so the operator is re-enabled.
