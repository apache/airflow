# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Per-scope emission policy resolution for the OpenLineage provider.

The ``emission_policy`` Airflow configuration option (``[openlineage]`` section)
accepts a JSON array of rule objects.  Each rule has the shape::

    {
        "scope":      {<scope keys>},        # required, may be empty for "global"
        "match_mode": "exact" | "regex",     # optional, default "exact"
        "controls":   {<control flags>},     # required, must be non-empty
        "locked":     true | false           # optional, default false
    }

Top-level keys outside the four above are rejected with a WARNING and the rule
is skipped.

Scope keys (all optional inside ``scope``):

- ``operator`` — fully-qualified operator class name; matches task events for that
  operator type.
- ``dag_id`` — matches task and/or dag run events for all tasks / dag runs belonging
  to the named DAG.  Use ``emit_task_events`` / ``emit_dag_events`` to target one
  event type selectively.
- ``task_id`` — only valid alongside ``dag_id``; targets a specific task.
- *(empty ``scope: {}``)* — global default override; applies to every task and DAG event.

``operator`` cannot be combined with ``dag_id`` or ``task_id`` (scope is one of:
global, operator-only, dag-only, dag+task).  ``task_id`` without ``dag_id`` and
``operator`` combined with ``emit_dag_events`` (or ``task_id`` combined with
``emit_dag_events``) are also rejected with a WARNING.

``match_mode`` lives at the top level: ``"exact"`` (default) or ``"regex"``.
When set to ``"regex"``, every value inside ``scope`` (``dag_id``, ``task_id``,
``operator``) is treated as a ``re.fullmatch`` pattern.

Control flag keys (all optional inside ``controls``; the dict must be non-empty):

- ``emit`` — shorthand: disable ALL OpenLineage events in scope (both task and dag
  run events).  Default: ``true``.
- ``emit_task_events`` — disable task-level events only; takes precedence over
  ``emit`` for task event decisions.  Default: ``true``.
- ``emit_dag_events`` — disable dag-run-level events only; takes precedence over
  ``emit`` for dag event decisions.  Default: ``true``.
- ``extract_operator_metadata`` — whether to run operator-specific extractor-based metadata
  collection.  When ``true``, the extractor manager calls the operator's OpenLineage extractor
  (if registered), which may produce dataset inputs/outputs, job facets, run facets, and
  other operator-specific metadata.  When ``false``, the entire extraction pipeline is skipped
  and a minimal event is emitted.  Only meaningful for task events.  Default: ``true``.
- ``include_source_code`` — whether to include operator source code in the
  ``SourceCodeJobFacet`` for Python and Bash operators.  Only meaningful for task events
  when ``extract_operator_metadata`` is also ``true``.  Default: ``true``.
- ``hook_lineage`` — whether to use ``HookLineageCollector`` as a fallback when the
  extractor finds no inputs/outputs.  **Has no effect when ``extract_operator_metadata``
  is ``false``** — the entire extraction pipeline (including hook lineage) is skipped.
  Only meaningful for task events.  Default: ``true``.
- ``include_full_task_info`` — whether to include the full serialized operator state
  in the ``AirflowRunFacet``.  When ``false`` (default), only a curated subset of
  task attributes is sent.  Only meaningful for task events.  Default: ``false``.

``locked: true`` is an admin floor lock that prevents per-Dag / per-task authoring
overrides from changing the field(s) carried by this rule's ``controls`` dict.

Flag hierarchy
~~~~~~~~~~~~~~

Flags are not fully independent — some only take effect when a higher-level flag is
enabled:

- ``extract_operator_metadata: false`` skips the **entire** operator extraction pipeline.
  When this is ``false``, both ``include_source_code`` and ``hook_lineage`` have **no
  effect** regardless of their values, because the code paths they control are never
  reached.
- ``include_source_code`` only applies inside Python and Bash operator extractors; setting
  it to ``false`` on other operator types is a no-op.

Priority for **task events** (most specific tier wins; within a tier, last matching
rule wins):

1. ``dag_id`` + ``task_id``
2. ``dag_id``
3. ``operator``
4. Global (no match keys)
5. Built-in defaults

For ``emit`` at the task level: ``emit_task_events`` beats ``emit`` within the same
rule.

Priority for **DAG run events**:

1. ``dag_id`` rule (using ``emit_dag_events`` or ``emit``)
2. Global (no match keys)
3. Built-in defaults

For ``emit`` at the dag level: ``emit_dag_events`` beats ``emit`` within the same
rule.

Legacy config translation
-------------------------

Any active legacy config option (``disabled_for_operators``, ``disable_source_code``,
``include_full_task_info``, ``selective_enable``) is **always** translated into equivalent
``emission_policy`` rules, regardless of whether ``emission_policy`` itself is set. The
translated rules are prepended before any user-provided rules, so user rules win within
each priority tier (last-wins). A ``DeprecationWarning`` is issued listing every translated
option — silence it by migrating those options into ``emission_policy`` exclusively.

When no legacy option is active and ``emission_policy`` is empty, the resolver returns
the built-in defaults with no warnings.

Audit logging
-------------

Every resolved field that is non-default — whether from a rule or from a legacy
translation — is logged at INFO level, identifying the field, the event context,
and the exact rule that caused the change.
"""

from __future__ import annotations

import logging
import re
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import Param
from airflow.providers.openlineage import conf as _ol_conf

if TYPE_CHECKING:
    from airflow.providers.openlineage.utils.utils import AnyOperator

log = logging.getLogger(__name__)

# Control flag names — the keys that may appear inside a rule's ``controls`` dict.
EMIT = "emit"
EMIT_TASK_EVENTS = "emit_task_events"
EMIT_DAG_EVENTS = "emit_dag_events"
EXTRACT_OPERATOR_METADATA = "extract_operator_metadata"
INCLUDE_SOURCE_CODE = "include_source_code"
HOOK_LINEAGE = "hook_lineage"
INCLUDE_FULL_TASK_INFO = "include_full_task_info"

# Scope keys — the keys that may appear inside a rule's ``scope`` dict.
SCOPE_DAG_ID = "dag_id"
SCOPE_TASK_ID = "task_id"
SCOPE_OPERATOR = "operator"

# Top-level rule keys.
RULE_SCOPE = "scope"
RULE_CONTROLS = "controls"
RULE_MATCH_MODE = "match_mode"
RULE_LOCKED = "locked"

# Param names used by the authoring API to store flow-control flags on operator / DAG
# objects. Kept in this module (rather than the api module) so the resolver can read
# them without a circular import.
OL_EMISSION_POLICY_PARAM = "_openlineage_emission_policy"

# Schema enforcement: only these keys may appear at each level.  Unknown keys cause
# the rule to be skipped with a WARNING (catches typos like {"scope": {"dgg_id": "x"}}).
_ALLOWED_TOP_LEVEL_KEYS: frozenset[str] = frozenset({RULE_SCOPE, RULE_MATCH_MODE, RULE_CONTROLS, RULE_LOCKED})
_ALLOWED_SCOPE_KEYS: frozenset[str] = frozenset({SCOPE_DAG_ID, SCOPE_TASK_ID, SCOPE_OPERATOR})
_ALLOWED_CONTROL_KEYS: frozenset[str] = frozenset(
    {
        EMIT,
        EMIT_TASK_EVENTS,
        EMIT_DAG_EVENTS,
        EXTRACT_OPERATOR_METADATA,
        INCLUDE_SOURCE_CODE,
        HOOK_LINEAGE,
        INCLUDE_FULL_TASK_INFO,
    }
)

# Fields scanned for ``locked: true`` on task-event resolution.
# ``emit`` is intentionally absent: emit-locking is driven by either ``emit`` or
# ``emit_task_events`` appearing in a locked rule (handled separately in
# :func:`_compute_locked_task_fields`), so listing it here would double-count.
_LOCKABLE_TASK_FIELDS: tuple[str, ...] = (
    EXTRACT_OPERATOR_METADATA,
    INCLUDE_SOURCE_CODE,
    HOOK_LINEAGE,
    INCLUDE_FULL_TASK_INFO,
)

# Authoring flag classification: which keys are relevant for task-level vs DAG-run
# resolution.  Used by the authoring API to split a single call into the two stored
# param dicts.
_TASK_FLAG_KEYS: frozenset[str] = frozenset(
    {
        EMIT,
        EMIT_TASK_EVENTS,
        EXTRACT_OPERATOR_METADATA,
        INCLUDE_SOURCE_CODE,
        HOOK_LINEAGE,
        INCLUDE_FULL_TASK_INFO,
    }
)
_DAG_FLAG_KEYS: frozenset[str] = frozenset({EMIT, EMIT_DAG_EVENTS})


@dataclass(frozen=True)
class EmissionPolicy:
    """Resolved emission policy for a specific event context."""

    emit: bool
    extract_operator_metadata: bool
    include_source_code: bool
    hook_lineage: bool
    include_full_task_info: bool

    @classmethod
    def defaults(cls) -> EmissionPolicy:
        """Return the default policy (all controls at their built-in defaults)."""
        return cls(
            emit=True,
            extract_operator_metadata=True,
            include_source_code=True,
            hook_lineage=True,
            include_full_task_info=False,
        )


@dataclass(frozen=True)
class Rule:
    """
    A single, fully-validated ``emission_policy`` rule.

    Instances are produced exclusively by :func:`_parse_rule`, so holding a ``Rule``
    is itself the "this passed schema validation" guarantee — downstream resolution
    never re-validates. ``scope`` / ``controls`` are the raw (validated) sub-dicts.
    """

    scope: dict[str, str]
    controls: dict[str, bool]
    match_mode: str = "exact"
    locked: bool = False

    @property
    def has_dag(self) -> bool:
        return SCOPE_DAG_ID in self.scope

    @property
    def has_task(self) -> bool:
        return SCOPE_TASK_ID in self.scope

    @property
    def has_op(self) -> bool:
        return SCOPE_OPERATOR in self.scope


def _matches(pattern: str, value: str, match_mode: str) -> bool:
    """Return ``True`` if *value* matches *pattern* according to *match_mode*."""
    if match_mode == "regex":
        return bool(re.fullmatch(pattern, value))
    return pattern == value


def _read_param(obj: object, param_name: str) -> dict[str, bool]:
    """Read the flags dict stored at *param_name* on *obj*, or ``{}`` if absent."""
    val = getattr(obj, "params", {}).get(param_name)
    if val is None:
        return {}
    if isinstance(val, Param):
        val = val.value
    return val if isinstance(val, dict) else {}


def _merge_param(obj: object, param_name: str, new_flags: dict[str, bool]) -> None:
    """
    Merge *new_flags* into the ``param_name`` param on *obj*, creating it if absent.

    The public entry point
    (:func:`~airflow.providers.openlineage.api.emission_policy.extend_global_openlineage_emission_policy`)
    validates that *obj* is a DAG or task-like object before reaching here, so a missing
    ``params`` attribute indicates a programmer error (e.g. an incorrectly-mocked object
    in tests) — we raise loud rather than silently dropping flags.
    """
    params = getattr(obj, "params", None)
    if params is None:
        raise TypeError(
            f"extend_global_openlineage_emission_policy: {type(obj).__name__} instance has no "
            "'params' attribute — cannot store flow-control flags. "
            "Pass a DAG, an Operator, or an XComArg."
        )
    existing = params.get(param_name)
    if existing is not None:
        existing_val = existing.value if isinstance(existing, Param) else existing
        if isinstance(existing_val, dict):
            new_flags = {**existing_val, **new_flags}
    params[param_name] = Param(new_flags)


def _audit_log_conf_field(field: str, value: bool, context: str, source: Rule) -> None:
    """Log the winning conf value for a field whenever any rule touched it."""
    log.info(
        "OpenLineage emission policy: '%s' %s for %s by %r",
        field,
        "enabled" if value else "disabled",
        context,
        source,
    )


def _audit_log_authoring_updates(
    field_changes: dict[str, bool],
    context: str,
) -> None:
    """Audit-log authoring overrides that actually change the resolved policy value."""
    for field, new_value in field_changes.items():
        log.info(
            "OpenLineage emission policy: '%s' %s for %s "
            "by manual `extend_global_openlineage_emission_policy` call.",
            field,
            "enabled" if new_value else "disabled",
            context,
        )


def _parse_rule(rule: object) -> Rule | None:
    """
    Parse and validate a raw rule dict into a :class:`Rule`; warn and return ``None`` otherwise.

    A valid rule has exactly these top-level keys: ``scope`` (dict, possibly empty),
    ``controls`` (non-empty dict), and the optionals ``match_mode`` / ``locked``.
    Unknown keys at any level cause the rule to be skipped — that catches user
    typos like ``{"scope": {"dgg_id": "x"}}``.

    This is the single validation gate: it runs once at config-read time (see
    :func:`_parse_user_rules`), so the resolution hot path never re-validates.
    """
    if not isinstance(rule, dict):
        log.warning("OpenLineage emission_policy entry is not a dict: %r; ignoring.", rule)
        return None

    unknown_top = set(rule) - _ALLOWED_TOP_LEVEL_KEYS
    if unknown_top:
        log.warning(
            "OpenLineage emission_policy rule has unknown top-level key(s) %s (allowed: %s); ignoring: %r",
            sorted(unknown_top),
            sorted(_ALLOWED_TOP_LEVEL_KEYS),
            rule,
        )
        return None

    if RULE_SCOPE not in rule:
        log.warning(
            "OpenLineage emission_policy rule is missing required 'scope' key (use 'scope': {} for global); ignoring: %r",
            rule,
        )
        return None
    scope = rule[RULE_SCOPE]
    if not isinstance(scope, dict):
        log.warning(
            "OpenLineage emission_policy rule 'scope' must be a dict (use {} for global), got %r; ignoring: %r",
            type(scope).__name__,
            rule,
        )
        return None
    unknown_scope = set(scope) - _ALLOWED_SCOPE_KEYS
    if unknown_scope:
        log.warning(
            "OpenLineage emission_policy rule 'scope' has unknown key(s) %s (allowed: %s); ignoring: %r",
            sorted(unknown_scope),
            sorted(_ALLOWED_SCOPE_KEYS),
            rule,
        )
        return None
    has_dag = SCOPE_DAG_ID in scope
    has_task = SCOPE_TASK_ID in scope
    has_op = SCOPE_OPERATOR in scope
    if has_task and not has_dag:
        log.warning(
            "OpenLineage emission_policy rule scope has 'task_id' without 'dag_id'; ignoring: %r",
            rule,
        )
        return None
    if has_op and (has_dag or has_task):
        log.warning(
            "OpenLineage emission_policy rule scope combines 'operator' with 'dag_id'/'task_id' "
            "(must be exactly one of: global, operator-only, dag-only, dag+task); ignoring: %r",
            rule,
        )
        return None

    if RULE_CONTROLS not in rule:
        log.warning(
            "OpenLineage emission_policy rule is missing required 'controls' dict; ignoring: %r",
            rule,
        )
        return None
    controls = rule[RULE_CONTROLS]
    if not isinstance(controls, dict):
        log.warning(
            "OpenLineage emission_policy rule 'controls' must be a dict, got %r; ignoring: %r",
            type(controls).__name__,
            rule,
        )
        return None
    if not controls:
        log.warning(
            "OpenLineage emission_policy rule has empty 'controls' dict "
            "(at least one control flag is required); ignoring: %r",
            rule,
        )
        return None
    unknown_controls = set(controls) - _ALLOWED_CONTROL_KEYS
    if unknown_controls:
        log.warning(
            "OpenLineage emission_policy rule 'controls' has unknown key(s) %s (allowed: %s); ignoring: %r",
            sorted(unknown_controls),
            sorted(_ALLOWED_CONTROL_KEYS),
            rule,
        )
        return None
    for k, v in controls.items():
        if not isinstance(v, bool):
            log.warning(
                "OpenLineage emission_policy rule 'controls.%s' must be bool, got %r; ignoring: %r",
                k,
                v,
                rule,
            )
            return None

    if has_op and EMIT_DAG_EVENTS in controls:
        log.warning(
            "OpenLineage emission_policy rule has scope 'operator' with controls 'emit_dag_events' which is meaningless "
            "(operators are not associated with DAG run events); ignoring: %r",
            rule,
        )
        return None
    if has_task and EMIT_DAG_EVENTS in controls:
        log.warning(
            "OpenLineage emission_policy rule has scope 'task_id' with controls 'emit_dag_events' which is meaningless "
            "(task-specific rules do not target DAG run events); ignoring: %r",
            rule,
        )
        return None

    match_mode = rule.get(RULE_MATCH_MODE, "exact")
    if match_mode not in ("exact", "regex"):
        log.warning(
            "OpenLineage emission_policy rule has invalid 'match_mode' %r (must be 'exact' or 'regex'); ignoring: %r",
            match_mode,
            rule,
        )
        return None
    if match_mode == "regex":
        for key in (SCOPE_DAG_ID, SCOPE_TASK_ID, SCOPE_OPERATOR):
            if key in scope:
                try:
                    re.compile(scope[key])
                except re.error as exc:
                    log.warning(
                        "OpenLineage emission_policy rule scope.'%s' pattern %r is not a valid regex (%s); ignoring: %r",
                        key,
                        scope[key],
                        exc,
                        rule,
                    )
                    return None

    if RULE_LOCKED in rule and not isinstance(rule[RULE_LOCKED], bool):
        log.warning(
            "OpenLineage emission_policy rule 'locked' must be bool, got %r; ignoring: %r",
            rule[RULE_LOCKED],
            rule,
        )
        return None
    # Under the nested schema every allowed control key is lockable (validation already
    # requires non-empty controls with keys from the allowed set), so there is no
    # "lock with no lockable field" warning case to raise.

    return Rule(
        scope=rule[RULE_SCOPE],
        controls=rule[RULE_CONTROLS],
        match_mode=match_mode,
        locked=rule.get(RULE_LOCKED, False),
    )


@_ol_conf.cache
def _parse_user_rules() -> tuple[Rule, ...]:
    """
    Parse and cache the user-configured ``emission_policy`` rules.

    Validation runs here exactly once per config value (``conf.emission_policy()`` is
    itself cached), so task / dag resolution operates on pre-validated :class:`Rule`
    objects without re-validating on every event.
    """
    return tuple(parsed for raw in _ol_conf.emission_policy() if (parsed := _parse_rule(raw)) is not None)


def _classify_task_rules(
    rules: Sequence[Rule],
    fqcn: str,
    dag_id: str,
    task_id: str,
) -> tuple[list[Rule], list[Rule], list[Rule], list[Rule]]:
    """
    Classify pre-validated rules into the four priority tiers for task resolution.

    Returns ``(task_rules, dag_rules, operator_rules, global_rules)``.
    """
    task_rules: list[Rule] = []
    dag_rules: list[Rule] = []
    operator_rules: list[Rule] = []
    global_rules: list[Rule] = []

    for rule in rules:
        scope = rule.scope
        match_mode = rule.match_mode

        if rule.has_dag and rule.has_task:
            if _matches(scope[SCOPE_DAG_ID], dag_id, match_mode) and _matches(
                scope[SCOPE_TASK_ID], task_id, match_mode
            ):
                task_rules.append(rule)
        elif rule.has_dag:
            if _matches(scope[SCOPE_DAG_ID], dag_id, match_mode):
                dag_rules.append(rule)
        elif rule.has_op:
            if _matches(scope[SCOPE_OPERATOR], fqcn, match_mode):
                operator_rules.append(rule)
        else:
            global_rules.append(rule)

    return task_rules, dag_rules, operator_rules, global_rules


def _classify_dag_rules(
    rules: Sequence[Rule],
    dag_id: str,
) -> tuple[list[Rule], list[Rule]]:
    """
    Classify pre-validated rules into the two priority tiers for dag event resolution.

    Only rules with empty ``scope`` (global) and rules with exactly ``{dag_id}``
    contribute to dag-event resolution. Operator-scoped and task-scoped rules
    never affect dag-run events.

    Returns ``(dag_rules, global_rules)``.
    """
    dag_rules: list[Rule] = []
    global_rules: list[Rule] = []

    for rule in rules:
        if rule.has_dag and not rule.has_task and not rule.has_op:
            if _matches(rule.scope[SCOPE_DAG_ID], dag_id, rule.match_mode):
                dag_rules.append(rule)
        elif not (rule.has_dag or rule.has_task or rule.has_op):  # global = empty scope dict
            global_rules.append(rule)

    return dag_rules, global_rules


def _walk_tiers(
    tiers: list[list[Rule]],
    extract_value: Callable[[dict[str, bool]], bool | None],
    field_label: str,
    default: bool,
) -> tuple[bool, Rule | None]:
    """
    Walk priority tiers from most-specific to least-specific, last-wins within each tier.

    *extract_value* maps a rule's ``controls`` dict to ``bool | None``; ``None`` means
    "this rule does not cover this field".  A contradiction WARNING is emitted when two
    rules in the same tier produce different non-``None`` values.  Returns
    ``(default, None)`` when no rule in any tier matches.
    """
    for tier_rules in tiers:
        value: bool | None = None
        winning_rule: Rule | None = None
        for rule in tier_rules:
            new_v = extract_value(rule.controls)
            if new_v is not None:
                if winning_rule is not None and value != new_v:
                    log.warning(
                        "OpenLineage emission_policy: field '%s' has contradictory values in the same "
                        "priority tier (using last value %r); conflicting rules:"
                        "\n - %r"
                        "\n - %r",
                        field_label,
                        new_v,
                        winning_rule,
                        rule,
                    )
                value = new_v
                winning_rule = rule
        if value is not None:
            return value, winning_rule
    return default, None


def _resolve_field_with_source(
    tiers: list[list[Rule]], field: str, default: bool
) -> tuple[bool, Rule | None]:
    """Walk tiers for a single boolean *field* in ``controls``."""
    return _walk_tiers(tiers, lambda c: c.get(field), field, default)


def _resolve_emit_with_source(
    tiers: list[list[Rule]],
    scope: str,
    default: bool,
) -> tuple[bool, Rule | None]:
    """
    Resolve the effective ``emit`` decision for the given *scope*.

    ``scope`` must be ``"task"`` or ``"dag"``.  Within each rule, the
    scope-specific key (``emit_task_events`` / ``emit_dag_events``) takes
    precedence over the generic ``emit`` shorthand.
    """
    specific_key = EMIT_TASK_EVENTS if scope == "task" else EMIT_DAG_EVENTS

    def _extract(c: dict[str, bool]) -> bool | None:
        if specific_key in c:
            return c[specific_key]
        return c.get(EMIT)

    return _walk_tiers(tiers, _extract, f"emit ({scope})", default)


def _synthesize_legacy_task_rules(
    operator: AnyOperator,
    dag_id: str,
    task_id: str,
) -> list[Rule]:
    """
    Translate legacy config options into equivalent ``emission_policy`` rules for task resolution.

    The returned rules are intended to be **prepended** to the user-provided rules so that
    user rules (appended later) win within each tier (last-wins within a tier). They are
    constructed directly (trusted, no validation path).
    """
    rules: list[Rule] = []

    # disabled_for_operators -> operator-scope emit:false rules
    for fqcn in _ol_conf.disabled_operators():
        rules.append(Rule(scope={SCOPE_OPERATOR: fqcn}, controls={EMIT: False}))

    # disable_source_code -> global include_source_code:false
    if not _ol_conf.is_source_enabled():
        rules.append(Rule(scope={}, controls={INCLUDE_SOURCE_CODE: False}))

    # include_full_task_info -> global include_full_task_info:true (only when non-default True)
    if _ol_conf.include_full_task_info():
        rules.append(Rule(scope={}, controls={INCLUDE_FULL_TASK_INFO: True}))

    # selective_enable -> global emit:false baseline + per-task opt-in rule
    if _ol_conf.selective_enable():
        rules.append(Rule(scope={}, controls={EMIT: False}))  # global baseline
        try:
            from airflow.providers.openlineage.utils.selective_enable import is_task_lineage_enabled

            if is_task_lineage_enabled(operator):
                # Task is explicitly opted in -> task-tier rule that overrides the global baseline.
                rules.append(
                    Rule(scope={SCOPE_DAG_ID: dag_id, SCOPE_TASK_ID: task_id}, controls={EMIT: True})
                )
        except Exception:
            # Non-standard operator types may not expose params correctly. Surface at debug
            # level so the failure is discoverable without spamming production logs.
            log.debug(
                "OpenLineage selective_enable translation: is_task_lineage_enabled() failed for task '%s'",
                task_id,
                exc_info=True,
            )

    return rules


def _synthesize_legacy_dag_rules(
    dag_id: str,
    dag: object | None,
) -> list[Rule]:
    """Translate legacy config options into equivalent ``emission_policy`` rules for dag event resolution."""
    rules: list[Rule] = []

    if _ol_conf.selective_enable() and dag is not None:
        rules.append(Rule(scope={}, controls={EMIT: False}))  # global baseline
        try:
            from airflow.providers.openlineage.utils.selective_enable import is_dag_lineage_enabled

            if is_dag_lineage_enabled(dag):  # type: ignore[arg-type]
                rules.append(Rule(scope={SCOPE_DAG_ID: dag_id}, controls={EMIT: True}))
        except Exception:
            log.debug(
                "OpenLineage selective_enable translation: is_dag_lineage_enabled() failed for dag '%s'",
                dag_id,
                exc_info=True,
            )

    return rules


def _warn_legacy_with_emission_policy(legacy_rules: list[Rule], scope: str) -> None:
    """
    Issue a DeprecationWarning listing legacy options that were translated to rules.

    *scope* is ``"task"`` or ``"dag"``: a task-scope warning enumerates every legacy
    option (``disabled_for_operators``, ``disable_source_code``,
    ``include_full_task_info``, ``selective_enable``); a dag-scope warning only
    mentions ``selective_enable`` because the other three are task-only.
    """
    if not legacy_rules:
        return

    parts: list[str] = []
    if scope == "task":
        if _ol_conf.disabled_operators():
            translated = [
                {RULE_SCOPE: {SCOPE_OPERATOR: f}, RULE_CONTROLS: {EMIT: False}}
                for f in _ol_conf.disabled_operators()
            ]
            parts.append(f" - disabled_for_operators -> {translated}")
        if not _ol_conf.is_source_enabled():
            parts.append(
                ' - disable_source_code -> [{"scope": {}, "controls": {"include_source_code": false}}]'
            )
        if _ol_conf.include_full_task_info():
            parts.append(
                ' - include_full_task_info -> [{"scope": {}, "controls": {"include_full_task_info": true}}]'
            )
        if _ol_conf.selective_enable():
            parts.append(
                " - selective_enable -> "
                '[{"scope": {}, "controls": {"emit": false}}] (global baseline) '
                "+ per-task opt-in rules injected at runtime from enable_lineage()/disable_lineage()"
            )
    else:  # dag scope: only selective_enable contributes legacy dag-event rules
        if not _ol_conf.selective_enable():
            return
        parts.append(
            " - selective_enable -> "
            '[{"scope": {}, "controls": {"emit": false}}] (global baseline) '
            "+ dag opt-in rules injected at runtime from enable_lineage()/disable_lineage()"
        )

    if not parts:
        return

    warnings.warn(
        "[openlineage] One or more legacy config options are set and have been translated "
        "into equivalent 'emission_policy' rules:\n" + "\n".join(parts) + "\n"
        "Migrate to 'emission_policy' exclusively to silence this warning. "
        "These legacy options will be removed in a future version.",
        AirflowProviderDeprecationWarning,
        stacklevel=4,
    )


def _compute_locked_fields(
    rules: list[Rule],
    emit_key: str,
    extra_lockable: Sequence[str] = (),
) -> frozenset[str]:
    """
    Return the set of fields locked by *any* rule in *rules* that carries ``locked: true``.

    **Floor-lock semantics**: a field is locked if *any* matching rule marks it locked,
    regardless of whether that rule wins the value race.

    *emit_key* is the scope-specific emit control (``emit_task_events`` or
    ``emit_dag_events``); both it and the generic ``emit`` shorthand lock the ``"emit"``
    policy field.  *extra_lockable* lists additional fields to scan (task-event only).
    """
    locked: set[str] = set()
    for rule in rules:
        if not rule.locked:
            continue
        controls = rule.controls
        if emit_key in controls or EMIT in controls:
            locked.add(EMIT)
        for field in extra_lockable:
            if field in controls:
                locked.add(field)
    return frozenset(locked)


def _compute_locked_task_fields(
    task_rules: list[Rule],
    dag_rules: list[Rule],
    operator_rules: list[Rule],
    global_rules: list[Rule],
) -> frozenset[str]:
    """Compute the set of task-event fields locked by any matching conf rule."""
    return _compute_locked_fields(
        task_rules + dag_rules + operator_rules + global_rules,
        EMIT_TASK_EVENTS,
        _LOCKABLE_TASK_FIELDS,
    )


def _apply_authoring_overrides(
    config: EmissionPolicy,
    locked_fields: frozenset[str],
    flags: dict[str, bool],
    emit_key: str,
    context: str,
    extra_fields: tuple[str, ...] = (),
) -> EmissionPolicy:
    """
    Core of the authoring-override merge: apply *flags* onto *config*, skipping locked fields.

    *emit_key* is the scope-specific emit control (``emit_task_events`` for tasks,
    ``emit_dag_events`` for DAG events).  *extra_fields* lists additional lockable
    fields that the task path carries but the DAG path does not.
    """
    updates: dict[str, bool] = {}

    effective_emit: bool | None = flags.get(emit_key, flags.get(EMIT))
    if effective_emit is not None:
        if EMIT in locked_fields:
            log.warning(
                "OpenLineage emission_policy: extend_global_openlineage_emission_policy call for 'emit' on %s"
                " has no effect — locked by conf rule at value %r",
                context,
                config.emit,
            )
        else:
            updates[EMIT] = effective_emit

    for field in extra_fields:
        if field not in flags:
            continue
        if field in locked_fields:
            log.warning(
                "OpenLineage emission_policy: extend_global_openlineage_emission_policy call for '%s' on %s"
                " has no effect — locked by conf rule at value %r",
                field,
                context,
                getattr(config, field),
            )
        else:
            updates[field] = flags[field]

    changed = {k: v for k, v in updates.items() if getattr(config, k) != v}
    if not changed:
        return config

    _audit_log_authoring_updates(changed, context)
    return replace(config, **changed)


def _extend_policy_with_task_authoring(
    config: EmissionPolicy,
    locked_fields: frozenset[str],
    operator: AnyOperator,
    context: str,
) -> EmissionPolicy:
    """
    Apply per-task authoring flags on top of *config*.

    Authoring flags are stored on operator objects by
    :func:`~airflow.providers.openlineage.api.emission_policy.extend_global_openlineage_emission_policy`.
    Fields listed in *locked_fields* are protected — attempts to override them are
    logged at WARNING level and silently ignored. ``emit_task_events`` in the authoring
    flags takes precedence over ``emit`` for the task ``emit`` field, mirroring the
    same precedence used in conf rule resolution.
    """
    flags = _read_param(operator, OL_EMISSION_POLICY_PARAM)
    if not flags:
        return config
    return _apply_authoring_overrides(
        config,
        locked_fields,
        flags,
        EMIT_TASK_EVENTS,
        context,
        extra_fields=(EXTRACT_OPERATOR_METADATA, INCLUDE_SOURCE_CODE, HOOK_LINEAGE, INCLUDE_FULL_TASK_INFO),
    )


def _extend_policy_with_dag_authoring(
    config: EmissionPolicy,
    locked_fields: frozenset[str],
    dag: object,
    context: str,
) -> EmissionPolicy:
    """
    Apply per-DAG authoring flags on top of *config* for dag-run events.

    ``emit_dag_events`` in the authoring flags takes precedence over ``emit``,
    mirroring conf rule resolution.
    """
    flags = _read_param(dag, OL_EMISSION_POLICY_PARAM)
    if not flags:
        return config
    return _apply_authoring_overrides(config, locked_fields, flags, EMIT_DAG_EVENTS, context)


def _resolve_task_policy_from_conf_only(
    rules: Sequence[Rule],
    fqcn: str,
    dag_id: str,
    task_id: str,
) -> tuple[EmissionPolicy, frozenset[str]]:
    """
    Pure, deterministic conf resolution for task events.

    Takes the already-parsed, pre-validated *rules* and a pre-computed *fqcn* string.
    Rules are validated once at config-read time (:func:`_parse_user_rules`), so this
    path performs no re-validation.
    """
    task_rules, dag_rules, operator_rules, global_rules = _classify_task_rules(rules, fqcn, dag_id, task_id)

    defaults = EmissionPolicy.defaults()
    tiers = [task_rules, dag_rules, operator_rules, global_rules]
    context = f"task '{task_id}' in dag '{dag_id}'"

    emit, emit_rule = _resolve_emit_with_source(tiers, "task", defaults.emit)
    extract_operator_metadata, em_rule = _resolve_field_with_source(
        tiers, EXTRACT_OPERATOR_METADATA, defaults.extract_operator_metadata
    )
    include_source_code, isc_rule = _resolve_field_with_source(
        tiers, INCLUDE_SOURCE_CODE, defaults.include_source_code
    )
    hook_lineage, hl_rule = _resolve_field_with_source(tiers, HOOK_LINEAGE, defaults.hook_lineage)
    include_full_task_info, ift_rule = _resolve_field_with_source(
        tiers, INCLUDE_FULL_TASK_INFO, defaults.include_full_task_info
    )

    if emit_rule is not None:
        _audit_log_conf_field(EMIT, emit, context, emit_rule)
    if em_rule is not None:
        _audit_log_conf_field(EXTRACT_OPERATOR_METADATA, extract_operator_metadata, context, em_rule)
    if isc_rule is not None:
        _audit_log_conf_field(INCLUDE_SOURCE_CODE, include_source_code, context, isc_rule)
    if hl_rule is not None:
        _audit_log_conf_field(HOOK_LINEAGE, hook_lineage, context, hl_rule)
    if ift_rule is not None:
        _audit_log_conf_field(INCLUDE_FULL_TASK_INFO, include_full_task_info, context, ift_rule)

    locked_fields = _compute_locked_task_fields(task_rules, dag_rules, operator_rules, global_rules)

    return (
        EmissionPolicy(
            emit=emit,
            extract_operator_metadata=extract_operator_metadata,
            include_source_code=include_source_code,
            hook_lineage=hook_lineage,
            include_full_task_info=include_full_task_info,
        ),
        locked_fields,
    )


def _resolve_dag_policy_from_conf_only(
    rules: Sequence[Rule],
    dag_id: str,
) -> tuple[EmissionPolicy, frozenset[str]]:
    """
    Pure, deterministic conf resolution for dag-run events.

    Returns ``(config, locked_fields)``. Only ``emit`` is meaningful for dag events;
    all other fields in :class:`EmissionPolicy` carry their built-in defaults.

    **Floor-lock semantics** (see :func:`_compute_locked_task_fields`): a field
    is locked if *any* matching conf rule carries ``locked: true`` for that field.

    ``emission_policy()`` is cached at the conf layer, so calling this directly
    is cheap without any additional memoisation.
    """
    defaults = EmissionPolicy.defaults()
    dag_rules, global_rules = _classify_dag_rules(rules, dag_id)
    tiers = [dag_rules, global_rules]
    context = f"dag event '{dag_id}'"

    emit, emit_rule = _resolve_emit_with_source(tiers, "dag", defaults.emit)

    if emit_rule is not None:
        _audit_log_conf_field(EMIT, emit, context, emit_rule)

    locked_fields = _compute_locked_fields(dag_rules + global_rules, EMIT_DAG_EVENTS)

    return (
        EmissionPolicy(
            emit=emit,
            extract_operator_metadata=defaults.extract_operator_metadata,
            include_source_code=defaults.include_source_code,
            hook_lineage=defaults.hook_lineage,
            include_full_task_info=defaults.include_full_task_info,
        ),
        locked_fields,
    )


def resolve_task_emission_policy(
    operator: AnyOperator,
    dag_id: str,
    task_id: str,
) -> EmissionPolicy:
    """
    Resolve the emission policy for a task-level event.

    This is the **single authoritative entry point** for task event emission decisions.

    Any active legacy options (``disabled_for_operators``, ``disable_source_code``,
    ``include_full_task_info``, ``selective_enable``) are *always* translated into equivalent
    ``emission_policy`` rules, prepended to the user-provided rules so user rules win within
    each tier (last-wins). A ``DeprecationWarning`` is issued whenever a legacy option
    contributes a rule — silence it by migrating those options into ``emission_policy``
    exclusively.

    Never raises: any failure (e.g. a misconfigured ``emission_policy``) is logged and the
    built-in defaults are returned, so a bad config degrades to "emit with defaults" rather
    than crashing the listener's task-notification path.

    :param operator: The Airflow operator/task object.
    :param dag_id: The DAG ID for this task instance.
    :param task_id: The task ID for this task instance.
    """
    try:
        context = f"task '{task_id}' in dag '{dag_id}'"
        user_rules = _parse_user_rules()

        legacy_rules = _synthesize_legacy_task_rules(operator, dag_id, task_id)
        _warn_legacy_with_emission_policy(legacy_rules, scope="task")

        all_rules = list(legacy_rules) + list(user_rules)
        from airflow.providers.openlineage.utils.utils import get_fully_qualified_class_name

        fqcn = get_fully_qualified_class_name(operator)
        config, locked_fields = _resolve_task_policy_from_conf_only(all_rules, fqcn, dag_id, task_id)
        return _extend_policy_with_task_authoring(config, locked_fields, operator, context)
    except Exception as err:
        # This runs in the scheduler/listener hot path and must never raise — any failure
        # (a misconfigured ``emission_policy``, or even a deprecation warning promoted to an
        # error via ``-W error``) degrades to "emit with built-in defaults" rather than
        # breaking task-event emission.
        log.warning(
            "Failed to resolve OpenLineage emission policy for task `%s` in dag `%s`; "
            "emitting with default controls. Error: %s",
            task_id,
            dag_id,
            err,
        )
        log.debug("Exception details:", exc_info=True)
        return EmissionPolicy.defaults()


def resolve_dag_emission_policy(dag_id: str, dag: object | None = None) -> EmissionPolicy:
    """
    Resolve the emission policy for a DAG-level event.

    Returns a full :class:`EmissionPolicy` for a uniform resolver API across task and
    DAG scopes. Today only ``emit`` governs DAG-run events; the remaining fields
    (``extract_operator_metadata``, ``include_source_code``, ``hook_lineage``,
    ``include_full_task_info``) are reserved — they carry their built-in defaults and
    are not yet meaningful at DAG scope (no extraction happens at the DAG level). The
    unified return type keeps room for future DAG-level controls (e.g. terminal-event
    filtering) without re-widening the API.

    Any active legacy options (currently only ``selective_enable`` affects DAG events)
    are *always* translated into equivalent ``emission_policy`` rules, prepended to
    the user-provided rules so user rules win within each tier (last-wins). A
    ``DeprecationWarning`` is issued whenever a legacy option contributes a rule.

    Never raises: any failure (e.g. a misconfigured ``emission_policy``) is logged and the
    built-in defaults are returned, so a bad config degrades to "emit with defaults" rather
    than crashing the listener's dag-run-notification path.

    :param dag_id: The DAG ID for the dag run event.
    :param dag: Optional DAG object used for the ``selective_enable`` check / translation.
    """
    try:
        context = f"dag event '{dag_id}'"
        user_rules = _parse_user_rules()

        legacy_rules = _synthesize_legacy_dag_rules(dag_id, dag)
        _warn_legacy_with_emission_policy(legacy_rules, scope="dag")

        all_rules = list(legacy_rules) + list(user_rules)
        config, locked_fields = _resolve_dag_policy_from_conf_only(all_rules, dag_id)

        if dag is not None:
            config = _extend_policy_with_dag_authoring(config, locked_fields, dag, context)
        return config
    except Exception as err:
        # This runs in the scheduler/listener hot path and must never raise — any failure
        # (a misconfigured ``emission_policy``, or even a deprecation warning promoted to an
        # error via ``-W error``) degrades to "emit with built-in defaults" rather than
        # breaking dag-run-event emission.
        log.warning(
            "Failed to resolve OpenLineage emission policy for dag `%s`;"
            " emitting with default controls. Error: %s",
            dag_id,
            err,
        )
        log.debug("Exception details:", exc_info=True)
        return EmissionPolicy.defaults()
