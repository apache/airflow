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
"""SQL data-quality toolset for LLMDataQualityOperator."""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

try:
    from airflow.providers.common.ai.utils.dataquality.validation import ValidatorRegistry, default_registry
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import ToolsetTool

from airflow.providers.common.ai.toolsets.dataquality.base import _PASSTHROUGH_VALIDATOR, BaseDQToolset
from airflow.providers.common.ai.utils.dataquality.models import RowLevelResult

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

_log = logging.getLogger(__name__)

# JSON Schemas for the three SQL-DQ-specific tools.
_LIST_VALIDATORS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {},
}

_APPLY_VALIDATOR_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "check_name": {"type": "string", "description": "Name of the DQ check being evaluated."},
        "value": {"description": "Metric value returned by the SQL query for this check."},
        "validator_name": {
            "type": "string",
            "description": "Registered validator name (from list_validators). Pass 'none' to skip.",
        },
        "validator_args": {
            "type": "object",
            "description": 'Keyword arguments for the validator factory (e.g. {"max_pct": 0.05}).',
        },
    },
    "required": ["check_name", "value", "validator_name", "validator_args"],
}

_ROW_LEVEL_SAMPLE_LIMIT = 20


class SQLDQToolset(BaseDQToolset):
    """
    Data-quality toolset for SQL-based checks.

    Provides three tools on top of
    :class:`~airflow.providers.common.ai.toolsets.dataquality.base.BaseDQToolset`:

    * ``list_validators`` — exposes the registered validator catalog so the LLM
      can choose an appropriate validator for each check.
    * ``apply_validator`` — instantiates a validator and applies it to a metric
      value, returning ``{"passed": bool, "reason": ...}``.

    Use this toolset alongside :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`
    or :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset` in
    :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator`.
    The data-source toolset handles schema discovery and query execution; this
    toolset handles the DQ-specific logic::

        LLMDataQualityOperator(
            checks=[...],
            llm_conn_id="pydanticai_default",
            toolsets=[
                SQLToolset(db_conn_id="postgres_default"),
                SQLDQToolset(),
            ],
        )

    :param validator_registry: Validator registry to expose to the LLM.
        Defaults to :data:`~airflow.providers.common.ai.utils.dataquality.validation.default_registry`.
    """

    def __init__(self, *, validator_registry: ValidatorRegistry | None = None) -> None:
        super().__init__()
        self._registry = validator_registry if validator_registry is not None else default_registry

    @property
    def id(self) -> str:
        return "dq-sql"

    @property
    def output_mode(self) -> Literal["execute", "generate"]:
        return "execute"

    # ------------------------------------------------------------------
    # AbstractToolset interface
    # ------------------------------------------------------------------

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools = await super().get_tools(ctx)

        for name, description, schema in (
            (
                "list_validators",
                "List available validator names, parameters, and descriptions.",
                _LIST_VALIDATORS_SCHEMA,
            ),
            (
                "apply_validator",
                "Apply a registered validator to a metric value and return pass/fail.",
                _APPLY_VALIDATOR_SCHEMA,
            ),
        ):
            # In planning mode (two-phase approval flow) the agent must NOT call
            # apply_validator — it only selects a validator by name/args and records
            # that choice in the DQPlan output.  apply_validator runs in Phase 2
            # (pure Python) after the human reviewer approves.
            if name == "apply_validator" and getattr(self, "_planning_mode", False):
                continue
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name == "list_validators":
            result = self._list_validators()
            _log.info("list_validators result: %s", result)
            return result
        if name == "apply_validator":
            check_name = tool_args["check_name"]
            validator_name = tool_args["validator_name"]
            validator_args = tool_args.get("validator_args") or {}
            value = tool_args["value"]

            # Resolve the display name: for fixed validators, show the actual
            # validator's name/signature followed by "(fixed)" so logs are
            # meaningful instead of just showing "fixed".
            if validator_name.lower() == "fixed" or self._has_fixed_validator(check_name):
                fixed = self._get_fixed_validator(check_name)
                display_name = getattr(fixed, "_validator_display", None) or getattr(
                    fixed, "_validator_name", "fixed"
                )
                log_validator = f"{display_name}(fixed)"
            else:
                log_validator = validator_name

            _log.info(
                "apply_validator: check=%s, validator=%s, args=%s, value=%s",
                check_name,
                log_validator,
                validator_args,
                value,
            )
            result = self._apply_validator(check_name, value, validator_name, validator_args)
            parsed = json.loads(result)
            _log.info(
                "apply_validator result: check=%s, passed=%s, reason=%s",
                parsed.get("check_name"),
                parsed.get("passed"),
                parsed.get("reason"),
            )
            return result
        return await super().call_tool(name, tool_args, ctx, tool)

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _list_validators(self) -> str:
        entries = []
        for name in self._registry.list_validators():
            entry = self._registry.get(name)
            entries.append(
                {
                    "name": name,
                    "category": entry.check_category or None,
                    "row_level": entry.row_level,
                    "parameters": self._format_signature(entry.factory),
                    "description": entry.llm_context or None,
                }
            )
        return json.dumps(entries)

    def _apply_validator(
        self,
        check_name: str,
        value: Any,
        validator_name: str,
        validator_args: dict[str, Any],
    ) -> str:
        if validator_name.lower() == "fixed" or self._has_fixed_validator(check_name):
            fixed = self._get_fixed_validator(check_name)
            if fixed is None:
                return json.dumps(
                    {
                        "check_name": check_name,
                        "passed": False,
                        "reason": f"No fixed validator configured for check {check_name!r}.",
                    }
                )
            is_row_level = bool(getattr(fixed, "_row_level", False))
            if is_row_level:
                return self._apply_row_level_validator(check_name, value, fixed, validator_name="fixed")

            _log.info("apply_validator scalar: check=%s, value=%s", check_name, value)
            try:
                passed = bool(fixed(value))
            except Exception as exc:
                return json.dumps({"check_name": check_name, "passed": False, "reason": str(exc)})
            reason = None if passed else f"Fixed validator returned False for value {value!r}"
            return json.dumps({"check_name": check_name, "passed": passed, "reason": reason})

        if not validator_name or validator_name.lower() == "none":
            return json.dumps({"check_name": check_name, "passed": True, "reason": "no validator"})

        ok, err = self._validate_suggestion(validator_name, validator_args)
        if not ok:
            return json.dumps({"check_name": check_name, "passed": False, "reason": err})

        try:
            entry = self._registry.get(validator_name)
            validator = entry.factory(**validator_args)
        except Exception as exc:
            return json.dumps({"check_name": check_name, "passed": False, "reason": str(exc)})

        if entry.row_level:
            return self._apply_row_level_validator(
                check_name,
                value,
                validator,
                validator_name=validator_name,
            )

        try:
            passed = bool(validator(value))
        except Exception as exc:
            return json.dumps({"check_name": check_name, "passed": False, "reason": str(exc)})

        reason = None if passed else f"{validator_name} returned False for value {value!r}"
        return json.dumps({"check_name": check_name, "passed": passed, "reason": reason})

    def _apply_row_level_validator(
        self,
        check_name: str,
        value: Any,
        validator: Callable[[Any], bool],
        *,
        validator_name: str,
    ) -> str:
        """Apply a row-level validator to each row value and return a RowLevelResult payload."""
        values = self._coerce_row_values(check_name, value)
        invalid_values: list[Any] = []

        for row_value in values:
            try:
                is_valid = bool(validator(row_value))
            except Exception as exc:
                return json.dumps(
                    {
                        "check_name": check_name,
                        "passed": False,
                        "reason": (
                            f"{validator_name} raised {exc!s} while evaluating row-level value {row_value!r}."
                        ),
                    }
                )
            if not is_valid:
                invalid_values.append(row_value)

        summary = RowLevelResult.build(invalid_values, len(values), sample_limit=_ROW_LEVEL_SAMPLE_LIMIT)
        max_invalid_pct = self._get_max_invalid_pct(validator)
        passed = summary.invalid_pct <= max_invalid_pct
        reason = None
        if not passed:
            reason = (
                f"{validator_name} failed row-level validation: invalid_pct={summary.invalid_pct:.6f} "
                f"exceeded max_invalid_pct={max_invalid_pct:.6f}."
            )

        return json.dumps(
            {
                "check_name": check_name,
                "passed": passed,
                "reason": reason,
                "value": summary.model_dump(),
            }
        )

    @staticmethod
    def _coerce_row_values(check_name: str, value: Any) -> list[Any]:
        """Ensure row-level validators always receive a list of row values."""
        if isinstance(value, list):
            return value
        _log.warning(
            "apply_validator: check=%s is row_level but value is %s, not a list — wrapping.",
            check_name,
            type(value).__name__,
        )
        return [value]

    @staticmethod
    def _get_max_invalid_pct(validator: Callable[[Any], bool]) -> float:
        """Read max_invalid_pct from validator metadata; default to strict mode (0.0)."""
        raw = getattr(validator, "_max_invalid_pct", 0.0)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    def _validate_suggestion(self, validator_name: str, validator_args: dict[str, Any]) -> tuple[bool, str]:
        """Check that *validator_name* is registered and *validator_args* are valid."""
        try:
            entry = self._registry.get(validator_name)
        except KeyError:
            return (
                False,
                f"Validator {validator_name!r} is not registered. Available: {self._registry.list_validators()}",
            )

        sig = inspect.signature(entry.factory)
        unknown = [k for k in validator_args if k not in sig.parameters]
        if unknown:
            valid = [p for p in sig.parameters if p != "self"]
            return False, f"Unknown argument(s) {unknown} for {validator_name!r}. Valid: {valid}"

        try:
            entry.factory(**validator_args)
        except Exception as exc:
            return False, f"Failed to instantiate {validator_name!r} with {validator_args!r}: {exc}"

        return True, ""

    def _has_fixed_validator(self, check_name: str) -> bool:
        """Return ``True`` when the check has a pre-assigned validator callable."""
        return any(c.name == check_name and c.validator is not None for c in self._checks)

    def _get_fixed_validator(self, check_name: str) -> Callable[[Any], bool] | None:
        """Return the pre-assigned validator for *check_name*, or ``None``."""
        return next(
            (c.validator for c in self._checks if c.name == check_name and c.validator is not None),
            None,
        )

    @staticmethod
    def _format_signature(factory: Any) -> str:
        """Return a compact human-readable parameter list for a validator factory."""
        try:
            sig = inspect.signature(factory)
        except (ValueError, TypeError):
            return "(unknown)"
        parts: list[str] = []
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            annotation = (
                param.annotation.__name__
                if isinstance(param.annotation, type)
                else (str(param.annotation) if param.annotation is not inspect.Parameter.empty else "Any")
            )
            if param.default is inspect.Parameter.empty:
                parts.append(f"{name}: {annotation}")
            else:
                parts.append(f"{name}: {annotation} = {param.default!r}")
        return "(" + ", ".join(parts) + ")" if parts else "(no parameters)"
