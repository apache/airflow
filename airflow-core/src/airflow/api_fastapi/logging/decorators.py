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
from __future__ import annotations

import itertools
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pendulum
from fastapi import Request
from pendulum.parsing.exceptions import ParserError
from sqlalchemy import select

from airflow._shared.secrets_masker import secrets_masker
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.configuration import conf
from airflow.models import Connection, Log, Pool, Variable
from airflow.models.team import find_invalid_team_names

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Request parameter identifying a team-scoped resource, and the columns to read its team from.
_TEAM_SCOPED_RESOURCES = {
    "pool_name": (Pool.team_name, Pool.pool),
    "variable_key": (Variable.team_name, Variable.key),
    "connection_id": (Connection.team_name, Connection.conn_id),
}


def _sanitize_for_stdlib_log(value: str) -> str:
    """
    Strip CR/LF from a user-supplied value before passing it to stdlib's ``%s``-style logging.

    Defends against log injection when the deployment is configured with a non-JSON
    (plain-text) log formatter: a newline in the value would otherwise let an attacker forge
    log lines. ``structlog``-style formatters are unaffected, but the access-log path uses
    the stdlib logger here, so the sanitisation is unconditional.
    """
    return value.replace("\r", " ").replace("\n", " ")


def _mask_bulk_entities(extra_fields, mask_entity):
    """
    Apply per-entity masking to a bulk request body.

    A ``BulkBody`` has exactly one top-level field, ``actions``; the entities carrying the
    secrets sit two levels down, in ``actions[].entities[]``. The per-entity maskers below
    inspect top-level key names, so handing them a bulk body means they see only the key
    ``actions`` and pass its whole payload through untouched. Reach the entities first.

    Returns ``None`` when the body is not bulk-shaped, so callers fall back to flat masking.
    """
    actions = extra_fields.get("actions")
    if not isinstance(actions, list):
        return None

    masked_actions = []
    for action in actions:
        if not isinstance(action, dict):
            masked_actions.append(action)
            continue
        entities = action.get("entities")
        if not isinstance(entities, list):
            masked_actions.append(action)
            continue
        # ``delete`` actions may list bare id/key strings rather than entity objects;
        # those carry no secret and are left as they are.
        masked_actions.append(
            {
                **action,
                "entities": [mask_entity(e) if isinstance(e, dict) else e for e in entities],
            }
        )
    return {**extra_fields, "actions": masked_actions}


def _mask_connection_fields(extra_fields):
    """Mask connection fields, for either a single-entity or a bulk request body."""
    bulk = _mask_bulk_entities(extra_fields, _mask_connection_entity)
    if bulk is not None:
        return bulk
    return _mask_connection_entity(extra_fields)


def _mask_connection_entity(extra_fields):
    """Mask the fields of one connection."""
    result = {}
    for k, v in extra_fields.items():
        if k == "extra" and v:
            try:
                parsed_extra = json.loads(v)
                if isinstance(parsed_extra, dict):
                    # Connection ``extra`` can carry values under arbitrary key names, so the
                    # audit-log entry records only *which* ``extra`` fields were present and masks
                    # every value rather than deciding what to mask from the key name.
                    result[k] = {ek: "***" for ek in parsed_extra}
                else:
                    result[k] = "Expected JSON object in `extra` field, got non-dict JSON"
            except (json.JSONDecodeError, TypeError):
                # ``extra`` is declared as a string, but this runs on the raw body before
                # validation, so it can arrive as any JSON type -- a number or an already-decoded
                # object makes ``json.loads`` raise TypeError rather than JSONDecodeError. Both
                # are recorded without the value, instead of raising out of the audit-log path.
                result[k] = "Encountered non-JSON in `extra` field"
        else:
            result[k] = secrets_masker.redact(v, k)
    return result


def _mask_variable_fields(extra_fields):
    """Mask variable values, for either a single-entity or a bulk request body."""
    bulk = _mask_bulk_entities(extra_fields, _mask_variable_entity)
    if bulk is not None:
        return bulk
    return _mask_variable_entity(extra_fields)


def _mask_variable_entity(extra_fields):
    """
    Mask the variable value.

    The variable requests values and args comes in this form:
    {'key': 'key_content', 'val': 'val_content', 'description': 'description_content'}

    The value is masked unconditionally — the audit log records that a variable
    changed, not its contents, so a secret stored under any key name (not just a
    sensitive-looking one) is never persisted to the log.
    """
    result = {}
    for k, v in extra_fields.items():
        result[k] = "***" if k in ("val", "value") else v
    return result


def _resolve_team_name(params: dict, *, session: Session) -> str | None:
    """
    Return the team the audited action belongs to, for the resources that own no Dag.

    A Dag-scoped event has its team stamped from ``dag_id`` when the row is inserted, and a request
    that names a team carries it directly. What is left is an action on a team-scoped resource that
    names no team -- a deletion, or a patch that does not touch ``team_name`` -- where the team can
    only come from the resource being acted on. It is read here rather than in the routes because
    this dependency runs before the endpoint, so the row is still there to read even when the action
    is about to delete it.
    """
    team_name = params.get("team_name")
    if isinstance(team_name, str):
        # The endpoint's own validation rejects a name that is too long or malformed, but this row
        # is committed before that runs, so recording it would fail the insert on a backend that
        # enforces the column width. The value stays visible in ``extra`` either way.
        return None if find_invalid_team_names([team_name]) else team_name
    if params.get("dag_id"):
        # Left to the insert-time hook on ``Log``, which covers every writer of an audit row rather
        # than only this one, and resolves a Dag's team through its bundle instead of a column.
        return None
    if not conf.getboolean("core", "multi_team"):
        return None
    for param, (team_column, resource_column) in _TEAM_SCOPED_RESOURCES.items():
        if (resource_id := params.get(param)) is not None:
            return session.scalar(select(team_column).where(resource_column == resource_id))
    return None


def action_logging(event: str | None = None):
    async def log_action(
        request: Request,
        session: SessionDep,
        user: GetUserDep,
    ):
        """Log user actions."""
        event_name = event or request.scope["endpoint"].__name__
        skip_dry_run_events = {"clear_dag_run", "post_clear_task_instances"}

        if not user:
            user_name = "anonymous"
            user_display = ""
        else:
            user_name = user.get_name()
            user_display = user.get_display_name()

        has_json_body = "application/json" in request.headers.get("content-type", "") and await request.body()
        request_body = {}
        masked_body_json = {}

        if has_json_body:
            request_body = await request.json()
            if isinstance(request_body, dict):
                masked_body_json = {k: secrets_masker.redact(v, k) for k, v in request_body.items()}

                if event_name in skip_dry_run_events and request_body.get("dry_run", True):
                    return

        fields_skip_logging = {
            "csrf_token",
            "_csrf_token",
            "is_paused",
            "dag_id",
            "task_id",
            "dag_run_id",
            "run_id",
            "logical_date",
        }

        extra_fields = {
            k: secrets_masker.redact(v, k)
            for k, v in itertools.chain(request.query_params.items(), request.path_params.items())
            if k not in fields_skip_logging
        }
        if "variable" in event_name:
            extra_fields = _mask_variable_fields(
                {k: v for k, v in request_body.items()} if has_json_body else extra_fields
            )
        elif "connection" in event_name:
            extra_fields = _mask_connection_fields(
                {k: v for k, v in request_body.items()} if has_json_body else extra_fields
            )
        elif has_json_body:
            extra_fields = {**extra_fields, **masked_body_json}

        params = {
            **request.query_params,
            **request.path_params,
        }

        if has_json_body:
            params.update(masked_body_json)
        if params and "is_paused" in params:
            extra_fields["is_paused"] = params["is_paused"]

        extra_fields["method"] = request.method

        # Create log entry
        log = Log(
            event=event_name,
            task_instance=None,
            owner=user_name,
            owner_display_name=user_display,
            extra=json.dumps(extra_fields),
            task_id=params.get("task_id"),
            dag_id=params.get("dag_id"),
            run_id=params.get("run_id") or params.get("dag_run_id"),
            team_name=_resolve_team_name(params, session=session),
        )

        if "logical_date" in request.query_params:
            logical_date_value = request.query_params.get("logical_date")
            if logical_date_value:
                try:
                    logical_date = pendulum.parse(logical_date_value, strict=False)
                    if not isinstance(logical_date, datetime):
                        raise ParserError
                    log.logical_date = logical_date
                except ParserError:
                    logger.exception(
                        "Failed to parse logical_date from the request: %s",
                        _sanitize_for_stdlib_log(logical_date_value),
                    )
            else:
                logger.warning("Logical date is missing or empty")
        session.add(log)
        # Explicit commit to persist the access log independently if the path operation fails or not.
        # Also it cannot be deferred to a 'function' scoped dependency because of the `request` parameter.
        session.commit()

    return log_action
