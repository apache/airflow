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

import pendulum
from fastapi import Request
from pendulum.parsing.exceptions import ParserError

from airflow._shared.secrets_masker import secrets_masker
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.models import Log

logger = logging.getLogger(__name__)


def _mask_connection_fields(extra_fields):
    """Mask connection fields."""
    result = {}
    for k, v in extra_fields.items():
        if k == "extra" and v:
            try:
                parsed_extra = json.loads(v)
                if isinstance(parsed_extra, dict):
                    masked_extra = {ek: secrets_masker.redact(ev, ek) for ek, ev in parsed_extra.items()}
                    result[k] = masked_extra
                else:
                    result[k] = "Expected JSON object in `extra` field, got non-dict JSON"
            except json.JSONDecodeError:
                result[k] = "Encountered non-JSON in `extra` field"
        else:
            result[k] = secrets_masker.redact(v, k)
    return result


def _mask_variable_fields(extra_fields):
    """
    Mask the 'val_content' field if 'key_content' is in the mask list.

    The variable requests values and args comes in this form:
    {'key': 'key_content', 'val': 'val_content', 'description': 'description_content'}
    """
    result = {}
    keyname = None
    for k, v in extra_fields.items():
        if k == "key":
            keyname = v
            result[k] = v
        elif keyname and (k == "val" or k == "value"):
            x = secrets_masker.redact(v, keyname)
            result[k] = x
            keyname = None
        else:
            result[k] = v
    return result


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
            user_display = user.get_name()

        has_json_body = "application/json" in request.headers.get("content-type", "") and await request.body()

        if has_json_body:
            request_body = await request.json()
            masked_body_json = {k: secrets_masker.redact(v, k) for k, v in request_body.items()}
        else:
            request_body = {}
            masked_body_json = {}

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
                    logger.exception("Failed to parse logical_date from the request: %s", logical_date_value)
            else:
                logger.warning("Logical date is missing or empty")
        session.add(log)
        # Explicit commit to persist the access log independently if the path operation fails or not.
        # Also it cannot be deferred to a 'function' scoped dependency because of the `request` parameter.
        session.commit()

    return log_action
