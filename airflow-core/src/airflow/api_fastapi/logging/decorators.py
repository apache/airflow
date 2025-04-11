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
from typing import Annotated

import pendulum
from fastapi import Depends, Request
from pendulum.parsing.exceptions import ParserError

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.security import get_user_with_exception_handling
from airflow.models import Log
from airflow.sdk.execution_time import secrets_masker

logger = logging.getLogger(__name__)


def _mask_connection_fields(extra_fields):
    """Mask connection fields."""
    result = {}
    for k, v in extra_fields.items():
        if k == "extra" and v:
            try:
                extra = json.loads(v)
                extra = {k: secrets_masker.redact(v, k) for k, v in extra.items()}
                result[k] = dict(extra)
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
        user: Annotated[BaseUser, Depends(get_user_with_exception_handling)],
    ):
        """Log user actions."""
        event_name = event or request.scope["endpoint"].__name__

        if not user:
            user_name = "anonymous"
            user_display = ""
        else:
            user_name = user.get_name()
            user_display = user.get_name()

        hasJsonBody = "application/json" in request.headers.get("content-type", "") and await request.body()

        if hasJsonBody:
            request_body = await request.json()
            masked_body_json = {k: secrets_masker.redact(v, k) for k, v in request_body.items()}
        else:
            request_body = {}
            masked_body_json = {}

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
                {k: v for k, v in request_body.items()} if hasJsonBody else extra_fields
            )
        elif "connection" in event_name:
            extra_fields = _mask_connection_fields(
                {k: v for k, v in request_body.items()} if hasJsonBody else extra_fields
            )
        elif hasJsonBody:
            extra_fields = {**extra_fields, **masked_body_json}

        params = {
            **request.query_params,
            **request.path_params,
        }

        if hasJsonBody:
            params.update(masked_body_json)
        if params and "is_paused" in params:
            extra_fields["is_paused"] = params["is_paused"] == "false"

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
                    log.logical_date = pendulum.parse(logical_date_value, strict=False)
                except ParserError:
                    logger.exception("Failed to parse logical_date from the request: %s", logical_date_value)
            else:
                logger.warning("Logical date is missing or empty")
        session.add(log)

    return log_action
