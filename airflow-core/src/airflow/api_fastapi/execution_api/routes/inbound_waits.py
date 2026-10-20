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

from uuid import UUID

import structlog
from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status

from airflow._shared.timezones import timezone
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.inbound_wait import InboundWaitRequest, InboundWaitResponse
from airflow.api_fastapi.execution_api.security import ExecutionAPIRoute, get_team_name_for_ti, require_auth
from airflow.models.inbound_wait import WaitStatus
from airflow.utils.session import create_session
from airflow.webhooks.connections import ConfiguredWebhook, ResolutionFailure, resolve_webhook
from airflow.webhooks.registry import WebhookRegistry, get_default_wait_timeout, get_early_delivery_ttl
from airflow.webhooks.wait_registration import (
    TaskInstanceArchived,
    TaskInstanceNotFound,
    TaskInstanceNotRunning,
    WaitKeyTaken,
    claim_early_delivery_after_commit,
    lock_registering_task_instance,
    register_wait,
)

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        # Validates that the JWT sub matches the task_instance_id path parameter.
        Security(require_auth, scopes=["ti:self"]),
    ],
)

log = structlog.get_logger(__name__)


def _check_dag_has_no_team(task_instance_id: UUID) -> None:
    """Refuse a task whose Dag has a team: webhooks have none, so they serve only Dags without one."""
    with create_session(scoped=False) as session:
        team_name = get_team_name_for_ti(task_instance_id, session)
    if team_name is not None:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "dag_has_team",
                "message": (
                    f"The task's Dag belongs to team {team_name!r}, and webhook waits serve only Dags "
                    "without a team in this version of Airflow"
                ),
            },
        )


def _resolve_webhook(name: str) -> None:
    """Refuse a webhook that is not served: not listed, or without a connection that configures it."""
    registry = WebhookRegistry.from_config()
    if name not in registry.names:
        log.warning("Refused a webhook wait: the webhook is not listed in [webhooks] names", webhook=name)
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "webhook_not_found", "message": f"Webhook {name!r} is not served"},
        )
    resolution = resolve_webhook(
        name,
        source_type_owners=registry.source_type_owners,
        default_max_body_bytes=registry.max_body_bytes,
        default_store_payload=registry.store_payload,
    )
    if isinstance(resolution, ConfiguredWebhook):
        return
    log.warning(
        "Refused a webhook wait: the webhook's connection is not usable",
        webhook=name,
        reason=resolution.reason,
    )
    if resolution.failure is ResolutionFailure.UNAVAILABLE:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"reason": "webhook_unavailable", "message": f"Webhook {name!r} cannot be read for now"},
        )
    raise HTTPException(
        status.HTTP_404_NOT_FOUND,
        detail={"reason": "webhook_not_found", "message": f"Webhook {name!r} is not served"},
    )


@router.post(
    "/{task_instance_id}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            (
                status.HTTP_403_FORBIDDEN,
                "The task's Dag has a team, and webhook waits serve only Dags without one",
            ),
            (status.HTTP_404_NOT_FOUND, "The task instance or the webhook is not found"),
            (
                status.HTTP_409_CONFLICT,
                "The task instance is not running, or another one waits on the webhook with the same key",
            ),
            (status.HTTP_410_GONE, "The task instance's attempt has been archived"),
            (HTTP_422_UNPROCESSABLE_CONTENT, "Invalid wait, or no timeout and no default timeout"),
            (status.HTTP_503_SERVICE_UNAVAILABLE, "The webhook's connection cannot be read for now"),
        ]
    ),
)
def register_inbound_wait(task_instance_id: UUID, body: InboundWaitRequest) -> InboundWaitResponse:
    """
    Register the task instance's wait for a delivery to an inbound webhook.

    The task then parks, and the first matching delivery resumes it. A delivery that arrived before the
    wait and was kept early is claimed now, and the wait answers ``received``.

    The route opens its own transactions, since the wait looks for early deliveries again once its
    registration has committed.
    """
    timeout = body.timeout or get_default_wait_timeout()
    if timeout is None:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "timeout_required",
                "message": "A webhook wait needs a timeout, as [webhooks] default_wait_timeout is not set",
            },
        )
    _check_dag_has_no_team(task_instance_id)
    _resolve_webhook(body.webhook)
    early_delivery_ttl = get_early_delivery_ttl()
    try:
        with create_session(scoped=False) as session:
            ti = lock_registering_task_instance(task_instance_id, session=session)
            now = timezone.utcnow()
            wait = register_wait(
                ti,
                webhook=body.webhook,
                events=body.events,
                match=body.match,
                deadline=now + timeout,
                early_delivery_cutoff=now - early_delivery_ttl,
                session=session,
            )
            response = InboundWaitResponse(id=wait.id, status=WaitStatus(wait.status), deadline=wait.deadline)
    except TaskInstanceNotFound as e:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail={"reason": "not_found", "message": str(e)}
        ) from None
    except TaskInstanceArchived as e:
        raise HTTPException(status.HTTP_410_GONE, detail={"reason": "not_found", "message": str(e)}) from None
    except TaskInstanceNotRunning as e:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"reason": "invalid_state", "message": str(e), "previous_state": e.state},
        ) from None
    except WaitKeyTaken as e:
        raise HTTPException(
            status.HTTP_409_CONFLICT, detail={"reason": "wait_key_taken", "message": str(e)}
        ) from None
    if response.status == WaitStatus.REGISTERED:
        with create_session(scoped=False) as session:
            claimed = claim_early_delivery_after_commit(
                response.id, cutoff=timezone.utcnow() - early_delivery_ttl, session=session
            )
        if claimed is not None:
            response.status = WaitStatus.RECEIVED
    log.info(
        "Registered a webhook wait",
        ti_id=str(task_instance_id),
        webhook=body.webhook,
        wait_id=str(response.id),
        status=response.status,
    )
    return response
