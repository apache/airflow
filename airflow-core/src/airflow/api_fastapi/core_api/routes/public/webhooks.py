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

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, status
from sqlalchemy import ColumnElement, select
from sqlalchemy.orm import Session, defer

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.webhooks import (
    WebhookCollectionResponse,
    WebhookDeliveryCollectionResponse,
    WebhookResponse,
    WebhookWaitCollectionResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    DagAccessEntity,
    GetUserDep,
    requires_access_dag,
    requires_access_webhook,
)
from airflow.api_fastapi.core_api.services.public.task_coordinates import TaskScopeDep
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.inbound_delivery import InboundDelivery
from airflow.models.inbound_wait import InboundWait
from airflow.models.taskinstance import TaskInstance
from airflow.webhooks.connections import CONN_ID_PREFIX
from airflow.webhooks.registry import WEBHOOK_NAME_PATTERN, WebhookRegistry, parse_webhook_names
from airflow.webhooks.status import get_webhook_status

webhooks_router = AirflowRouter(tags=["Webhook"], prefix="/webhooks")
webhook_waits_router = AirflowRouter(
    tags=["Task Instance"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/webhookWaits",
)


def _get_listed_webhook_names() -> frozenset[str]:
    try:
        return parse_webhook_names(conf.get("webhooks", "names"))
    except AirflowConfigException:
        # [webhooks] is validated only where webhooks are served, so an invalid value, such as a
        # team-qualified name that only a newer Airflow accepts, must not hide recorded deliveries.
        return frozenset()


def get_readable_webhook_names(*, user: BaseUser, session: Session) -> set[str]:
    """
    Return the webhooks whose ``webhook_<name>`` connection the user may read.

    The candidates are the names listed in ``[webhooks] names`` and those with recorded deliveries, so
    a webhook removed from ``names`` keeps its history readable.
    """
    names = _get_listed_webhook_names().union(session.scalars(select(InboundDelivery.webhook).distinct()))
    readable_conn_ids = get_auth_manager().filter_authorized_connections(
        conn_ids={f"{CONN_ID_PREFIX}{name}" for name in names}, user=user, method="GET"
    )
    return {name for name in names if f"{CONN_ID_PREFIX}{name}" in readable_conn_ids}


@webhooks_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_500_INTERNAL_SERVER_ERROR, "The `[webhooks]` configuration is invalid.")]
    ),
    dependencies=[Depends(requires_access_webhook(method="GET", lists_webhooks=True))],
    tags=["experimental"],
    summary="Experimental: List webhooks and whether they can be served.",
    description=(
        "🚧 This is an experimental endpoint and may change or be removed without notice.\n\n"
        "Lists the webhooks whose connection you may read, by name: those that `[webhooks] names` lists "
        "and those with recorded deliveries. Each says whether its connection and source type let it be "
        "served, and when it last received a delivery. A caller who may read no webhook gets an empty "
        "list.\n\n"
        "Each call reads the connections of the webhooks it returns from the secrets backends. It never "
        "returns what a connection holds or why a secrets backend failed: `airflow webhooks check` "
        "says that."
    ),
)
def get_webhooks(
    limit: QueryLimit,
    offset: QueryOffset,
    user: GetUserDep,
    session: SessionDep,
) -> WebhookCollectionResponse:
    try:
        registry = WebhookRegistry.from_config()
    except AirflowConfigException:
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "The [webhooks] configuration is invalid, so no webhook can be served: run airflow webhooks "
            "check to see why",
        ) from None
    names = sorted(get_readable_webhook_names(user=user, session=session))
    start = offset.value or 0
    end = None if limit.value is None else start + limit.value
    webhooks = []
    for name in names[start:end]:
        webhook_status = get_webhook_status(name, registry=registry, session=session)
        # One query per webhook, each answered by the last entry of the (webhook, received_at) index,
        # rather than one that reads every delivery of the page's webhooks.
        last_delivery = session.execute(
            select(InboundDelivery.received_at, InboundDelivery.outcome)
            .where(InboundDelivery.webhook == name)
            .order_by(InboundDelivery.received_at.desc(), InboundDelivery.id.desc())
            .limit(1)
        ).first()
        webhooks.append(
            WebhookResponse(
                name=name,
                listed=webhook_status.listed,
                connection_status=webhook_status.connection_status,
                source_type=webhook_status.source_type,
                source_type_status=webhook_status.source_type_status,
                last_received_at=last_delivery.received_at if last_delivery else None,
                last_outcome=last_delivery.outcome if last_delivery else None,
            )
        )
    return WebhookCollectionResponse(webhooks=webhooks, total_entries=len(names))


@webhooks_router.get(
    "/{webhook_name}/deliveries",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_webhook(method="GET"))],
    tags=["experimental"],
    summary="Experimental: List the deliveries a webhook received.",
    description=(
        "🚧 This is an experimental endpoint and may change or be removed without notice.\n\n"
        "Lists the deliveries a webhook received, newest first unless `order_by` says otherwise, "
        "without their payloads.\n\n"
        "Use `~` as the webhook name to list the deliveries of every webhook whose connection you "
        "may read; `webhook_names` then narrows the list to some of them."
    ),
)
def get_webhook_deliveries(
    webhook_name: str,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["received_at", "id", "webhook", "outcome", "delivery_id"], InboundDelivery
            ).dynamic_depends(default="-received_at")
        ),
    ],
    webhook_names: Annotated[
        FilterParam[list[str]],
        Depends(
            filter_param_factory(
                InboundDelivery.webhook,
                list[str],
                FilterOptionEnum.IN,
                "webhook_names",
                description=(
                    "Only these webhooks. Repeat it to name several: `?webhook_names=a&webhook_names=b`."
                ),
            )
        ),
    ],
    outcome: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(InboundDelivery.outcome, str | None))
    ],
    delivery_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(InboundDelivery.delivery_id, str | None))
    ],
    id_: Annotated[FilterParam[UUID | None], Depends(filter_param_factory(InboundDelivery.id, UUID | None))],
    correlation_key: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(InboundDelivery.correlation_key, str | None))
    ],
    source_type: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(InboundDelivery.source_type, str | None))
    ],
    received_at_range: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("received_at", InboundDelivery))
    ],
    user: GetUserDep,
    session: SessionDep,
) -> WebhookDeliveryCollectionResponse:
    webhook_clause: ColumnElement[bool]
    if webhook_name == "~":
        webhook_clause = InboundDelivery.webhook.in_(get_readable_webhook_names(user=user, session=session))
    # No name outside the grammar can be listed or recorded. A webhook removed from [webhooks] names
    # still lists the deliveries it received.
    elif not WEBHOOK_NAME_PATTERN.fullmatch(webhook_name) or (
        webhook_name not in _get_listed_webhook_names()
        and session.scalar(select(InboundDelivery.id).where(InboundDelivery.webhook == webhook_name).limit(1))
        is None
    ):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The webhook `{webhook_name}` was not found")
    else:
        webhook_clause = InboundDelivery.webhook == webhook_name

    deliveries_select, total_entries = paginated_select(
        statement=select(InboundDelivery)
        .where(webhook_clause)
        .options(defer(InboundDelivery.payload, raiseload=True)),
        filters=[
            webhook_names,
            outcome,
            delivery_id,
            id_,
            correlation_key,
            source_type,
            received_at_range,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    return WebhookDeliveryCollectionResponse(
        deliveries=session.scalars(deliveries_select),
        total_entries=total_entries,
    )


@webhook_waits_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
    tags=["experimental"],
    summary="Experimental: List a task instance's webhook waits.",
    description=(
        "🚧 This is an experimental endpoint and may change or be removed without notice.\n\n"
        "Lists the webhook waits that a try of a task instance registered, by default its current try, "
        "newest first. A try has at most one open wait, its newest, with the status `registered` or "
        "`received`. A wait never shows the payload of the delivery it received."
    ),
)
def get_webhook_waits(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    scope: TaskScopeDep,
    session: SessionDep,
    try_number: int | None = None,
) -> WebhookWaitCollectionResponse:
    ti_query = select(TaskInstance.id).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.region_id == scope.region_id,
        TaskInstance.region_index == scope.region_index,
    )
    if try_number is not None:
        ti_query = ti_query.where(TaskInstance.try_number == try_number).execution_options(
            include_all_attempts=True
        )
    ti_id = session.scalar(ti_query)
    if ti_id is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "TaskInstance not found")

    waits_select, total_entries = paginated_select(
        statement=select(InboundWait)
        .where(InboundWait.ti_id == ti_id)
        .order_by(InboundWait.created_at.desc(), InboundWait.id.desc()),
        offset=offset,
        limit=limit,
        session=session,
    )
    return WebhookWaitCollectionResponse(
        webhook_waits=session.scalars(waits_select),
        total_entries=total_entries,
    )
