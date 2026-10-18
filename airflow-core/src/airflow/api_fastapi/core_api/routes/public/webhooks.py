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
from sqlalchemy import select
from sqlalchemy.orm import defer

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.webhooks import WebhookDeliveryCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_webhook
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.inbound_delivery import InboundDelivery
from airflow.webhooks.registry import WEBHOOK_NAME_PATTERN, parse_webhook_names

webhooks_router = AirflowRouter(tags=["Webhook"], prefix="/webhooks")


def _get_listed_webhook_names() -> frozenset[str]:
    try:
        return parse_webhook_names(conf.get("webhooks", "names"))
    except AirflowConfigException:
        # [webhooks] is validated only where webhooks are served, so an invalid value, such as a
        # team-qualified name that only a newer Airflow accepts, must not hide recorded deliveries.
        return frozenset()


@webhooks_router.get(
    "/{webhook_name}/deliveries",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_webhook(method="GET"))],
    tags=["experimental"],
    summary="Experimental: List the deliveries a webhook received.",
    description=(
        "🚧 This is an experimental endpoint and may change or be removed without notice.\n\n"
        "Lists the deliveries a webhook received, newest first unless `order_by` says otherwise, "
        "without their payloads."
    ),
)
def get_webhook_deliveries(
    webhook_name: str,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(["received_at", "id", "outcome", "delivery_id"], InboundDelivery).dynamic_depends(
                default="-received_at"
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
    received_at_range: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("received_at", InboundDelivery))
    ],
    session: SessionDep,
) -> WebhookDeliveryCollectionResponse:
    # No name outside the grammar can be listed or recorded. A webhook removed from [webhooks] names
    # still lists the deliveries it received.
    if not WEBHOOK_NAME_PATTERN.fullmatch(webhook_name) or (
        webhook_name not in _get_listed_webhook_names()
        and session.scalar(select(InboundDelivery.id).where(InboundDelivery.webhook == webhook_name).limit(1))
        is None
    ):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The webhook `{webhook_name}` was not found")

    deliveries_select, total_entries = paginated_select(
        statement=select(InboundDelivery)
        .where(InboundDelivery.webhook == webhook_name)
        .options(defer(InboundDelivery.payload, raiseload=True)),
        filters=[outcome, delivery_id, id_, correlation_key, received_at_range],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    return WebhookDeliveryCollectionResponse(
        deliveries=session.scalars(deliveries_select),
        total_entries=total_entries,
    )
