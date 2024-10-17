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

from typing import TYPE_CHECKING, Sequence

from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.common.parameters import BaseParam


async def get_session() -> Session:
    """
    Dependency for providing a session.

    For non route function please use the :class:`airflow.utils.session.provide_session` decorator.

    Example usage:

    .. code:: python

        @router.get("/your_path")
        def your_route(session: Annotated[Session, Depends(get_session)]):
            pass
    """
    with create_session() as session:
        yield session


def apply_filters_to_select(base_select: Select, filters: Sequence[BaseParam | None]) -> Select:
    base_select = base_select
    for filter in filters:
        if filter is None:
            continue
        base_select = filter.to_orm(base_select)

    return base_select


@provide_session
def paginated_select(
    base_select: Select,
    filters: Sequence[BaseParam],
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
) -> Select:
    base_select = apply_filters_to_select(
        base_select,
        filters,
    )

    total_entries = get_query_count(base_select, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    base_select = apply_filters_to_select(base_select, [order_by, offset, limit])

    return base_select, total_entries
