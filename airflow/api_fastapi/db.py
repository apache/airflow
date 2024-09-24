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

from typing import TYPE_CHECKING

from airflow.utils.session import create_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.parameters import BaseParam


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


def apply_filters_to_select(base_select: Select, filters: list[BaseParam]) -> Select:
    select = base_select
    for filter in filters:
        select = filter.to_orm(select)

    return select
