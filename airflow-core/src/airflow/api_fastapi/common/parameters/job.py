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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, Query
from sqlalchemy import select as sql_select

from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.jobs.job import Job
from airflow.models.team import JobTeam

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


class _JobTeamsFilter(BaseParam[list[str]]):
    """Filter jobs by the teams they serve (via the ``JobTeam`` association)."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        return select.where(Job.id.in_(sql_select(JobTeam.job_id).where(JobTeam.team_name.in_(self.value))))

    @classmethod
    def depends(cls, teams: list[str] = Query(default_factory=list)) -> _JobTeamsFilter:
        return cls().set_value(teams)


QueryJobTeamsFilter = Annotated[_JobTeamsFilter, Depends(_JobTeamsFilter.depends)]
