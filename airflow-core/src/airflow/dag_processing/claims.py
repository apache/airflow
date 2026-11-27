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

from sqlalchemy import exists, or_

from airflow.jobs.job import Job

if TYPE_CHECKING:
    from sqlalchemy import ColumnElement, SQLColumnExpression


def get_available_work_filter(processor_job_id: SQLColumnExpression[int | None]) -> ColumnElement[bool]:
    """Select unowned work or work whose owning Job has ended."""
    # Correlate by owner so polling an empty queue never materializes the entire Job history.
    retired_owner = exists().where(Job.id == processor_job_id, Job.end_date.is_not(None))
    return or_(processor_job_id.is_(None), retired_owner)
