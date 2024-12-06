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

from datetime import datetime
from typing import TYPE_CHECKING

import sqlalchemy_jsonfield
from sqlalchemy import Column, DateTime, Index, Integer, String

from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class Deadlines(LoggingMixin):
    """A Deadline is a 'need-by' date which triggers a callback if the provided time has passed."""

    @classmethod
    @provide_session
    def add_deadline(cls, deadline: DeadlinesModel, session: Session = NEW_SESSION):
        """Add the provided deadline to the table."""
        session.add(deadline)


class DeadlinesModel(Base, LoggingMixin):
    """Table containing Deadline Alert properties."""

    __tablename__ = "deadlines"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # If the Deadline Alert is for a DAG, store the DAG ID and Run ID from the dag_run.
    """
    TODO FIXME:   making these foreign keys causes the following error:

    qlalchemy.exc.OperationalError: (sqlite3.OperationalError) foreign key mismatch - "deadlines" referencing "dag_run"
    [SQL: INSERT INTO deadlines (dag_id, run_id, deadline, callback, callback_kwargs) VALUES (?, ?, ?, ?, ?)]
    [parameters: ('deadlines_1', 'manual__2024-12-04T00:14:06.333084+00:00', '2024-12-04 01:14:06.362093', 'email', 'null')]
    """
    dag_id = Column(StringID())  # ForeignKey("dag_run.dag_id"))
    run_id = Column(StringID())  # ForeignKey("dag_run.run_id"))
    # The time after which the Deadline has passed and the callback should be triggered.
    deadline = Column(DateTime, nullable=False)
    # The Callback to be called when the Deadline has passed.
    callback = Column(String(), nullable=False)
    # Serialized kwargs to pass to the callback.
    callback_kwargs = Column(sqlalchemy_jsonfield.JSONField(json=json))

    __table_args__ = (Index("deadline_idx", deadline, unique=False),)

    def __init__(
        self,
        deadline: datetime,
        callback: str,
        callback_kwargs: dict | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
    ):
        super().__init__()
        self.deadline = deadline
        self.callback = callback
        self.callback_kwargs = callback_kwargs
        self.dag_id = dag_id
        self.run_id = run_id

    def __repr__(self):
        def _determine_resource() -> tuple[str, str]:
            """Determine the type of resource based on which values are present."""
            if self.dag_id and self.run_id:
                # The deadline is for a dagrun:
                return "DagRun", f"Dag: {self.dag_id} Run: {self.run_id}"

            return "Unknown", ""

        resource_type, resource_details = _determine_resource()
        callback_kwargs = json.dumps(self.callback_kwargs) if self.callback_kwargs else ""

        return (
            f"[{resource_type} Deadline] {resource_details} needed by "
            f"{self.deadline} or run: {self.callback}({callback_kwargs})"
        )
