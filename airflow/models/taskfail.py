#
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
"""Taskfail tracks the failed run durations of each task instance"""

from sqlalchemy import Column, ForeignKeyConstraint, Integer

from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime


class TaskFail(Base):
    """TaskFail tracks the failed run durations of each task instance."""

    __tablename__ = "task_fail"

    id = Column(Integer, primary_key=True)
    task_id = Column(StringID(), nullable=False)
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)
    map_index = Column(Integer, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Integer)

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name='task_fail_ti_fkey',
            ondelete="CASCADE",
        ),
    )

    def __init__(self, task, run_id, start_date, end_date, map_index):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.run_id = run_id
        self.map_index = map_index
        self.start_date = start_date
        self.end_date = end_date
        if self.end_date and self.start_date:
            self.duration = int((self.end_date - self.start_date).total_seconds())
        else:
            self.duration = None

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + '>'
