# -*- coding: utf-8 -*-
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

from sqlalchemy import Column, Index, Integer, String

from airflow.models.base import Base, ID_LEN
from airflow.utils.sqlalchemy import UtcDateTime


class TaskFail(Base):
    """
    TaskFail tracks the failed run durations of each task instance.
    """

    __tablename__ = "task_fail"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Integer)

    __table_args__ = (
        Index('idx_task_fail_dag_task_date', dag_id, task_id, execution_date,
              unique=False),
    )

    def __init__(self, task, execution_date, start_date, end_date):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.end_date = end_date
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None
