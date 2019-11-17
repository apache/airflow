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
"""TaskReschedule tracks rescheduled task instances."""
from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, String, asc

from airflow.models.base import ID_LEN, Base
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime


class TaskReschedule(Base):
    """
    TaskReschedule tracks rescheduled task instances.
    """

    __tablename__ = "task_reschedule"

    id = Column(Integer, primary_key=True)
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)
    try_number = Column(Integer, nullable=False)
    start_date = Column(UtcDateTime, nullable=False)
    end_date = Column(UtcDateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    reschedule_date = Column(UtcDateTime, nullable=False)

    __table_args__ = (
        Index('idx_task_reschedule_dag_task_date', dag_id, task_id, execution_date,
              unique=False),
        ForeignKeyConstraint([task_id, dag_id, execution_date],
                             ['task_instance.task_id', 'task_instance.dag_id',
                              'task_instance.execution_date'],
                             name='task_reschedule_dag_task_date_fkey',
                             ondelete='CASCADE')
    )

    def __init__(self, task, execution_date, try_number, start_date, end_date,
                 reschedule_date):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.try_number = try_number
        self.start_date = start_date
        self.end_date = end_date
        self.reschedule_date = reschedule_date
        self.duration = (self.end_date - self.start_date).total_seconds()

    @staticmethod
    @provide_session
    def find_for_task_instance(task_instance, session):
        """
        Returns all task reschedules for the task instance and try number,
        in ascending order.

        :param task_instance: the task instance to find task reschedules for
        :type task_instance: airflow.models.TaskInstance
        """
        TR = TaskReschedule
        return (
            session
            .query(TR)
            .filter(TR.dag_id == task_instance.dag_id,
                    TR.task_id == task_instance.task_id,
                    TR.execution_date == task_instance.execution_date,
                    TR.try_number == task_instance.try_number)
            .order_by(asc(TR.id))
            .all()
        )
