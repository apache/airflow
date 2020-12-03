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

from sqlalchemy import Column, Integer, String, Text, Index

from airflow.models.base import Base, ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class Log(Base):
    """
    Used to actively log events to the database
    """

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(UtcDateTime)
    dag_id = Column(String(ID_LEN))
    task_id = Column(String(ID_LEN))
    event = Column(String(30))
    execution_date = Column(UtcDateTime)
    owner = Column(String(500))
    extra = Column(Text)

    __table_args__ = (
        Index('idx_log_dag', dag_id),
    )

    def __init__(self, event, task_instance, owner=None, extra=None, **kwargs):
        self.dttm = timezone.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            if owner is None:
                # sometimes we use this model when task_instance is directly from the sqlalchemy
                # model, in which case the owner is not set bc the task is only set in the init
                # method of the TaskInstance class
                task_owner = task_instance.task.owner

        if 'task_id' in kwargs:
            self.task_id = kwargs['task_id']
        if 'dag_id' in kwargs:
            self.dag_id = kwargs['dag_id']
        if 'execution_date' in kwargs:
            if kwargs['execution_date']:
                self.execution_date = kwargs['execution_date']

        self.owner = owner or task_owner
