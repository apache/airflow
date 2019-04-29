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

from sqlalchemy import Column, Integer, String, Text, func

from airflow import conf
from airflow.models.base import Base
from airflow.utils.state import State
from airflow.utils.db import provide_session


class Pool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)
    slots = Column(Integer, default=0)
    description = Column(Text)

    default_pool_name = 'not_pooled'

    def __repr__(self):
        return self.pool

    @staticmethod
    @provide_session
    def default_pool_open_slots(session):
        from airflow.models import TaskInstance as TI  # To avoid circular imports
        total_slots = conf.getint('core', 'non_pooled_task_slot_count')
        used_slots = session.query(func.count()).filter(
            TI.pool == Pool.default_pool_name).filter(
            TI.state.in_([State.RUNNING, State.QUEUED])).scalar()
        return total_slots - used_slots

    def to_json(self):
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def open_slots(self, session):
        """
        Returns the number of slots open at the moment
        """
        from airflow.models.taskinstance import \
            TaskInstance as TI  # Avoid circular import

        used_slots = session.query(func.count()).filter(TI.pool == self.pool).filter(
            TI.state.in_([State.RUNNING, State.QUEUED])).scalar()
        return self.slots - used_slots
