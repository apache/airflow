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

from airflow.models.base import Base
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils.session import provide_session
from airflow.utils.state import State


class Pool(Base):
    """
    the class to get Pool info.
    """
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(256), unique=True)
    # -1 for infinite
    slots = Column(Integer, default=0)
    description = Column(Text)

    DEFAULT_POOL_NAME = 'default_pool'

    def __repr__(self):
        return self.pool

    @staticmethod
    @provide_session
    def get_pool(pool_name, session=None):
        """
        Get the Pool with specific pool name from the Pools.

        :param pool_name: The pool name of the Pool to get.
        :return: the pool object
        """
        return session.query(Pool).filter(Pool.pool == pool_name).first()

    @staticmethod
    @provide_session
    def get_default_pool(session=None):
        """
        Get the Pool of the default_pool from the Pools.

        :return: the pool object
        """
        return Pool.get_pool(Pool.DEFAULT_POOL_NAME, session=session)

    def to_json(self):
        """
        Get the Pool in a json structure

        :return: the pool object in json format
        """
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def occupied_slots(self, session):
        """
        Get the number of slots used by running/queued tasks at the moment.

        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import
        return (
            session
            .query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state.in_(list(EXECUTION_STATES)))
            .scalar()
        ) or 0

    @provide_session
    def used_slots(self, session):
        """
        Get the number of slots used by running tasks at the moment.

        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        return (
            session
            .query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .scalar()
        ) or 0

    @provide_session
    def queued_slots(self, session):
        """
        Get the number of slots used by queued tasks at the moment.

        :return: the used number of slots
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        return (
            session
            .query(func.sum(TaskInstance.pool_slots))
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .scalar()
        ) or 0

    @provide_session
    def open_slots(self, session):
        """
        Get the number of slots open at the moment.

        :return: the number of slots
        """
        if self.slots == -1:
            return float('inf')
        else:
            return self.slots - self.occupied_slots(session)
