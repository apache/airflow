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

from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Index, Integer, Text

from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime


class SlaMiss(Base):
    """
    Model that stores a history of the SLA that have been missed.
    It is used to keep track of SLA failures over time and to avoid double
    triggering alert emails.
    """

    __tablename__ = "sla_miss"

    task_id = Column(StringID(), primary_key=True)
    dag_id = Column(StringID(), primary_key=True)
    run_id = Column(StringID(), primary_key=True)
    map_index = Column(Integer, primary_key=True, server_default='-1')
    email_sent = Column(Boolean, default=False)
    timestamp = Column(UtcDateTime)
    description = Column(Text)
    notification_sent = Column(Boolean, default=False)

    __table_args__ = (
        Index('sm_dag', dag_id, unique=False),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name='sla_miss_ti_fkey',
            ondelete="CASCADE",
        ),
    )

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + '>'
