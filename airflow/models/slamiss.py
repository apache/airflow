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

from sqlalchemy import Boolean, Column, Index, String, Text

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils.sqlalchemy import UtcDateTime


class SlaMiss(Base):
    """
    Model that stores a history of the SLA that have been missed.
    It is used to keep track of SLA failures over time and to avoid double
    triggering alert emails.
    """
    TASK_DURATION_EXCEEDED = "task_duration_exceeded"
    TASK_LATE_START = "task_late_start"
    TASK_LATE_FINISH = "task_late_finish"

    __tablename__ = "sla_miss"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    sla_type = Column(String(50), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    email_sent = Column(Boolean, default=False)
    timestamp = Column(UtcDateTime)
    description = Column(Text)
    notification_sent = Column(Boolean, default=False)

    __table_args__ = (
        Index('sm_dag', dag_id, unique=False),
    )

    def __repr__(self):
        return "SlaMiss <{sla_type}, {dag_id}.{task_id} [{exc_date}]>".format(
            dag_id=self.dag_id, task_id=self.task_id, sla_type=self.sla_type,
            exc_date=self.execution_date.isoformat())
