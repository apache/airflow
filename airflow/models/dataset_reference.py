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
from typing import Optional

from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Integer, String

from airflow.models.base import ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class DatasetReference(Base):
    """References to datasets"""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(String(ID_LEN), primary_key=True, nullable=False)
    task_id = Column(String(ID_LEN), primary_key=True, nullable=False)
    is_write = Column(Boolean, primary_key=True, nullable=False)
    is_scheduling_dep = Column(Boolean, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow(), onupdate=timezone.utcnow, nullable=False)

    __tablename__ = "dataset_reference"
    __table_args__ = (
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='dataset_event_dataset_fkey',
            ondelete="CASCADE",
        ),
    )

    def __eq__(self, other):
        return self.uri == other.uri

    def __hash__(self):
        return hash((self.uri, self.extra))

    def __repr__(self):
        args = []
        for attr in ('dataset_id', 'dag_id', 'task_id', 'is_write'):
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


class InletDataset:
    """For inbound dataset references"""

    def __init__(self, uri, *, schedule_on: bool = True, **kwargs):
        self.uri = uri
        self.schedule_on = schedule_on

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)


class OutletDataset:
    """For outbound dataset references"""

    def __init__(self, uri, extra: Optional[dict] = None, **kwargs):
        self.uri = uri
        self.extra = extra

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)
