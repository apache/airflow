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

from sqlalchemy import Column, ForeignKeyConstraint, Integer

from airflow.models.base import Base, StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class DatasetDagRunQueue(Base):
    """Model for storing dataset events that need processing."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    target_dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __tablename__ = "dataset_dag_run_queue"
    __table_args__ = (
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='ddrq_dataset_fkey',
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            (target_dag_id,),
            ["dag.dag_id"],
            name='ddrq_dag_fkey',
            ondelete="CASCADE",
        ),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dataset_id == other.dataset_id and self.target_dag_id == other.target_dag_id
        else:
            return NotImplemented

    def __hash__(self):
        return hash((self.dataset_id, self.target_dag_id))

    def __repr__(self):
        args = []
        for attr in ('dataset_id', 'target_dag_id'):
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"
