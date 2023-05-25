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
from __future__ import annotations

from typing import TYPE_CHECKING

import dill
from sqlalchemy import BigInteger, Column, Integer, PickleType

from airflow.models.base import Base
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from airflow.models.dag import DAG


class DagPickle(Base):
    """
    Dags can originate from different places (user repos, main repo, ...)
    and also get executed in different places (different executors). This
    object represents a version of a DAG and becomes a source of truth for
    a BackfillJob execution. A pickle is a native python serialized object,
    and in this case gets stored in the database for the duration of the job.

    The executors pick up the DagPickle id and read the dag definition from
    the database.
    """

    id = Column(Integer, primary_key=True)
    pickle = Column(PickleType(pickler=dill))
    created_dttm = Column(UtcDateTime, default=timezone.utcnow)
    pickle_hash = Column(BigInteger)

    __tablename__ = "dag_pickle"

    def __init__(self, dag: DAG) -> None:
        self.dag_id = dag.dag_id
        if hasattr(dag, "template_env"):
            dag.template_env = None  # type: ignore[attr-defined]
        self.pickle_hash = hash(dag)
        self.pickle = dag
