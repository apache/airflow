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

from sqlalchemy import Column, String, DateTime

from airflow.models.base import Base


class DagEdge(Base):
    """
    Dags can originate from different places (user repos, master repo, ...)
    and also get executed in different places (different executors). This
    object represents a version of a DAG and becomes a source of truth for
    a BackfillJob execution. A pickle is a native python serialized object,
    and in this case gets stored in the database for the duration of the job.

    The executors pick up the DagPickle id and read the dag definition from
    the database.
    """

    dag_id = Column(String(250), primary_key=True)
    execution_date = Column(DateTime(), primary_key=True)
    from_task = Column(String(250), primary_key=True)
    to_task = Column(String(250), primary_key=True)

    __tablename__ = "dag_edge"

    def __init__(self, dag_id, execution_date, from_task, to_task):
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.from_task = from_task
        self.to_task = to_task

