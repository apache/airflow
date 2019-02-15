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

from sqlalchemy import Column, String, Index, Integer

from airflow.utils.db import create_session

from airflow.models.base import Base


class DagEdge(Base):
    """
    This will store DAG edges. This will be the upstream and downstream dependencies of task inside a DAG.
    """

    __tablename__ = "dag_edge"

    dag_id = Column(String(250), primary_key=True)
    graph_id = Column(Integer, primary_key=True)
    from_task = Column(String(250), primary_key=True)
    to_task = Column(String(250), primary_key=True)

    __table_args__ = (Index('idx_dag_edge', dag_id, graph_id, unique=False),)

    def __init__(self, dag_id, execution_date, from_task, to_task):
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.from_task = from_task
        self.to_task = to_task

    @staticmethod
    def fetch_edges(dag_id, graph_id):
        with create_session() as session:
            return session.query(DagEdge) \
                .filter(DagEdge.dag_id == dag_id) \
                .filter(DagEdge.graph_id == graph_id) \
                .all()
