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

from sqlalchemy import Column, ForeignKey, String

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base


class TaskGroup(Base):
    """
    A task group per dag per task; grouping is rendered in the Graph/Tree view.
    """
    __tablename__ = "task_group"
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), ForeignKey('dag.dag_id'), primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    current_group = Column(String(ID_LEN))
    parent_group = Column(String(ID_LEN))
