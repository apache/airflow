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

"""Table to store information about individual task tries."""

from sqlalchemy import Column, ForeignKeyConstraint, Integer, String

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base


class TaskTry(Base):
    """Model to track individual TaskInstance tries.
    This is currently only used for storing appropriate hostnames.
    """

    __tablename__ = "task_try"

    # Link to upstream TaskInstance creating this dynamic mapping information.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    map_index = Column(Integer, primary_key=True)
    try_number = Column(Integer, primary_key=True)

    hostname = Column(String(1000), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_try_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )

