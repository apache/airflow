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
"""Save Rendered Template Fields"""
from typing import Optional

import sqlalchemy_jsonfield
from sqlalchemy import Column, String, and_, not_, tuple_
from sqlalchemy.orm import Session

from airflow.configuration import conf
from airflow.models.base import ID_LEN, Base
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.helpers import serialize_template_field
from airflow.settings import json
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime


class RenderedTaskInstanceFields(Base):
    """Save Rendered Template Fields"""

    __tablename__ = "rendered_task_instance_fields"

    dag_id = Column(String(ID_LEN), primary_key=True)
    task_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    rendered_fields = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)

    def __init__(self, ti: TaskInstance, render_templates=True):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.task = ti.task
        self.execution_date = ti.execution_date
        self.ti = ti
        if render_templates:
            ti.render_templates()
        self.rendered_fields = {
            field: serialize_template_field(getattr(self.task, field)) for field in self.task.template_fields
        }

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.execution_date}"

    @classmethod
    @provide_session
    def get_templated_fields(cls, ti: TaskInstance, session: Session = None) -> Optional[dict]:
        """
        Get templated field for a TaskInstance from the RenderedTaskInstanceFields
        table.

        :param ti: Task Instance
        :param session: SqlAlchemy Session
        :return: Rendered Templated TI field
        """
        result = (
            session.query(cls.rendered_fields)
            .filter(
                cls.dag_id == ti.dag_id, cls.task_id == ti.task_id, cls.execution_date == ti.execution_date
            )
            .one_or_none()
        )

        if result:
            rendered_fields = result.rendered_fields
            return rendered_fields
        else:
            return None

    @provide_session
    def write(self, session: Session = None):
        """Write instance to database

        :param session: SqlAlchemy Session
        """
        session.merge(self)

    @classmethod
    @provide_session
    def delete_old_records(
        cls,
        task_id: str,
        dag_id: str,
        num_to_keep=conf.getint("core", "max_num_rendered_ti_fields_per_task", fallback=0),
        session: Session = None,
    ):
        """
        Keep only Last X (num_to_keep) number of records for a task by deleting others

        :param task_id: Task ID
        :param dag_id: Dag ID
        :param num_to_keep: Number of Records to keep
        :param session: SqlAlchemy Session
        """
        if num_to_keep <= 0:
            return

        tis_to_keep_query = (
            session.query(cls.dag_id, cls.task_id, cls.execution_date)
            .filter(cls.dag_id == dag_id, cls.task_id == task_id)
            .order_by(cls.execution_date.desc())
            .limit(num_to_keep)
        )

        if session.bind.dialect.name in ["postgresql", "sqlite"]:
            # Fetch Top X records given dag_id & task_id ordered by Execution Date
            subq1 = tis_to_keep_query.subquery('subq1')

            session.query(cls).filter(
                cls.dag_id == dag_id,
                cls.task_id == task_id,
                tuple_(cls.dag_id, cls.task_id, cls.execution_date).notin_(subq1),
            ).delete(synchronize_session=False)
        elif session.bind.dialect.name in ["mysql"]:
            # Fetch Top X records given dag_id & task_id ordered by Execution Date
            subq1 = tis_to_keep_query.subquery('subq1')

            # Second Subquery
            # Workaround for MySQL Limitation (https://stackoverflow.com/a/19344141/5691525)
            # Limitation: This version of MySQL does not yet support
            # LIMIT & IN/ALL/ANY/SOME subquery
            subq2 = session.query(subq1.c.dag_id, subq1.c.task_id, subq1.c.execution_date).subquery('subq2')

            session.query(cls).filter(
                cls.dag_id == dag_id,
                cls.task_id == task_id,
                tuple_(cls.dag_id, cls.task_id, cls.execution_date).notin_(subq2),
            ).delete(synchronize_session=False)
        else:
            # Fetch Top X records given dag_id & task_id ordered by Execution Date
            tis_to_keep = tis_to_keep_query.all()

            filter_tis = [
                not_(
                    and_(
                        cls.dag_id == ti.dag_id,
                        cls.task_id == ti.task_id,
                        cls.execution_date == ti.execution_date,
                    )
                )
                for ti in tis_to_keep
            ]

            session.query(cls).filter(and_(*filter_tis)).delete(synchronize_session=False)
