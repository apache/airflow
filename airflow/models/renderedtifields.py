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
import os
from typing import Optional

import sqlalchemy_jsonfield
from sqlalchemy import Column, ForeignKeyConstraint, Integer, and_, not_, tuple_
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Session, relationship

from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.helpers import serialize_template_field
from airflow.settings import json
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session


class RenderedTaskInstanceFields(Base):
    """Save Rendered Template Fields"""

    __tablename__ = "rendered_task_instance_fields"

    dag_id = Column(StringID(), primary_key=True)
    task_id = Column(StringID(), primary_key=True)
    run_id = Column(StringID(), primary_key=True)
    map_index = Column(Integer, primary_key=True, server_default='-1')
    rendered_fields = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    k8s_pod_yaml = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name='rtif_ti_fkey',
            ondelete="CASCADE",
        ),
    )
    task_instance = relationship(
        "TaskInstance",
        lazy='joined',
        back_populates="rendered_task_instance_fields",
    )

    # We don't need a DB level FK here, as we already have that to TI (which has one to DR) but by defining
    # the relationship we can more easily find the execution date for these rows
    dag_run = relationship(
        "DagRun",
        primaryjoin="""and_(
            RenderedTaskInstanceFields.dag_id == foreign(DagRun.dag_id),
            RenderedTaskInstanceFields.run_id == foreign(DagRun.run_id),
        )""",
        viewonly=True,
    )

    execution_date = association_proxy("dag_run", "execution_date")

    def __init__(self, ti: TaskInstance, render_templates=True):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.run_id = ti.run_id
        self.map_index = ti.map_index
        self.ti = ti
        if render_templates:
            ti.render_templates()
        self.task = ti.task
        if os.environ.get("AIRFLOW_IS_K8S_EXECUTOR_POD", None):
            self.k8s_pod_yaml = ti.render_k8s_pod_yaml()
        self.rendered_fields = {
            field: serialize_template_field(getattr(self.task, field)) for field in self.task.template_fields
        }

        self._redact()

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + '>'

    def _redact(self):
        from airflow.utils.log.secrets_masker import redact

        if self.k8s_pod_yaml:
            self.k8s_pod_yaml = redact(self.k8s_pod_yaml)

        for field, rendered in self.rendered_fields.items():
            self.rendered_fields[field] = redact(rendered, field)

    @classmethod
    @provide_session
    def get_templated_fields(cls, ti: TaskInstance, session: Session = NEW_SESSION) -> Optional[dict]:
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
                cls.dag_id == ti.dag_id,
                cls.task_id == ti.task_id,
                cls.run_id == ti.run_id,
                cls.map_index == ti.map_index,
            )
            .one_or_none()
        )

        if result:
            rendered_fields = result.rendered_fields
            return rendered_fields
        else:
            return None

    @classmethod
    @provide_session
    def get_k8s_pod_yaml(cls, ti: TaskInstance, session: Session = NEW_SESSION) -> Optional[dict]:
        """
        Get rendered Kubernetes Pod Yaml for a TaskInstance from the RenderedTaskInstanceFields
        table.

        :param ti: Task Instance
        :param session: SqlAlchemy Session
        :return: Kubernetes Pod Yaml
        """
        result = (
            session.query(cls.k8s_pod_yaml)
            .filter(
                cls.dag_id == ti.dag_id,
                cls.task_id == ti.task_id,
                cls.run_id == ti.run_id,
                cls.map_index == ti.map_index,
            )
            .one_or_none()
        )
        return result.k8s_pod_yaml if result else None

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
        Keep only Last X (num_to_keep) number of records for a task by deleting others.

        In the case of data for a mapped task either all of the rows or none of the rows will be deleted, so
        we don't end up with partial data for a set of mapped Task Instances left in the database.

        :param task_id: Task ID
        :param dag_id: Dag ID
        :param num_to_keep: Number of Records to keep
        :param session: SqlAlchemy Session
        """
        from airflow.models.dagrun import DagRun

        if num_to_keep <= 0:
            return

        tis_to_keep_query = (
            session.query(cls.dag_id, cls.task_id, cls.run_id)
            .filter(cls.dag_id == dag_id, cls.task_id == task_id)
            .join(cls.dag_run)
            .distinct()
            .order_by(DagRun.execution_date.desc())
            .limit(num_to_keep)
        )

        if session.bind.dialect.name in ["postgresql", "sqlite"]:
            # Fetch Top X records given dag_id & task_id ordered by Execution Date
            subq1 = tis_to_keep_query.subquery()
            excluded = session.query(subq1.c.dag_id, subq1.c.task_id, subq1.c.run_id)
            session.query(cls).filter(
                cls.dag_id == dag_id,
                cls.task_id == task_id,
                tuple_(cls.dag_id, cls.task_id, cls.run_id).notin_(excluded),
            ).delete(synchronize_session=False)
        elif session.bind.dialect.name in ["mysql"]:
            cls._remove_old_rendered_ti_fields_mysql(dag_id, session, task_id, tis_to_keep_query)
        else:
            # Fetch Top X records given dag_id & task_id ordered by Execution Date
            tis_to_keep = tis_to_keep_query.all()

            filter_tis = [
                not_(
                    and_(
                        cls.dag_id == ti.dag_id,
                        cls.task_id == ti.task_id,
                        cls.run_id == ti.run_id,
                    )
                )
                for ti in tis_to_keep
            ]

            session.query(cls).filter(and_(*filter_tis)).delete(synchronize_session=False)

        session.flush()

    @classmethod
    @retry_db_transaction
    def _remove_old_rendered_ti_fields_mysql(cls, dag_id, session, task_id, tis_to_keep_query):
        # Fetch Top X records given dag_id & task_id ordered by Execution Date
        subq1 = tis_to_keep_query.subquery('subq1')
        # Second Subquery
        # Workaround for MySQL Limitation (https://stackoverflow.com/a/19344141/5691525)
        # Limitation: This version of MySQL does not yet support
        # LIMIT & IN/ALL/ANY/SOME subquery
        subq2 = session.query(subq1.c.dag_id, subq1.c.task_id, subq1.c.run_id).subquery('subq2')
        # This query might deadlock occasionally and it should be retried if fails (see decorator)
        session.query(cls).filter(
            cls.dag_id == dag_id,
            cls.task_id == task_id,
            tuple_(cls.dag_id, cls.task_id, cls.run_id).notin_(subq2),
        ).delete(synchronize_session=False)
