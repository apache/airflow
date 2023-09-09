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
"""Save Rendered Template Fields."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

import sqlalchemy_jsonfield
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    delete,
    exists,
    select,
    text,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.serialization.helpers import serialize_template_field
from airflow.settings import json
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import FromClause

    from airflow.models.taskinstance import TaskInstance


class RenderedTaskInstanceFields(Base):
    """Save Rendered Template Fields."""

    __tablename__ = "rendered_task_instance_fields"

    dag_id = Column(StringID(), primary_key=True)
    task_id = Column(StringID(), primary_key=True)
    run_id = Column(StringID(), primary_key=True)
    map_index = Column(Integer, primary_key=True, server_default=text("-1"))
    rendered_fields = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False)
    k8s_pod_yaml = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            name="rendered_task_instance_fields_pkey",
            mssql_clustered=True,
        ),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="rtif_ti_fkey",
            ondelete="CASCADE",
        ),
    )
    task_instance = relationship(
        "TaskInstance",
        lazy="joined",
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
            # we can safely import it here from provider. In Airflow 2.7.0+ you need to have new version
            # of kubernetes provider installed to reach this place
            from airflow.providers.cncf.kubernetes.template_rendering import render_k8s_pod_yaml

            self.k8s_pod_yaml = render_k8s_pod_yaml(ti)
        self.rendered_fields = {
            field: serialize_template_field(getattr(self.task, field)) for field in self.task.template_fields
        }

        self._redact()

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + ">"

    def _redact(self):
        from airflow.utils.log.secrets_masker import redact

        if self.k8s_pod_yaml:
            self.k8s_pod_yaml = redact(self.k8s_pod_yaml)

        for field, rendered in self.rendered_fields.items():
            self.rendered_fields[field] = redact(rendered, field)

    @classmethod
    @provide_session
    def get_templated_fields(cls, ti: TaskInstance, session: Session = NEW_SESSION) -> dict | None:
        """
        Get templated field for a TaskInstance from the RenderedTaskInstanceFields table.

        :param ti: Task Instance
        :param session: SqlAlchemy Session
        :return: Rendered Templated TI field
        """
        result = session.scalar(
            select(cls).where(
                cls.dag_id == ti.dag_id,
                cls.task_id == ti.task_id,
                cls.run_id == ti.run_id,
                cls.map_index == ti.map_index,
            )
        )

        if result:
            rendered_fields = result.rendered_fields
            return rendered_fields
        else:
            return None

    @classmethod
    @provide_session
    def get_k8s_pod_yaml(cls, ti: TaskInstance, session: Session = NEW_SESSION) -> dict | None:
        """
        Get rendered Kubernetes Pod Yaml for a TaskInstance from the RenderedTaskInstanceFields table.

        :param ti: Task Instance
        :param session: SqlAlchemy Session
        :return: Kubernetes Pod Yaml
        """
        result = session.scalar(
            select(cls).where(
                cls.dag_id == ti.dag_id,
                cls.task_id == ti.task_id,
                cls.run_id == ti.run_id,
                cls.map_index == ti.map_index,
            )
        )
        return result.k8s_pod_yaml if result else None

    @provide_session
    @retry_db_transaction
    def write(self, session: Session = None):
        """Write instance to database.

        :param session: SqlAlchemy Session
        """
        session.merge(self)

    @classmethod
    @provide_session
    def delete_old_records(
        cls,
        task_id: str,
        dag_id: str,
        num_to_keep: int = conf.getint("core", "max_num_rendered_ti_fields_per_task", fallback=0),
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Keep only Last X (num_to_keep) number of records for a task by deleting others.

        In the case of data for a mapped task either all of the rows or none of the rows will be deleted, so
        we don't end up with partial data for a set of mapped Task Instances left in the database.

        :param task_id: Task ID
        :param dag_id: Dag ID
        :param num_to_keep: Number of Records to keep
        :param session: SqlAlchemy Session
        """
        if num_to_keep <= 0:
            return

        from airflow.models.dagrun import DagRun

        tis_to_keep_query = (
            select(cls.dag_id, cls.task_id, cls.run_id, DagRun.execution_date)
            .where(cls.dag_id == dag_id, cls.task_id == task_id)
            .join(cls.dag_run)
            .distinct()
            .order_by(DagRun.execution_date.desc())
            .limit(num_to_keep)
        )

        cls._do_delete_old_records(
            dag_id=dag_id,
            task_id=task_id,
            ti_clause=tis_to_keep_query.subquery(),
            session=session,
        )
        session.flush()

    @classmethod
    @retry_db_transaction
    def _do_delete_old_records(
        cls,
        *,
        task_id: str,
        dag_id: str,
        ti_clause: FromClause,
        session: Session,
    ) -> None:
        # This query might deadlock occasionally and it should be retried if fails (see decorator)

        stmt = (
            delete(cls)
            .where(
                cls.dag_id == dag_id,
                cls.task_id == task_id,
                ~exists(1).where(
                    ti_clause.c.dag_id == cls.dag_id,
                    ti_clause.c.task_id == cls.task_id,
                    ti_clause.c.run_id == cls.run_id,
                ),
            )
            .execution_options(synchronize_session=False)
        )

        session.execute(stmt)
