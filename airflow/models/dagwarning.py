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
from enum import Enum

from sqlalchemy import Column, ForeignKeyConstraint, String, Text, false

from airflow.models.base import Base, StringID
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime


class DagWarning(Base):
    """
    A table to store DAG warnings.

    DAG warnings are problems that don't rise to the level of failing the DAG parse
    but which users should nonetheless be warned about.  These warnings are recorded
    when parsing DAG and displayed on the Webserver in a flash message.
    """

    dag_id = Column(StringID(), primary_key=True)
    warning_type = Column(String(50), primary_key=True)
    message = Column(Text, nullable=False)
    timestamp = Column(UtcDateTime, nullable=False, default=timezone.utcnow)

    __tablename__ = "dag_warning"
    __table_args__ = (
        ForeignKeyConstraint(
            ('dag_id',),
            ['dag.dag_id'],
            name='dcw_dag_id_fkey',
            ondelete='CASCADE',
        ),
    )

    def __init__(self, dag_id, error_type, message, **kwargs):
        super().__init__(**kwargs)
        self.dag_id = dag_id
        self.warning_type = DagWarningType(error_type).value  # make sure valid type
        self.message = message

    def __eq__(self, other):
        return self.dag_id == other.dag_id and self.warning_type == other.warning_type

    def __hash__(self):
        return hash((self.dag_id, self.warning_type))

    @classmethod
    @provide_session
    def purge_inactive_dag_warnings(cls, session=NEW_SESSION):
        """
        Deactivate DagWarning records for inactive dags.

        :return: None
        """
        from airflow.models.dag import DagModel

        if session.get_bind().dialect.name == 'sqlite':
            dag_ids = session.query(DagModel).filter(DagModel.is_active == false()).all()
            session.query(cls).filter(cls.dag_id.in_(dag_ids)).delete(synchronize_session=False)
        else:
            session.query(cls).filter(cls.dag_id == DagModel.dag_id, DagModel.is_active == false()).delete(
                synchronize_session=False
            )
        session.commit()


class DagWarningType(str, Enum):
    """
    Enum for DAG warning types.

    This is the set of allowable values for the ``warning_type`` field
    in the DagWarning model.
    """

    NONEXISTENT_POOL = 'non-existent pool'
