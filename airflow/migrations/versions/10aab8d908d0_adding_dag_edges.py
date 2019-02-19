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
"""Adding dag edges

Revision ID: 10aab8d908d0
Revises: c8ffec048a3b
Create Date: 2018-12-28 09:11:56.730564

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
from airflow.models import DagModel, DagRun

revision = "10aab8d908d0"
down_revision = ('a56c9515abdc', 'dd4ecb8fbee3')
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=connection)

    op.create_table(
        "dag_edge",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("graph_id", sa.Integer, nullable=False),
        sa.Column("from_task", sa.String(length=250), nullable=False),
        sa.Column("to_task", sa.String(length=250), nullable=False),
        sa.PrimaryKeyConstraint("dag_id", "graph_id", "from_task", "to_task"),
    )

    op.create_index('idx_dag_edge', 'dag_edge',
                    ['dag_id', 'graph_id'],
                    unique=False)

    op.add_column("task_instance", sa.Column("ui_color", sa.String(10), nullable=True))
    op.add_column(
        "task_instance", sa.Column("ui_fgcolor", sa.String(10), nullable=True)
    )
    op.add_column("dag", sa.Column("parent_dag", sa.String(250), nullable=True))
    op.add_column("dag_run", sa.Column("graph_id", sa.Integer, nullable=True))

    dag_models = session.query(DagModel).all()
    session.query(DagRun).filter(DagRun.graph_id is None).update({"graph_id": 0})
    for dag_model in dag_models:
        first_run = session.query(DagRun)\
            .filter(DagRun.dag_id == dag_model.dag_id).first()
        if first_run is not None:
            dag = dag_model.get_dag()
            if dag is not None:
                edges = dag.create_edges(graph_id=0)
                session.add_all(edges)
        session.commit()


def downgrade():
    op.drop_index('idx_dag_edge', table_name='dag_edge')
    op.drop_table("dag_edge")
    op.drop_column("dag_run", "graph_id")
    op.drop_column("task_instance", "ui_color")
    op.drop_column("task_instance", "ui_fgcolor")
    op.drop_column("dag", "parent_dag")
