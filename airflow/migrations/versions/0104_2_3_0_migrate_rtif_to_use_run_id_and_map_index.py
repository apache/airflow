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
"""Migrate RTIF to use run_id and map_index

Revision ID: 4eaab2fe6582
Revises: c97c2ab6aa23
Create Date: 2022-03-03 17:48:29.955821

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import and_, select
from sqlalchemy.sql.schema import ForeignKeyConstraint

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.migrations.utils import get_mssql_table_constraints

ID_LEN = 250

# revision identifiers, used by Alembic.
revision = "4eaab2fe6582"
down_revision = "c97c2ab6aa23"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


# Just Enough Table to run the conditions for update.
def tables(for_downgrade=False):
    import sqlalchemy_jsonfield

    global task_instance, rendered_task_instance_fields, dag_run
    metadata = sa.MetaData()
    task_instance = sa.Table(
        "task_instance",
        metadata,
        sa.Column("task_id", StringID()),
        sa.Column("dag_id", StringID()),
        sa.Column("run_id", StringID()),
        sa.Column("execution_date", TIMESTAMP),
    )
    rendered_task_instance_fields = sa.Table(
        "rendered_task_instance_fields",
        metadata,
        sa.Column("dag_id", StringID()),
        sa.Column("task_id", StringID()),
        sa.Column("run_id", StringID()),
        sa.Column("execution_date", TIMESTAMP),
        sa.Column("rendered_fields", sqlalchemy_jsonfield.JSONField(), nullable=False),
        sa.Column("k8s_pod_yaml", sqlalchemy_jsonfield.JSONField(), nullable=True),
    )

    if for_downgrade:
        rendered_task_instance_fields.append_column(
            sa.Column("map_index", sa.Integer(), server_default="-1"),
        )
        rendered_task_instance_fields.append_constraint(
            ForeignKeyConstraint(
                ["dag_id", "run_id"],
                ["dag_run.dag_id", "dag_run.run_id"],
                name="rtif_dag_run_fkey",
                ondelete="CASCADE",
            ),
        )
    dag_run = sa.Table(
        "dag_run",
        metadata,
        sa.Column("dag_id", StringID()),
        sa.Column("run_id", StringID()),
        sa.Column("execution_date", TIMESTAMP),
    )


def _multi_table_update(dialect_name, target, column):
    condition = dag_run.c.dag_id == target.c.dag_id

    if column == target.c.run_id:
        condition = and_(condition, dag_run.c.execution_date == target.c.execution_date)
    else:
        condition = and_(condition, dag_run.c.run_id == target.c.run_id)

    if dialect_name == "sqlite":
        # Most SQLite versions don't support multi table update (and SQLA doesn't know about it anyway), so we
        # need to do a Correlated subquery update
        sub_q = select(dag_run.c[column.name]).where(condition).scalar_subquery()

        return target.update().values({column: sub_q})
    else:
        return target.update().where(condition).values({column: dag_run.c[column.name]})


def upgrade():
    tables()
    dialect_name = op.get_bind().dialect.name

    with op.batch_alter_table("rendered_task_instance_fields") as batch_op:
        batch_op.add_column(sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False))
        rendered_task_instance_fields.append_column(
            sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False)
        )
        batch_op.add_column(sa.Column("run_id", type_=StringID(), nullable=True))

    update_query = _multi_table_update(
        dialect_name, rendered_task_instance_fields, rendered_task_instance_fields.c.run_id
    )
    op.execute(update_query)
    with op.batch_alter_table(
        "rendered_task_instance_fields", copy_from=rendered_task_instance_fields
    ) as batch_op:
        if dialect_name == "mssql":
            constraints = get_mssql_table_constraints(op.get_bind(), "rendered_task_instance_fields")
            pk, _ = constraints["PRIMARY KEY"].popitem()
            batch_op.drop_constraint(pk, type_="primary")
        elif dialect_name != "sqlite":
            batch_op.drop_constraint("rendered_task_instance_fields_pkey", type_="primary")
        batch_op.alter_column("run_id", existing_type=StringID(), existing_nullable=True, nullable=False)
        batch_op.drop_column("execution_date")
        batch_op.create_primary_key(
            "rendered_task_instance_fields_pkey", ["dag_id", "task_id", "run_id", "map_index"]
        )
        batch_op.create_foreign_key(
            "rtif_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )


def downgrade():
    tables(for_downgrade=True)
    dialect_name = op.get_bind().dialect.name
    op.add_column("rendered_task_instance_fields", sa.Column("execution_date", TIMESTAMP, nullable=True))

    update_query = _multi_table_update(
        dialect_name, rendered_task_instance_fields, rendered_task_instance_fields.c.execution_date
    )
    op.execute(update_query)

    with op.batch_alter_table(
        "rendered_task_instance_fields", copy_from=rendered_task_instance_fields
    ) as batch_op:
        batch_op.alter_column("execution_date", existing_type=TIMESTAMP, nullable=False)
        if dialect_name != "sqlite":
            batch_op.drop_constraint("rtif_ti_fkey", type_="foreignkey")
            batch_op.drop_constraint("rendered_task_instance_fields_pkey", type_="primary")
        batch_op.create_primary_key(
            "rendered_task_instance_fields_pkey", ["dag_id", "task_id", "execution_date"]
        )
        batch_op.drop_column("map_index", mssql_drop_default=True)
        batch_op.drop_column("run_id")
