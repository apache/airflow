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

"""
Add ti_id to TaskReschedule.

Revision ID: c41fa20116a8
Revises: 038dc8bc6284
Create Date: 2024-12-17 22:41:03.699609

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from airflow.models.base import COLLATION_ARGS, ID_LEN, StringID

# revision identifiers, used by Alembic.
revision = "c41fa20116a8"
down_revision = "038dc8bc6284"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Add ti_id to TaskReschedule, BaseXcom, TaskMap, TaskInstanceNote."""
    # task_instance_note
    with op.batch_alter_table("task_instance_note") as batch_op:
        batch_op.drop_constraint("task_instance_note_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("task_instance_note_pkey", type_="primary")

        for column in ["task_id", "run_id", "dag_id", "map_index"]:
            batch_op.drop_column(column)

        batch_op.add_column(
            sa.Column(
                "ti_id",
                sa.String(36, **COLLATION_ARGS).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                sa.ForeignKey(
                    "task_instance.id", name=batch_op.f("task_instance_note_ti_fkey"), ondelete="CASCADE"
                ),
                nullable=False,
                primary_key=True,
            ),
        )

    # task_reschedule
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_ti_fkey", type_="foreignkey")
        batch_op.drop_index("idx_task_reschedule_dag_task_run")

        for column in ["task_id", "map_index"]:
            batch_op.drop_column(column)

        batch_op.add_column(
            sa.Column(
                "ti_id",
                sa.String(36, **COLLATION_ARGS).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                sa.ForeignKey(
                    "task_instance.id", name=batch_op.f("task_reschedule_ti_fkey"), ondelete="CASCADE"
                ),
                nullable=False,
            ),
        )

        batch_op.create_index("idx_task_reschedule_ti", ["ti_id"], unique=False)

    # rendered_task_instance_fields
    with op.batch_alter_table("rendered_task_instance_fields") as batch_op:
        batch_op.drop_constraint("rtif_ti_fkey", type_="foreignkey")
        batch_op.drop_constraint("rendered_task_instance_fields_pkey", type_="primary")

        for column in ["task_id", "map_index"]:
            batch_op.drop_column(column)

        batch_op.add_column(
            sa.Column(
                "ti_id",
                sa.String(36, **COLLATION_ARGS).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                sa.ForeignKey(
                    "task_instance.id",
                    name=batch_op.f("rendered_task_instance_fields_ti_fkey"),
                    ondelete="CASCADE",
                ),
                nullable=False,
                primary_key=True,
            ),
        )
        batch_op.create_primary_key("rendered_task_instance_fields_pkey", ["ti_id", "dag_id", "run_id"])

    # xcom
    with op.batch_alter_table("xcom") as batch_op:
        batch_op.drop_constraint("xcom_task_instance_fkey", type_="foreignkey")
        batch_op.drop_constraint("xcom_pkey", type_="primary")
        batch_op.drop_index("idx_xcom_task_instance")

        for column in ["task_id", "run_id", "dag_id", "map_index"]:
            batch_op.drop_column(column)

        batch_op.add_column(
            sa.Column(
                "ti_id",
                sa.String(36, **COLLATION_ARGS).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                nullable=False,
                primary_key=True,
            ),
        )
        batch_op.create_primary_key("xcom_pkey", ["ti_id", "key", "dag_run_id"])
        batch_op.create_foreign_key(
            "xcom_task_instance_fkey", "task_instance", ["ti_id"], ["ti_id"], ondelete="CASCADE"
        )

        batch_op.create_index("idx_xcom_task_instance", ["ti_id"])

    # task_map
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_constraint("task_map_task_instance_fkey", type_="foreignkey")

        for column in ["task_id", "run_id", "dag_id", "map_index"]:
            batch_op.drop_column(column)

        batch_op.add_column(
            sa.Column(
                "ti_id",
                sa.String(36, **COLLATION_ARGS).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
                sa.ForeignKey(
                    "task_instance.id",
                    name=batch_op.f("task_map_task_instance_fkey"),
                    ondelete="CASCADE",
                    onupdate="CASCADE",
                ),
                nullable=False,
                primary_key=True,
            ),
        )


def downgrade() -> None:
    """Remove ti_id to TaskReschedule, BaseXcom, TaskMap, TaskInstanceNote."""
    # task_instance_note
    with op.batch_alter_table("task_instance_note") as batch_op:
        batch_op.drop_column("ti_id")

        batch_op.add_column(sa.Column("task_id", StringID(), nullable=False, primary_key=True))
        batch_op.add_column(sa.Column("dag_id", StringID(), nullable=False, primary_key=True))
        batch_op.add_column(sa.Column("run_id", StringID(), nullable=False, primary_key=True))
        batch_op.add_column(sa.Column("map_index", StringID(), nullable=False, primary_key=True))

        batch_op.create_primary_key(
            batch_op.f("task_instance_note_pkey"), ["task_id", "dag_id", "run_id", "map_index"]
        )

        batch_op.create_foreign_key(
            "task_instance_note_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )

    # task_reschedule
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_index("idx_task_reschedule_ti")
        batch_op.drop_column("ti_id")

        batch_op.add_column(sa.Column("task_id", sa.String(ID_LEN, **COLLATION_ARGS), nullable=False))
        batch_op.add_column(sa.Column("map_index", sa.Integer, nullable=False, server_default=sa.text("-1")))

        batch_op.create_foreign_key(
            "task_reschedule_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )
        batch_op.create_index(
            "idx_task_reschedule_dag_task_run", ["dag_id", "task_id", "run_id", "map_index"], unique=False
        )

    # rendered_task_instance_fields
    with op.batch_alter_table("rendered_task_instance_fields") as batch_op:
        batch_op.drop_column("ti_id")
        batch_op.drop_constraint("rendered_task_instance_fields_pkey")

        batch_op.add_column(sa.Column("task_id", StringID(), nullable=False, primary_key=True))
        batch_op.add_column(sa.Column("map_index", StringID(), nullable=False, primary_key=True))

        batch_op.create_primary_key(
            batch_op.f("rendered_task_instance_fields_pkey"), ["task_id", "dag_id", "run_id", "map_index"]
        )

        batch_op.create_foreign_key(
            "rtif_ti_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )

    # xcom
    with op.batch_alter_table("xcom") as batch_op:
        batch_op.drop_constraint("xcom_task_instance_fkey", type_="foreignkey")
        batch_op.drop_constraint("xcom_pkey", type_="primary")

        batch_op.drop_index("idx_xcom_task_instance")

        batch_op.drop_column("ti_id")

        batch_op.add_column(
            sa.Column("task_id", sa.String(ID_LEN, **COLLATION_ARGS), nullable=False, primary_key=True)
        )
        batch_op.add_column(sa.Column("dag_id", sa.String(ID_LEN, **COLLATION_ARGS), nullable=False))
        batch_op.add_column(sa.Column("run_id", sa.String(ID_LEN, **COLLATION_ARGS), nullable=False))
        batch_op.add_column(
            sa.Column("map_index", sa.Integer, primary_key=True, nullable=False, server_default=sa.text("-1"))
        )
        batch_op.create_primary_key("xcom_pkey", ["dag_run_id", "task_id", "map_index", "key"])

        batch_op.create_foreign_key(
            "xcom_task_instance_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
        )

        batch_op.create_index(
            "idx_xcom_task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
        )

    # task_map
    with op.batch_alter_table("task_map") as batch_op:
        batch_op.drop_column("ti_id")

        batch_op.add_column(sa.Column("task_id", sa.String(ID_LEN, **COLLATION_ARGS), primary_key=True))
        batch_op.add_column(sa.Column("dag_id", sa.String(ID_LEN, **COLLATION_ARGS), primary_key=True))
        batch_op.add_column(sa.Column("run_id", sa.String(ID_LEN, **COLLATION_ARGS), primary_key=True))
        batch_op.add_column(sa.Column("map_index", sa.Integer, primary_key=True))

        batch_op.create_foreign_key(
            "task_map_task_instance_fkey",
            "task_instance",
            ["dag_id", "task_id", "run_id", "map_index"],
            ["dag_id", "task_id", "run_id", "map_index"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )
