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
"""Add ``LogTemplate`` table to track changes to config values ``log_filename_template``

Revision ID: f9da662e7089
Revises: 786e3737b18f
Create Date: 2021-12-09 06:11:21.044940
"""
from __future__ import annotations

from alembic import op
from sqlalchemy import Column, ForeignKey, Integer, Text

from airflow.migrations.utils import disable_sqlite_fkeys
from airflow.utils.sqlalchemy import UtcDateTime

# Revision identifiers, used by Alembic.
revision = "f9da662e7089"
down_revision = "786e3737b18f"
branch_labels = None
depends_on = None
airflow_version = "2.3.0"


def upgrade():
    """Add model for task log template and establish fk on task instance."""
    op.create_table(
        "log_template",
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("filename", Text, nullable=False),
        Column("elasticsearch_id", Text, nullable=False),
        Column("created_at", UtcDateTime, nullable=False),
    )
    dag_run_log_filename_id = Column(
        "log_template_id",
        Integer,
        ForeignKey("log_template.id", name="task_instance_log_template_id_fkey", ondelete="NO ACTION"),
    )

    with disable_sqlite_fkeys(op), op.batch_alter_table("dag_run") as batch_op:
        batch_op.add_column(dag_run_log_filename_id)


def downgrade():
    """Remove fk on task instance and model for task log filename template."""
    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.drop_constraint("task_instance_log_template_id_fkey", type_="foreignkey")
        batch_op.drop_column("log_template_id")
    op.drop_table("log_template")
