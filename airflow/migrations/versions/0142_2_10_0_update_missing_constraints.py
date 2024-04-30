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

"""Update missing constraints

Revision ID: 686269002441
Revises: 677fdbb7fc54
Create Date: 2024-04-15 14:19:49.913797

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import literal

# revision identifiers, used by Alembic.
revision = '686269002441'
down_revision = '677fdbb7fc54'
branch_labels = None
depends_on = None
airflow_version = '2.10.0'


def upgrade():
    """Apply Update missing constraints"""
    conn = op.get_bind()
    if conn.dialect.name == "sqlite":
        # SQLite does not support DROP CONSTRAINT
        return
    if conn.dialect.name == "mysql":
        # TODO: Rewrite these queries to use alembic when lowest MYSQL version supports IF EXISTS
        conn.execute(sa.text("""
        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
            CONSTRAINT_SCHEMA = DATABASE() AND
            TABLE_NAME        = 'connection' AND
            CONSTRAINT_NAME   = 'unique_conn_id' AND
            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE connection
            drop constraint unique_conn_id','select 1');

        prepare stmt from @var;
        execute stmt;
        deallocate prepare stmt;
        """))
        # Dropping the below and recreating cause there's no IF NOT EXISTS in mysql
        conn.execute(sa.text("""
                set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                    CONSTRAINT_SCHEMA = DATABASE() AND
                    TABLE_NAME        = 'connection' AND
                    CONSTRAINT_NAME   = 'connection_conn_id_uq' AND
                    CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE connection
                    drop constraint connection_conn_id_uq','select 1');

                prepare stmt from @var;
                execute stmt;
                deallocate prepare stmt;
                """))
    else:
        op.execute("ALTER TABLE connection DROP CONSTRAINT IF EXISTS unique_conn_id")
        # Dropping and recreating cause there's no IF NOT EXISTS
        op.execute("ALTER TABLE connection DROP CONSTRAINT IF EXISTS connection_conn_id_uq")
    with op.batch_alter_table('connection') as batch_op:
        batch_op.create_unique_constraint(batch_op.f('connection_conn_id_uq'), ['conn_id'])

    max_cons = sa.table('dag', sa.column('max_consecutive_failed_dag_runs'))
    op.execute(max_cons.update().values(max_consecutive_failed_dag_runs=literal("0")))
    with op.batch_alter_table('dag') as batch_op:
        batch_op.alter_column('max_consecutive_failed_dag_runs', existing_type=sa.Integer(), nullable=False)

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_dag_run_fkey", type_="foreignkey")
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_dr_fkey", type_="foreignkey")

    if conn.dialect.name == "mysql":
        conn.execute(sa.text("""
                        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                            CONSTRAINT_SCHEMA = DATABASE() AND
                            TABLE_NAME        = 'dag_run' AND
                            CONSTRAINT_NAME   = 'dag_run_dag_id_execution_date_uq' AND
                            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                            drop constraint dag_run_dag_id_execution_date_uq','select 1');

                        prepare stmt from @var;
                        execute stmt;
                        deallocate prepare stmt;
                        """))
        conn.execute(sa.text("""
                        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                            CONSTRAINT_SCHEMA = DATABASE() AND
                            TABLE_NAME        = 'dag_run' AND
                            CONSTRAINT_NAME   = 'dag_run_dag_id_run_id_uq' AND
                            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                            drop constraint dag_run_dag_id_run_id_uq','select 1');

                        prepare stmt from @var;
                        execute stmt;
                        deallocate prepare stmt;
                        """))
        # below we drop and recreate the constraints because there's no IF NOT EXISTS
        conn.execute(sa.text("""
                                set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                                    CONSTRAINT_SCHEMA = DATABASE() AND
                                    TABLE_NAME        = 'dag_run' AND
                                    CONSTRAINT_NAME   = 'dag_run_dag_id_execution_date_key' AND
                                    CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                                    drop constraint dag_run_dag_id_execution_date_key','select 1');

                                prepare stmt from @var;
                                execute stmt;
                                deallocate prepare stmt;
                                """))
        conn.execute(sa.text("""
                            set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                                CONSTRAINT_SCHEMA = DATABASE() AND
                                TABLE_NAME        = 'dag_run' AND
                                CONSTRAINT_NAME   = 'dag_run_dag_id_run_id_key' AND
                                CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                                drop constraint dag_run_dag_id_run_id_key','select 1');

                            prepare stmt from @var;
                            execute stmt;
                            deallocate prepare stmt;
                            """))
    else:
        op.execute("ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_uq")
        op.execute("ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_uq")
        # below we drop and recreate the constraints because there's no IF NOT EXISTS
        op.execute("ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_key")
        op.execute("ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_key")
    with op.batch_alter_table('dag_run') as batch_op:
        batch_op.create_unique_constraint('dag_run_dag_id_execution_date_key', ['dag_id', 'execution_date'])
        batch_op.create_unique_constraint('dag_run_dag_id_run_id_key', ['dag_id', 'run_id'])
    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.create_foreign_key("task_instance_dag_run_fkey", "dag_run", ["dag_id", "run_id"], ["dag_id", "run_id"],
                                    ondelete="CASCADE")
    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.create_foreign_key(
            "task_reschedule_dr_fkey",
            "dag_run",
            ["dag_id", "run_id"],
            ["dag_id", "run_id"],
            ondelete="CASCADE",
        )


def downgrade():
    """NO downgrade because this is to make ORM consistent with the database."""
