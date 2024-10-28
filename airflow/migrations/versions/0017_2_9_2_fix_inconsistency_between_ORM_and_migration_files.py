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
Fix inconsistency between ORM and migration files.

Revision ID: 686269002441
Revises: bff083ad727d
Create Date: 2024-04-15 14:19:49.913797

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import literal

# revision identifiers, used by Alembic.
revision = "686269002441"
down_revision = "bff083ad727d"
branch_labels = None
depends_on = None
airflow_version = "2.9.2"


def upgrade():
    """Apply Update missing constraints."""
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        # TODO: Rewrite these queries to use alembic when lowest MYSQL version supports IF EXISTS
        conn.execute(
            sa.text("""
        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
            CONSTRAINT_SCHEMA = DATABASE() AND
            TABLE_NAME        = 'connection' AND
            CONSTRAINT_NAME   = 'unique_conn_id' AND
            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE connection
            DROP INDEX unique_conn_id','select 1');

        prepare stmt from @var;
        execute stmt;
        deallocate prepare stmt;
        """)
        )
        # Dropping the below and recreating cause there's no IF NOT EXISTS in mysql
        conn.execute(
            sa.text("""
                set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                    CONSTRAINT_SCHEMA = DATABASE() AND
                    TABLE_NAME        = 'connection' AND
                    CONSTRAINT_NAME   = 'connection_conn_id_uq' AND
                    CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE connection
                    DROP INDEX connection_conn_id_uq','select 1');

                prepare stmt from @var;
                execute stmt;
                deallocate prepare stmt;
                """)
        )
    elif conn.dialect.name == "sqlite":
        # SQLite does not support DROP CONSTRAINT
        # We have to recreate the table without the constraint
        conn.execute(sa.text("PRAGMA foreign_keys=off"))
        conn.execute(
            sa.text("""
        CREATE TABLE connection_new (
                id INTEGER NOT NULL,
                conn_id VARCHAR(250) NOT NULL,
                conn_type VARCHAR(500) NOT NULL,
                host VARCHAR(500),
                schema VARCHAR(500),
                login TEXT,
                password TEXT,
                port INTEGER,
                extra TEXT,
                is_encrypted BOOLEAN,
                is_extra_encrypted BOOLEAN,
                description VARCHAR(5000),
                CONSTRAINT connection_pkey PRIMARY KEY (id),
                CONSTRAINT connection_conn_id_uq UNIQUE (conn_id)
        )
        """)
        )
        conn.execute(sa.text("INSERT INTO connection_new SELECT * FROM connection"))
        conn.execute(sa.text("DROP TABLE connection"))
        conn.execute(sa.text("ALTER TABLE connection_new RENAME TO connection"))
        conn.execute(sa.text("PRAGMA foreign_keys=on"))
    else:
        op.execute("ALTER TABLE connection DROP CONSTRAINT IF EXISTS unique_conn_id")
        # Dropping and recreating cause there's no IF NOT EXISTS
        op.execute(
            "ALTER TABLE connection DROP CONSTRAINT IF EXISTS connection_conn_id_uq"
        )

    with op.batch_alter_table("connection") as batch_op:
        batch_op.create_unique_constraint(
            batch_op.f("connection_conn_id_uq"), ["conn_id"]
        )

    max_cons = sa.table("dag", sa.column("max_consecutive_failed_dag_runs"))
    op.execute(max_cons.update().values(max_consecutive_failed_dag_runs=literal("0")))
    with op.batch_alter_table("dag") as batch_op:
        batch_op.alter_column(
            "max_consecutive_failed_dag_runs", existing_type=sa.Integer(), nullable=False
        )

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.drop_constraint("task_instance_dag_run_fkey", type_="foreignkey")

    with op.batch_alter_table("task_reschedule") as batch_op:
        batch_op.drop_constraint("task_reschedule_dr_fkey", type_="foreignkey")

    if conn.dialect.name == "mysql":
        conn.execute(
            sa.text("""
                        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                            CONSTRAINT_SCHEMA = DATABASE() AND
                            TABLE_NAME        = 'dag_run' AND
                            CONSTRAINT_NAME   = 'dag_run_dag_id_execution_date_uq' AND
                            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                            DROP INDEX dag_run_dag_id_execution_date_uq','select 1');

                        prepare stmt from @var;
                        execute stmt;
                        deallocate prepare stmt;
                        """)
        )
        conn.execute(
            sa.text("""
                        set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                            CONSTRAINT_SCHEMA = DATABASE() AND
                            TABLE_NAME        = 'dag_run' AND
                            CONSTRAINT_NAME   = 'dag_run_dag_id_run_id_uq' AND
                            CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                            DROP INDEX dag_run_dag_id_run_id_uq','select 1');

                        prepare stmt from @var;
                        execute stmt;
                        deallocate prepare stmt;
                        """)
        )
        # below we drop and recreate the constraints because there's no IF NOT EXISTS
        conn.execute(
            sa.text("""
                                set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                                    CONSTRAINT_SCHEMA = DATABASE() AND
                                    TABLE_NAME        = 'dag_run' AND
                                    CONSTRAINT_NAME   = 'dag_run_dag_id_execution_date_key' AND
                                    CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                                    DROP INDEX dag_run_dag_id_execution_date_key','select 1');

                                prepare stmt from @var;
                                execute stmt;
                                deallocate prepare stmt;
                                """)
        )
        conn.execute(
            sa.text("""
                            set @var=if((SELECT true FROM information_schema.TABLE_CONSTRAINTS WHERE
                                CONSTRAINT_SCHEMA = DATABASE() AND
                                TABLE_NAME        = 'dag_run' AND
                                CONSTRAINT_NAME   = 'dag_run_dag_id_run_id_key' AND
                                CONSTRAINT_TYPE   = 'UNIQUE') = true,'ALTER TABLE dag_run
                                DROP INDEX dag_run_dag_id_run_id_key','select 1');

                            prepare stmt from @var;
                            execute stmt;
                            deallocate prepare stmt;
                            """)
        )
        with op.batch_alter_table("callback_request", schema=None) as batch_op:
            batch_op.alter_column(
                "processor_subdir",
                existing_type=sa.Text(length=2000),
                type_=sa.String(length=2000),
                existing_nullable=True,
            )

        with op.batch_alter_table("dag", schema=None) as batch_op:
            batch_op.alter_column(
                "processor_subdir",
                existing_type=sa.Text(length=2000),
                type_=sa.String(length=2000),
                existing_nullable=True,
            )

        with op.batch_alter_table("import_error", schema=None) as batch_op:
            batch_op.alter_column(
                "processor_subdir",
                existing_type=sa.Text(length=2000),
                type_=sa.String(length=2000),
                existing_nullable=True,
            )

        with op.batch_alter_table("serialized_dag", schema=None) as batch_op:
            batch_op.alter_column(
                "processor_subdir",
                existing_type=sa.Text(length=2000),
                type_=sa.String(length=2000),
                existing_nullable=True,
            )

    elif conn.dialect.name == "sqlite":
        # SQLite does not support DROP CONSTRAINT
        # We have to recreate the table without the constraint
        conn.execute(sa.text("PRAGMA foreign_keys=off"))
        conn.execute(
            sa.text("""
            CREATE TABLE dag_run_new (
                id INTEGER NOT NULL,
                dag_id VARCHAR(250) NOT NULL,
                queued_at TIMESTAMP,
                execution_date TIMESTAMP NOT NULL,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                state VARCHAR(50),
                run_id VARCHAR(250) NOT NULL,
                creating_job_id INTEGER,
                external_trigger BOOLEAN,
                run_type VARCHAR(50) NOT NULL,
                conf BLOB,
                data_interval_start TIMESTAMP,
                data_interval_end TIMESTAMP,
                last_scheduling_decision TIMESTAMP,
                dag_hash VARCHAR(32),
                log_template_id INTEGER,
                updated_at TIMESTAMP,
                clear_number INTEGER DEFAULT '0' NOT NULL,
                CONSTRAINT dag_run_pkey PRIMARY KEY (id),
                CONSTRAINT dag_run_dag_id_execution_date_key UNIQUE (dag_id, execution_date),
                CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, run_id),
                CONSTRAINT task_instance_log_template_id_fkey FOREIGN KEY(log_template_id) REFERENCES log_template (id) ON DELETE NO ACTION
            )
        """)
        )

        conn.execute(sa.text("INSERT INTO dag_run_new SELECT * FROM dag_run"))
        conn.execute(sa.text("DROP TABLE dag_run"))
        conn.execute(sa.text("ALTER TABLE dag_run_new RENAME TO dag_run"))
        conn.execute(sa.text("PRAGMA foreign_keys=on"))
        with op.batch_alter_table("dag_run") as batch_op:
            batch_op.create_index("dag_id_state", ["dag_id", "state"], if_not_exists=True)
            batch_op.create_index("idx_dag_run_dag_id", ["dag_id"], if_not_exists=True)
            batch_op.create_index(
                "idx_dag_run_running_dags",
                ["state", "dag_id"],
                sqlite_where=sa.text("state='running'"),
                if_not_exists=True,
            )
            batch_op.create_index(
                "idx_dag_run_queued_dags",
                ["state", "dag_id"],
                sqlite_where=sa.text("state='queued'"),
                if_not_exists=True,
            )

    else:
        op.execute(
            "ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_uq"
        )
        op.execute(
            "ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_uq"
        )
        # below we drop and recreate the constraints because there's no IF NOT EXISTS
        op.execute(
            "ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_key"
        )
        op.execute(
            "ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_key"
        )

    with op.batch_alter_table("dag_run") as batch_op:
        batch_op.create_unique_constraint(
            "dag_run_dag_id_execution_date_key", ["dag_id", "execution_date"]
        )
        batch_op.create_unique_constraint(
            "dag_run_dag_id_run_id_key", ["dag_id", "run_id"]
        )

    with op.batch_alter_table("task_instance") as batch_op:
        batch_op.create_foreign_key(
            "task_instance_dag_run_fkey",
            "dag_run",
            ["dag_id", "run_id"],
            ["dag_id", "run_id"],
            ondelete="CASCADE",
        )

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
