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
Create initial database state from Airflow v2.6.2.

Revision ID: 4bc4d934e2bc
Revises: None
Create Date: 2025-12-08 14:46:22.497513

"""

from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op

from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime

# revision identifiers, used by Alembic.
revision = "4bc4d934e2bc"
down_revision = None
branch_labels = None
depends_on = None
airflow_version = "2.6.2"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "ab_permission",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint("id", name="ab_permission_pkey"),
        sa.UniqueConstraint("name", name="ab_permission_name_uq"),
    )
    op.create_table(
        "ab_register_user",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("first_name", sa.String(length=256), nullable=False),
        sa.Column("last_name", sa.String(length=256), nullable=False),
        sa.Column("username", sa.String(length=512), nullable=False),
        sa.Column("email", sa.String(length=512), nullable=False),
        sa.Column("password", sa.String(length=256), nullable=True),
        sa.Column("registration_date", sa.DateTime(), nullable=True),
        sa.Column("registration_hash", sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint("id", name="ab_register_user_pkey"),
        sa.UniqueConstraint("username", name="ab_register_user_username_uq"),
    )
    op.create_table(
        "ab_role",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.PrimaryKeyConstraint("id", name="ab_role_pkey"),
        sa.UniqueConstraint("name", name="ab_role_name_uq"),
    )
    op.create_table(
        "ab_user",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("first_name", sa.String(length=256), nullable=False),
        sa.Column("last_name", sa.String(length=256), nullable=False),
        sa.Column("username", sa.String(length=512), nullable=False),
        sa.Column("email", sa.String(length=512), nullable=False),
        sa.Column("password", sa.String(length=256), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("last_login", sa.DateTime(), nullable=True),
        sa.Column("login_count", sa.Integer(), nullable=True),
        sa.Column("fail_login_count", sa.Integer(), nullable=True),
        sa.Column("created_on", sa.DateTime(), nullable=True),
        sa.Column("changed_on", sa.DateTime(), nullable=True),
        sa.Column("created_by_fk", sa.Integer(), nullable=True),
        sa.Column("changed_by_fk", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["changed_by_fk"], ["ab_user.id"], name="ab_user_changed_by_fk_fkey"),
        sa.ForeignKeyConstraint(["created_by_fk"], ["ab_user.id"], name="ab_user_created_by_fk_fkey"),
        sa.PrimaryKeyConstraint("id", name="ab_user_pkey"),
        sa.UniqueConstraint("email", name="ab_user_email_uq"),
        sa.UniqueConstraint("username", name="ab_user_username_uq"),
    )
    op.create_table(
        "ab_view_menu",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=250), nullable=False),
        sa.PrimaryKeyConstraint("id", name="ab_view_menu_pkey"),
        sa.UniqueConstraint("name", name="ab_view_menu_name_uq"),
    )
    op.create_table(
        "callback_request",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("priority_weight", sa.Integer(), nullable=False),
        sa.Column("callback_data", sa.JSON(), nullable=False),
        sa.Column("callback_type", sa.String(length=20), nullable=False),
        sa.Column("processor_subdir", sa.String(length=2000), nullable=True),
        sa.PrimaryKeyConstraint("id", name="callback_request_pkey"),
    )
    op.create_table(
        "connection",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("conn_id", sa.String(length=250), nullable=False),
        sa.Column("conn_type", sa.String(length=500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("host", sa.String(length=500), nullable=True),
        sa.Column("schema", sa.String(length=500), nullable=True),
        sa.Column("login", sa.String(length=500), nullable=True),
        sa.Column("password", sa.String(length=5000), nullable=True),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=True),
        sa.Column("is_extra_encrypted", sa.Boolean(), nullable=True),
        sa.Column("extra", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="connection_pkey"),
        sa.UniqueConstraint("conn_id", name="connection_conn_id_uq"),
    )
    op.create_table(
        "dag",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("max_active_tasks", sa.Integer(), nullable=False),
        sa.Column("has_task_concurrency_limits", sa.Boolean(), nullable=False),
        sa.Column("root_dag_id", sa.String(length=250), nullable=True),
        sa.Column("is_paused", sa.Boolean(), nullable=True),
        sa.Column("is_subdag", sa.Boolean(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column("last_parsed_time", UtcDateTime(timezone=True), nullable=True),
        sa.Column("last_pickled", UtcDateTime(timezone=True), nullable=True),
        sa.Column("last_expired", UtcDateTime(timezone=True), nullable=True),
        sa.Column("scheduler_lock", sa.Boolean(), nullable=True),
        sa.Column("pickle_id", sa.Integer(), nullable=True),
        sa.Column("fileloc", sa.String(length=2000), nullable=True),
        sa.Column("processor_subdir", sa.String(length=2000), nullable=True),
        sa.Column("owners", sa.String(length=2000), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("default_view", sa.String(length=25), nullable=True),
        sa.Column("schedule_interval", sa.Text(), nullable=True),
        sa.Column("timetable_description", sa.String(length=1000), nullable=True),
        sa.Column("max_active_runs", sa.Integer(), nullable=True),
        sa.Column(
            "has_import_errors",
            sa.Boolean(),
            server_default=sa.text("(false)"),
            nullable=True,
        ),
        sa.Column("next_dagrun", UtcDateTime(timezone=True), nullable=True),
        sa.Column(
            "next_dagrun_data_interval_start",
            UtcDateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("next_dagrun_data_interval_end", UtcDateTime(timezone=True), nullable=True),
        sa.Column("next_dagrun_create_after", UtcDateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("dag_id", name="dag_pkey"),
    )
    op.create_index(
        "idx_next_dagrun_create_after",
        "dag",
        ["next_dagrun_create_after"],
        unique=False,
    )
    op.create_index("idx_root_dag_id", "dag", ["root_dag_id"], unique=False)
    op.create_table(
        "dag_code",
        sa.Column("fileloc_hash", sa.BigInteger(), nullable=False),
        sa.Column("fileloc", sa.String(length=2000), nullable=False),
        sa.Column("last_updated", UtcDateTime(timezone=True), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("fileloc_hash", name="dag_code_pkey"),
    )
    op.create_table(
        "dag_pickle",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("pickle", sa.LargeBinary(), nullable=True),
        sa.Column("created_dttm", UtcDateTime(timezone=True), nullable=True),
        sa.Column("pickle_hash", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="dag_pickle_pkey"),
    )
    op.create_table(
        "dataset",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("uri", sa.String(length=3000), nullable=False),
        sa.Column("extra", sa.JSON(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column(
            "is_orphaned",
            sa.Boolean(),
            server_default=sa.text("(false)"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="dataset_pkey"),
    )
    op.create_index("idx_uri_unique", "dataset", ["uri"], unique=True)
    op.create_table(
        "dataset_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("extra", sa.JSON(), nullable=False),
        sa.Column("timestamp", UtcDateTime(timezone=True), nullable=False),
        sa.Column("source_task_id", sa.String(length=250), nullable=True),
        sa.Column("source_dag_id", sa.String(length=250), nullable=True),
        sa.Column("source_run_id", sa.String(length=250), nullable=True),
        sa.Column("source_map_index", sa.Integer(), server_default="-1", nullable=True),
        sa.PrimaryKeyConstraint("id", name="dataset_event_pkey"),
    )
    op.create_index(
        "idx_dataset_id_timestamp",
        "dataset_event",
        ["dataset_id", "timestamp"],
        unique=False,
    )
    op.create_table(
        "import_error",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", UtcDateTime(timezone=True), nullable=True),
        sa.Column("filename", sa.String(length=1024), nullable=True),
        sa.Column("stacktrace", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="import_error_pkey"),
    )
    op.create_table(
        "job",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=True),
        sa.Column("state", sa.String(length=20), nullable=True),
        sa.Column("job_type", sa.String(length=30), nullable=True),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("latest_heartbeat", UtcDateTime(timezone=True), nullable=True),
        sa.Column("executor_class", sa.String(length=500), nullable=True),
        sa.Column("hostname", sa.String(length=500), nullable=True),
        sa.Column("unixname", sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint("id", name="job_pkey"),
    )
    op.create_index("idx_job_dag_id", "job", ["dag_id"], unique=False)
    op.create_index("idx_job_state_heartbeat", "job", ["state", "latest_heartbeat"], unique=False)
    op.create_index("job_type_heart", "job", ["job_type", "latest_heartbeat"], unique=False)
    op.create_table(
        "log",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dttm", UtcDateTime(timezone=True), nullable=True),
        sa.Column("dag_id", sa.String(length=250), nullable=True),
        sa.Column("task_id", sa.String(length=250), nullable=True),
        sa.Column("map_index", sa.Integer(), nullable=True),
        sa.Column("event", sa.String(length=30), nullable=True),
        sa.Column("execution_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("owner", sa.String(length=500), nullable=True),
        sa.Column("extra", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="log_pkey"),
    )
    op.create_index("idx_log_dag", "log", ["dag_id"], unique=False)
    op.create_index("idx_log_dttm", "log", ["dttm"], unique=False)
    op.create_index("idx_log_event", "log", ["event"], unique=False)
    op.create_table(
        "log_template",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("elasticsearch_id", sa.Text(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="log_template_pkey"),
    )
    op.create_table(
        "serialized_dag",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("fileloc", sa.String(length=2000), nullable=False),
        sa.Column("fileloc_hash", sa.BigInteger(), nullable=False),
        sa.Column("last_updated", UtcDateTime(timezone=True), nullable=False),
        sa.Column("dag_hash", sa.String(length=32), nullable=False),
        sa.Column("data", sa.JSON(), nullable=True),
        sa.Column("data_compressed", sa.LargeBinary(), nullable=True),
        sa.Column("processor_subdir", sa.String(length=2000), nullable=True),
        sa.PrimaryKeyConstraint("dag_id", name="serialized_dag_pkey"),
    )
    op.create_index("idx_fileloc_hash", "serialized_dag", ["fileloc_hash"], unique=False)
    op.create_table(
        "slot_pool",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("pool", sa.String(length=256), nullable=True),
        sa.Column("slots", sa.Integer(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="slot_pool_pkey"),
        sa.UniqueConstraint("pool", name="slot_pool_pool_uq"),
    )
    op.create_table(
        "trigger",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("classpath", sa.String(length=1000), nullable=False),
        sa.Column("kwargs", sa.JSON(), nullable=False),
        sa.Column("created_date", UtcDateTime(timezone=True), nullable=False),
        sa.Column("triggerer_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="trigger_pkey"),
    )
    op.create_table(
        "variable",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("key", sa.String(length=250), nullable=True),
        sa.Column("val", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="variable_pkey"),
        sa.UniqueConstraint("key", name="variable_key_uq"),
    )
    op.create_table(
        "ab_permission_view",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("permission_id", sa.Integer(), nullable=True),
        sa.Column("view_menu_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["permission_id"],
            ["ab_permission.id"],
            name="ab_permission_view_permission_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["view_menu_id"],
            ["ab_view_menu.id"],
            name="ab_permission_view_view_menu_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="ab_permission_view_pkey"),
        sa.UniqueConstraint(
            "permission_id",
            "view_menu_id",
            name="ab_permission_view_permission_id_view_menu_id_uq",
        ),
    )
    op.create_table(
        "ab_user_role",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("role_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["role_id"], ["ab_role.id"], name="ab_user_role_role_id_fkey"),
        sa.ForeignKeyConstraint(["user_id"], ["ab_user.id"], name="ab_user_role_user_id_fkey"),
        sa.PrimaryKeyConstraint("id", name="ab_user_role_pkey"),
        sa.UniqueConstraint("user_id", "role_id", name="ab_user_role_user_id_role_id_uq"),
    )
    op.create_table(
        "dag_owner_attributes",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("owner", sa.String(length=500), nullable=False),
        sa.Column("link", sa.String(length=500), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="dag.dag_id", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dag_id", "owner", name="dag_owner_attributes_pkey"),
    )
    op.create_table(
        "dag_run",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("execution_date", UtcDateTime(timezone=True), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("run_type", sa.String(length=50), nullable=False),
        sa.Column("queued_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("state", sa.String(length=50), nullable=True),
        sa.Column("creating_job_id", sa.Integer(), nullable=True),
        sa.Column("external_trigger", sa.Boolean(), nullable=True),
        sa.Column("conf", sa.LargeBinary(), nullable=True),
        sa.Column("data_interval_start", UtcDateTime(timezone=True), nullable=True),
        sa.Column("data_interval_end", UtcDateTime(timezone=True), nullable=True),
        sa.Column("last_scheduling_decision", UtcDateTime(timezone=True), nullable=True),
        sa.Column("dag_hash", sa.String(length=32), nullable=True),
        sa.Column("log_template_id", sa.Integer(), nullable=True),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["log_template_id"],
            ["log_template.id"],
            name="task_instance_log_template_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="dag_run_pkey"),
        sa.UniqueConstraint("dag_id", "execution_date", name="dag_run_dag_id_execution_date_key"),
        sa.UniqueConstraint("dag_id", "run_id", name="dag_run_dag_id_run_id_key"),
    )
    op.create_index("dag_id_state", "dag_run", ["dag_id", "state"], unique=False)
    op.create_index("idx_dag_run_dag_id", "dag_run", ["dag_id"], unique=False)
    op.create_index("idx_dag_run_queued_dags", "dag_run", ["state", "dag_id"], unique=False)
    op.create_index("idx_dag_run_running_dags", "dag_run", ["state", "dag_id"], unique=False)
    op.create_index(
        "idx_last_scheduling_decision",
        "dag_run",
        ["last_scheduling_decision"],
        unique=False,
    )
    op.create_table(
        "dag_schedule_dataset_reference",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="dsdr_dag_id_fkey", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"], name="dsdr_dataset_fkey", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dataset_id", "dag_id", name="dsdr_pkey"),
    )
    op.create_table(
        "dag_tag",
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="dag_tag_dag_id_fkey", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("name", "dag_id", name="dag_tag_pkey"),
    )
    op.create_table(
        "dag_warning",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("warning_type", sa.String(length=50), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("timestamp", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="dcw_dag_id_fkey", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dag_id", "warning_type", name="dag_warning_pkey"),
    )
    op.create_table(
        "dataset_dag_run_queue",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("target_dag_id", sa.String(length=250), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"], name="ddrq_dataset_fkey", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_dag_id"], ["dag.dag_id"], name="ddrq_dag_fkey", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dataset_id", "target_dag_id", name="datasetdagrunqueue_pkey"),
    )
    op.create_table(
        "task_outlet_dataset_reference",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dag_id"], ["dag.dag_id"], name="todr_dag_id_fkey", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"], name="todr_dataset_fkey", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dataset_id", "dag_id", "task_id", name="todr_pkey"),
    )
    op.create_table(
        "ab_permission_view_role",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("permission_view_id", sa.Integer(), nullable=True),
        sa.Column("role_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["permission_view_id"],
            ["ab_permission_view.id"],
            name="ab_permission_view_role_permission_view_id_fkey",
        ),
        sa.ForeignKeyConstraint(["role_id"], ["ab_role.id"], name="ab_permission_view_role_role_id_fkey"),
        sa.PrimaryKeyConstraint("id", name="ab_permission_view_role_pkey"),
        sa.UniqueConstraint(
            "permission_view_id",
            "role_id",
            name="ab_permission_view_role_permission_view_id_role_id_uq",
        ),
    )
    op.create_table(
        "dag_run_note",
        sa.Column("dag_run_id", sa.Integer(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("content", sa.String(length=1000), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_run_id"],
            ["dag_run.id"],
            name="dag_run_note_dr_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["ab_user.id"], name="dag_run_note_user_fkey"),
        sa.PrimaryKeyConstraint("dag_run_id", name="dag_run_note_pkey"),
    )
    op.create_table(
        "dagrun_dataset_event",
        sa.Column("dag_run_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dag_run_id"],
            ["dag_run.id"],
            name="dagrun_dataset_event_dag_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["dataset_event.id"],
            name="dagrun_dataset_event_event_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dag_run_id", "event_id", name="dagrun_dataset_event_pkey"),
    )
    op.create_index(
        "idx_dagrun_dataset_events_dag_run_id",
        "dagrun_dataset_event",
        ["dag_run_id"],
        unique=False,
    )
    op.create_index(
        "idx_dagrun_dataset_events_event_id",
        "dagrun_dataset_event",
        ["event_id"],
        unique=False,
    )
    op.create_table(
        "task_instance",
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("pool", sa.String(length=256), nullable=False),
        sa.Column("pool_slots", sa.Integer(), nullable=False),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("duration", sa.Float(), nullable=True),
        sa.Column("state", sa.String(length=20), nullable=True),
        sa.Column("try_number", sa.Integer(), nullable=True),
        sa.Column("max_tries", sa.Integer(), server_default="-1", nullable=True),
        sa.Column("hostname", sa.String(length=1000), nullable=True),
        sa.Column("unixname", sa.String(length=1000), nullable=True),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("queue", sa.String(length=256), nullable=True),
        sa.Column("priority_weight", sa.Integer(), nullable=True),
        sa.Column("operator", sa.String(length=1000), nullable=True),
        sa.Column("queued_dttm", UtcDateTime(timezone=True), nullable=True),
        sa.Column("queued_by_job_id", sa.Integer(), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("executor_config", sa.LargeBinary(), nullable=True),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=True),
        sa.Column("external_executor_id", sa.String(length=250), nullable=True),
        sa.Column("trigger_id", sa.Integer(), nullable=True),
        sa.Column("trigger_timeout", UtcDateTime(timezone=True), nullable=True),
        sa.Column("next_method", sa.String(length=1000), nullable=True),
        sa.Column("next_kwargs", ExtendedJSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "run_id"],
            ["dag_run.dag_id", "dag_run.run_id"],
            name="task_instance_dag_run_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["trigger_id"],
            ["trigger.id"],
            name="task_instance_trigger_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index", name="task_instance_pkey"),
    )
    op.create_index("ti_dag_run", "task_instance", ["dag_id", "run_id"], unique=False)
    op.create_index("ti_dag_state", "task_instance", ["dag_id", "state"], unique=False)
    op.create_index("ti_job_id", "task_instance", ["job_id"], unique=False)
    op.create_index("ti_pool", "task_instance", ["pool", "state", "priority_weight"], unique=False)
    op.create_index("ti_state", "task_instance", ["state"], unique=False)
    op.create_index(
        "ti_state_lkp",
        "task_instance",
        ["dag_id", "task_id", "run_id", "state"],
        unique=False,
    )
    op.create_index("ti_trigger_id", "task_instance", ["trigger_id"], unique=False)
    op.create_table(
        "rendered_task_instance_fields",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("rendered_fields", sa.JSON(), nullable=False),
        sa.Column("k8s_pod_yaml", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="rtif_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            name="rendered_task_instance_fields_pkey",
        ),
    )
    op.create_table(
        "task_fail",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_fail_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="task_fail_pkey"),
    )
    op.create_index(
        "idx_task_fail_task_instance",
        "task_fail",
        ["dag_id", "task_id", "run_id", "map_index"],
        unique=False,
    )
    op.create_table(
        "task_instance_note",
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False),
        sa.Column("created_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", UtcDateTime(timezone=True), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("content", sa.String(length=1000), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_instance_note_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["ab_user.id"], name="task_instance_note_user_fkey"),
        sa.PrimaryKeyConstraint("task_id", "dag_id", "run_id", "map_index", name="task_instance_note_pkey"),
    )
    op.create_table(
        "task_map",
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), nullable=False),
        sa.Column("length", sa.Integer(), nullable=False),
        sa.Column("keys", ExtendedJSON(), nullable=True),
        sa.CheckConstraint("length >= 0", name="task_map_length_not_negative"),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_map_task_instance_fkey",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index", name="task_map_pkey"),
    )
    op.create_table(
        "task_reschedule",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("try_number", sa.Integer(), nullable=False),
        sa.Column("start_date", UtcDateTime(timezone=True), nullable=False),
        sa.Column("end_date", UtcDateTime(timezone=True), nullable=False),
        sa.Column("duration", sa.Integer(), nullable=False),
        sa.Column("reschedule_date", UtcDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["dag_id", "run_id"],
            ["dag_run.dag_id", "dag_run.run_id"],
            name="task_reschedule_dr_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_reschedule_ti_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="task_reschedule_pkey"),
    )
    op.create_index(
        "idx_task_reschedule_dag_run",
        "task_reschedule",
        ["dag_id", "run_id"],
        unique=False,
    )
    op.create_index(
        "idx_task_reschedule_dag_task_run",
        "task_reschedule",
        ["dag_id", "task_id", "run_id", "map_index"],
        unique=False,
    )
    op.create_table(
        "xcom",
        sa.Column("dag_run_id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(length=250), nullable=False),
        sa.Column("map_index", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("key", sa.String(length=512), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("timestamp", UtcDateTime(timezone=True), nullable=False),
        sa.Column("value", sa.LargeBinary(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dag_id", "task_id", "run_id", "map_index"],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="xcom_task_instance_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dag_run_id", "task_id", "map_index", "key", name="xcom_pkey"),
    )
    op.create_index("idx_xcom_key", "xcom", ["key"], unique=False)
    op.create_index(
        "idx_xcom_task_instance",
        "xcom",
        ["dag_id", "task_id", "run_id", "map_index"],
        unique=False,
    )
    op.bulk_insert(
        sa.table(
            "log_template",
            sa.column("filename", sa.Text()),
            sa.column("elasticsearch_id", sa.Text()),
            sa.column("created_at", UtcDateTime(timezone=True)),
        ),
        [
            {
                "filename": "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
                "elasticsearch_id": "{dag_id}-{task_id}-{execution_date}-{try_number}",
                "created_at": datetime.now(tz=timezone.utc),
            },
            {
                "filename": "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log",
                "elasticsearch_id": "{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
                "created_at": datetime.now(tz=timezone.utc),
            },
        ],
    )
    op.bulk_insert(
        sa.table(
            "slot_pool",
            sa.column("id", sa.Integer()),
            sa.column("pool", sa.String()),
            sa.column("slots", sa.Integer()),
            sa.column("description", sa.Text()),
        ),
        [
            {
                "id": 1,
                "pool": "default_pool",
                "slots": 128,
                "description": "Default pool",
            },
        ],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_xcom_task_instance", table_name="xcom")
    op.drop_index("idx_xcom_key", table_name="xcom")
    op.drop_table("xcom")
    op.drop_index("idx_task_reschedule_dag_task_run", table_name="task_reschedule")
    op.drop_index("idx_task_reschedule_dag_run", table_name="task_reschedule")
    op.drop_table("task_reschedule")
    op.drop_table("task_map")
    op.drop_table("task_instance_note")
    op.drop_index("idx_task_fail_task_instance", table_name="task_fail")
    op.drop_table("task_fail")
    op.drop_table("rendered_task_instance_fields")
    op.drop_index("ti_trigger_id", table_name="task_instance")
    op.drop_index("ti_state_lkp", table_name="task_instance")
    op.drop_index("ti_state", table_name="task_instance")
    op.drop_index("ti_pool", table_name="task_instance")
    op.drop_index("ti_job_id", table_name="task_instance")
    op.drop_index("ti_dag_state", table_name="task_instance")
    op.drop_index("ti_dag_run", table_name="task_instance")
    op.drop_table("task_instance")
    op.drop_index("idx_dagrun_dataset_events_event_id", table_name="dagrun_dataset_event")
    op.drop_index("idx_dagrun_dataset_events_dag_run_id", table_name="dagrun_dataset_event")
    op.drop_table("dagrun_dataset_event")
    op.drop_table("dag_run_note")
    op.drop_table("ab_permission_view_role")
    op.drop_table("task_outlet_dataset_reference")
    op.drop_table("dataset_dag_run_queue")
    op.drop_table("dag_warning")
    op.drop_table("dag_tag")
    op.drop_table("dag_schedule_dataset_reference")
    op.drop_index("idx_last_scheduling_decision", table_name="dag_run")
    op.drop_index("idx_dag_run_running_dags", table_name="dag_run")
    op.drop_index("idx_dag_run_queued_dags", table_name="dag_run")
    op.drop_index("idx_dag_run_dag_id", table_name="dag_run")
    op.drop_index("dag_id_state", table_name="dag_run")
    op.drop_table("dag_run")
    op.drop_table("dag_owner_attributes")
    op.drop_table("ab_user_role")
    op.drop_table("ab_permission_view")
    op.drop_table("variable")
    op.drop_table("trigger")
    op.drop_table("slot_pool")
    op.drop_index("sm_dag", table_name="sla_miss")
    op.drop_index("idx_fileloc_hash", table_name="serialized_dag")
    op.drop_table("serialized_dag")
    op.drop_table("log_template")
    op.drop_index("idx_log_event", table_name="log")
    op.drop_index("idx_log_dttm", table_name="log")
    op.drop_index("idx_log_dag", table_name="log")
    op.drop_table("log")
    op.drop_index("job_type_heart", table_name="job")
    op.drop_index("idx_job_state_heartbeat", table_name="job")
    op.drop_index("idx_job_dag_id", table_name="job")
    op.drop_table("job")
    op.drop_table("import_error")
    op.drop_index("idx_dataset_id_timestamp", table_name="dataset_event")
    op.drop_table("dataset_event")
    op.drop_index("idx_uri_unique", table_name="dataset")
    op.drop_table("dataset")
    op.drop_table("dag_pickle")
    op.drop_table("dag_code")
    op.drop_index("idx_root_dag_id", table_name="dag")
    op.drop_index("idx_next_dagrun_create_after", table_name="dag")
    op.drop_table("dag")
    op.drop_table("connection")
    op.drop_table("callback_request")
    op.drop_table("ab_view_menu")
    op.drop_index("idx_ab_user_username", table_name="ab_user")
    op.drop_table("ab_user")
    op.drop_table("ab_role")
    op.drop_index("idx_ab_register_user_username", table_name="ab_register_user")
    op.drop_table("ab_register_user")
    op.drop_table("ab_permission")
