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
add dag versioning.

Revision ID: 2b47dc6bc8df
Revises: d03e4a635aa3
Create Date: 2024-10-09 05:44:04.670984

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy_utils import UUIDType
from uuid6 import uuid7

from airflow.migrations.db_types import TIMESTAMP, StringID
from airflow.models.base import naming_convention
from airflow.models.dagcode import DagCode
from airflow.utils import timezone

# revision identifiers, used by Alembic.
revision = "2b47dc6bc8df"
down_revision = "d03e4a635aa3"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def _get_rows(sql, conn):
    stmt = sa.text(sql)
    rows = conn.execute(stmt)
    if rows:
        rows = rows.fetchall()
    else:
        rows = []
    return rows


def _airflow_2_fileloc_hash(fileloc):
    import hashlib
    import struct

    # Only 7 bytes because MySQL BigInteger can hold only 8 bytes (signed).
    return struct.unpack(">Q", hashlib.sha1(fileloc.encode("utf-8")).digest()[-8:])[0] >> 8


def upgrade():
    """Apply add dag versioning."""
    conn = op.get_bind()
    op.create_table(
        "dag_version",
        sa.Column("id", UUIDType(binary=False), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("dag_id", StringID(), nullable=False),
        sa.Column("created_at", TIMESTAMP(), nullable=False, default=timezone.utcnow),
        sa.Column(
            "last_updated", TIMESTAMP(), nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
        ),
        sa.ForeignKeyConstraint(
            ("dag_id",), ["dag.dag_id"], name=op.f("dag_version_dag_id_fkey"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("dag_version_pkey")),
        sa.UniqueConstraint("dag_id", "version_number", name="dag_id_v_name_v_number_unique_constraint"),
    )
    with op.batch_alter_table(
        "dag_code",
    ) as batch_op:
        batch_op.drop_constraint("dag_code_pkey", type_="primary")
        batch_op.add_column(sa.Column("id", UUIDType(binary=False)))
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))
        batch_op.add_column(sa.Column("source_code_hash", sa.String(length=32)))
        batch_op.add_column(sa.Column("dag_id", StringID()))
        batch_op.add_column(sa.Column("created_at", TIMESTAMP(), default=timezone.utcnow))

    with op.batch_alter_table(
        "serialized_dag",
    ) as batch_op:
        batch_op.add_column(sa.Column("id", UUIDType(binary=False)))
        batch_op.drop_index("idx_fileloc_hash")
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))
        batch_op.add_column(sa.Column("created_at", TIMESTAMP(), default=timezone.utcnow))

    # Data migration
    rows = _get_rows("SELECT dag_id FROM serialized_dag", conn)

    stmt = sa.text("""
            UPDATE serialized_dag
            SET id = :_id
            WHERE dag_id = :dag_id AND id IS NULL
        """)

    for row in rows:
        id = uuid7()
        if conn.dialect.name != "postgresql":
            id = id.hex
        else:
            id = str(id)

        conn.execute(stmt.bindparams(_id=id, dag_id=row.dag_id))
        id2 = uuid7()
        if conn.dialect.name != "postgresql":
            id2 = id2.hex
        else:
            id2 = str(id2)
        # Update dagversion table
        conn.execute(
            sa.text("""
            INSERT INTO dag_version (id, version_number, dag_id, created_at, last_updated)
            VALUES (:id, 1, :dag_id, :created_at, :last_updated)
        """).bindparams(
                id=id2, dag_id=row.dag_id, created_at=timezone.utcnow(), last_updated=timezone.utcnow()
            )
        )

    # Update serialized_dag table with dag_version_id where dag_id matches
    if conn.dialect.name == "mysql":
        conn.execute(
            sa.text("""
            UPDATE serialized_dag sd
            JOIN dag_version dv ON sd.dag_id = dv.dag_id
            SET sd.dag_version_id = dv.id,
                sd.created_at = dv.created_at
        """)
        )
    else:
        conn.execute(
            sa.text("""
            UPDATE serialized_dag
            SET dag_version_id = dag_version.id,
                created_at = dag_version.created_at
            FROM dag_version
            WHERE serialized_dag.dag_id = dag_version.dag_id
        """)
        )
    # Update dag_code table where fileloc_hash of serialized_dag matches
    if conn.dialect.name == "mysql":
        conn.execute(
            sa.text("""
            UPDATE dag_code dc
            JOIN serialized_dag sd ON dc.fileloc_hash = sd.fileloc_hash
            SET dc.dag_version_id = sd.dag_version_id,
                dc.created_at = sd.created_at,
                dc.dag_id = sd.dag_id
        """)
        )
    else:
        conn.execute(
            sa.text("""
            UPDATE dag_code
            SET dag_version_id = dag_version.id,
                created_at = serialized_dag.created_at,
                dag_id = serialized_dag.dag_id
            FROM serialized_dag, dag_version
            WHERE dag_code.fileloc_hash = serialized_dag.fileloc_hash
            AND serialized_dag.dag_version_id = dag_version.id
        """)
        )

    # select all rows in serialized_dag where the dag_id is not in dag_code

    stmt = """
        SELECT dag_id, fileloc, fileloc_hash, dag_version_id
        FROM serialized_dag
        WHERE dag_id NOT IN (SELECT dag_id FROM dag_code)
        AND dag_id in (SELECT dag_id FROM dag)
    """
    rows = _get_rows(stmt, conn)
    # Insert the missing rows from serialized_dag to dag_code
    stmt = sa.text("""
        INSERT INTO dag_code (dag_version_id, dag_id, fileloc, fileloc_hash, source_code, last_updated, created_at)
        VALUES (:dag_version_id, :dag_id, :fileloc, :fileloc_hash, :source_code, :last_updated, :created_at)
    """)
    for row in rows:
        try:
            source_code = DagCode.get_code_from_file(row.fileloc)
        except FileNotFoundError:
            source_code = "source_code"
        conn.execute(
            stmt.bindparams(
                dag_version_id=row.dag_version_id,
                dag_id=row.dag_id,
                fileloc=row.fileloc,
                fileloc_hash=row.fileloc_hash,
                source_code=source_code,
                last_updated=timezone.utcnow(),
                created_at=timezone.utcnow(),
            )
        )

    stmt = "SELECT dag_id, fileloc FROM dag_code"
    rows = _get_rows(stmt, conn)
    stmt = sa.text("""
                    UPDATE dag_code
                    SET id = :_id,
                        dag_id = :dag_id,
                        source_code = :source_code,
                        source_code_hash = :source_code_hash
                    WHERE dag_id = :dag_id AND id IS NULL
                """)
    for row in rows:
        id = uuid7()
        if conn.dialect.name != "postgresql":
            id = id.hex
        else:
            id = str(id)
        try:
            source_code = DagCode.get_code_from_file(row.fileloc)
        except FileNotFoundError:
            source_code = "source_code"
        conn.execute(
            stmt.bindparams(
                _id=id,
                source_code_hash=DagCode.dag_source_hash(source_code),
                source_code=source_code,
                dag_id=row.dag_id,
            )
        )

    with op.batch_alter_table("dag_code") as batch_op:
        batch_op.alter_column("dag_id", existing_type=StringID(), nullable=False)
        batch_op.alter_column("id", existing_type=UUIDType(binary=False), nullable=False)
        batch_op.create_primary_key("dag_code_pkey", ["id"])
        batch_op.alter_column("dag_version_id", existing_type=UUIDType(binary=False), nullable=False)
        batch_op.create_foreign_key(
            batch_op.f("dag_code_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint("dag_code_dag_version_id_uq", ["dag_version_id"])
        batch_op.drop_column("fileloc_hash")
        batch_op.alter_column("source_code_hash", existing_type=sa.String(length=32), nullable=False)
        batch_op.alter_column("created_at", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("serialized_dag") as batch_op:
        batch_op.drop_constraint("serialized_dag_pkey", type_="primary")
        batch_op.alter_column("id", existing_type=UUIDType(binary=False), nullable=False)
        batch_op.alter_column("dag_version_id", existing_type=UUIDType(binary=False), nullable=False)
        batch_op.drop_column("fileloc_hash")
        batch_op.drop_column("fileloc")
        batch_op.create_primary_key("serialized_dag_pkey", ["id"])
        batch_op.create_foreign_key(
            batch_op.f("serialized_dag_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_unique_constraint("serialized_dag_dag_version_id_uq", ["dag_version_id"])
        batch_op.alter_column("created_at", existing_type=TIMESTAMP(), nullable=False)

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))
        batch_op.create_foreign_key(
            batch_op.f("task_instance_dag_version_id_fkey"),
            "dag_version",
            ["dag_version_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_version_id", UUIDType(binary=False)))

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.drop_column("dag_hash")
        batch_op.add_column(sa.Column("created_dag_version_id", UUIDType(binary=False), nullable=True))
        batch_op.create_foreign_key(
            "created_dag_version_id_fkey",
            "dag_version",
            ["created_dag_version_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    """Unapply add dag versioning."""
    conn = op.get_bind()

    with op.batch_alter_table("task_instance_history", schema=None) as batch_op:
        batch_op.drop_column("dag_version_id")

    with op.batch_alter_table("task_instance", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("task_instance_dag_version_id_fkey"), type_="foreignkey")
        batch_op.drop_column("dag_version_id")

    with op.batch_alter_table("dag_code", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_code_dag_version_id_fkey"), type_="foreignkey")
        batch_op.add_column(sa.Column("fileloc_hash", sa.BigInteger, nullable=True))
        batch_op.drop_column("source_code_hash")
        batch_op.drop_column("created_at")

    # Update the added fileloc_hash with the hash of fileloc
    stmt = "SELECT fileloc FROM dag_code"
    rows = _get_rows(stmt, conn)
    stmt = sa.text("""
                    UPDATE dag_code
                    SET fileloc_hash = :_hash
                    where fileloc = :fileloc and fileloc_hash is null
                """)
    for row in rows:
        hash = _airflow_2_fileloc_hash(row.fileloc)
        conn.execute(stmt.bindparams(_hash=hash, fileloc=row.fileloc))

    with op.batch_alter_table("serialized_dag", schema=None, naming_convention=naming_convention) as batch_op:
        batch_op.drop_column("id")
        batch_op.add_column(sa.Column("fileloc", sa.String(length=2000), nullable=True))
        batch_op.add_column(sa.Column("fileloc_hash", sa.BIGINT(), nullable=True))

        batch_op.drop_constraint(batch_op.f("serialized_dag_dag_version_id_fkey"), type_="foreignkey")
        batch_op.drop_column("created_at")

    # Update the serialized fileloc with fileloc from dag_code where dag_version_id matches
    if conn.dialect.name == "mysql":
        conn.execute(
            sa.text("""
            UPDATE serialized_dag sd
            JOIN dag_code dc ON sd.dag_version_id = dc.dag_version_id
            SET sd.fileloc = dc.fileloc,
                sd.fileloc_hash = dc.fileloc_hash
        """)
        )
    else:
        conn.execute(
            sa.text("""
                    UPDATE serialized_dag
                    SET fileloc = dag_code.fileloc,
                        fileloc_hash = dag_code.fileloc_hash
                    FROM dag_code
                    WHERE serialized_dag.dag_version_id = dag_code.dag_version_id
                """)
        )
    # Deduplicate the rows in dag_code with the same fileloc_hash so we can make fileloc_hash the primary key
    stmt = sa.text("""
                WITH ranked_rows AS (
            SELECT
                fileloc_hash,
                ROW_NUMBER() OVER (PARTITION BY fileloc_hash ORDER BY id) as row_num
            FROM dag_code
        )
        DELETE FROM dag_code
        WHERE EXISTS (
            SELECT 1
            FROM ranked_rows
            WHERE ranked_rows.fileloc_hash = dag_code.fileloc_hash
            AND ranked_rows.row_num > 1
        );
        """)
    conn.execute(stmt)
    with op.batch_alter_table("serialized_dag") as batch_op:
        batch_op.drop_column("dag_version_id")
        batch_op.create_index("idx_fileloc_hash", ["fileloc_hash"], unique=False)
        batch_op.create_primary_key("serialized_dag_pkey", ["dag_id"])
        batch_op.alter_column("fileloc", existing_type=sa.String(length=2000), nullable=False)
        batch_op.alter_column("fileloc_hash", existing_type=sa.BIGINT(), nullable=False)

    with op.batch_alter_table("dag_code") as batch_op:
        batch_op.drop_column("id")
        batch_op.create_primary_key("dag_code_pkey", ["fileloc_hash"])
        batch_op.drop_column("dag_version_id")
        batch_op.drop_column("dag_id")

    with op.batch_alter_table("dag_run", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dag_hash", sa.String(length=32), autoincrement=False, nullable=True))
        batch_op.drop_constraint("created_dag_version_id_fkey", type_="foreignkey")
        batch_op.drop_column("created_dag_version_id")

    # Update dag_run dag_hash with dag_hash from serialized_dag where dag_id matches
    if conn.dialect.name == "mysql":
        conn.execute(
            sa.text("""
            UPDATE dag_run dr
            JOIN serialized_dag sd ON dr.dag_id = sd.dag_id
            SET dr.dag_hash = sd.dag_hash
        """)
        )
    else:
        conn.execute(
            sa.text("""
            UPDATE dag_run
            SET dag_hash = serialized_dag.dag_hash
            FROM serialized_dag
            WHERE dag_run.dag_id = serialized_dag.dag_id
        """)
        )

    op.drop_table("dag_version")
