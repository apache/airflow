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
remove pickled data from dagrun table.

Revision ID: e39a26ac59f6
Revises: 38770795785f
Create Date: 2024-12-01 08:33:15.425141

"""

from __future__ import annotations

import json
import pickle
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e39a26ac59f6"
down_revision = "38770795785f"
branch_labels = None
depends_on = None
airflow_version = "3.0.0"


def upgrade():
    """Apply remove pickled data from dagrun table."""
    conn = op.get_bind()
    empty_vals = {
        "mysql": "X'80057D942E'",
        "postgresql": r"'\x80057D942E'",
        "sqlite": "X'80057D942E'",
    }
    dialect = conn.dialect.name
    try:
        empty_val = empty_vals[dialect]
    except KeyError:
        raise RuntimeError(f"Dialect {dialect} not supported.")

    conf_type = sa.JSON().with_variant(postgresql.JSONB, "postgresql")
    op.add_column("dag_run", sa.Column("conf_json", conf_type, nullable=True))

    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the 'conf' column while in offline mode!
            --  The 'conf' column will be set to NULL in offline mode.
            --  Avoid using offline mode if you need to retain 'conf' values.
            ------------
            """)
        )
    else:
        BATCH_SIZE = 1000
        offset = 0
        while True:
            err_count = 0
            batch_num = offset + 1
            print(f"converting dag run conf. batch={batch_num}")
            rows = conn.execute(
                text(
                    "SELECT id, conf "
                    "FROM dag_run "
                    "WHERE conf IS not NULL "
                    f"AND conf != {empty_val}"
                    f"ORDER BY id LIMIT {BATCH_SIZE} "
                    f"OFFSET {offset}"
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                row_id, pickle_data = row

                try:
                    original_data = pickle.loads(pickle_data)
                    json_data = json.dumps(original_data)
                    conn.execute(
                        text("""
                                                UPDATE dag_run
                                                SET conf_json = :json_data
                                                WHERE id = :id
                                            """),
                        {"json_data": json_data, "id": row_id},
                    )
                except Exception:
                    err_count += 1
                    continue
            if err_count:
                print(f"could not convert dag run conf for {err_count} records. batch={batch_num}")
            offset += BATCH_SIZE

    op.drop_column("dag_run", "conf")

    op.alter_column("dag_run", "conf_json", existing_type=conf_type, new_column_name="conf")


def downgrade():
    """Unapply Remove pickled data from dagrun table."""
    conn = op.get_bind()
    op.add_column("dag_run", sa.Column("conf_pickle", sa.PickleType(), nullable=True))

    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the 'conf' column while in offline mode!
            --  The 'conf' column will be set to NULL in offline mode.
            --  Avoid using offline mode if you need to retain 'conf' values.
            ------------
            """)
        )

    else:
        BATCH_SIZE = 1000
        offset = 0
        while True:
            rows = conn.execute(
                text(
                    "SELECT id,conf "
                    "FROM dag_run "
                    "WHERE conf IS NOT NULL "
                    f"ORDER BY id LIMIT {BATCH_SIZE} "
                    f"OFFSET {offset}"
                )
            ).fetchall()
            if not rows:
                break
            for row in rows:
                row_id, json_data = row

                try:
                    pickled_data = pickle.dumps(json_data, protocol=pickle.HIGHEST_PROTOCOL)
                    conn.execute(
                        text("""
                            UPDATE dag_run
                            SET conf_pickle = :pickle_data
                            WHERE id = :id
                        """),
                        {"pickle_data": pickled_data, "id": row_id},
                    )
                except Exception as e:
                    print(f"Error pickling dagrun conf for dagrun ID {row_id}: {e}")
                    continue
            offset += BATCH_SIZE

    op.drop_column("dag_run", "conf")

    op.alter_column("dag_run", "conf_pickle", existing_type=sa.PickleType(), new_column_name="conf")
