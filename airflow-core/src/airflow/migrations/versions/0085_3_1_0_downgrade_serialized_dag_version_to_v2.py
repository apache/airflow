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
Add backward compatibility for serialized DAG format v3 to v2.

Revision ID: cc92b33c6709
Revises: eaf332f43c7c
Create Date: 2025-09-22 22:50:48.035121

"""

from __future__ import annotations

from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

# revision identifiers, used by Alembic.
revision = "cc92b33c6709"
down_revision = "eaf332f43c7c"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply Downgrade Serialized Dag version to v2."""
    # No-op: Server handles v1/v2/v3 DAGs at runtime via conversion functions
    pass


def downgrade():
    """Convert v3 serialized DAGs back to v2 format for compatibility with older Airflow versions."""
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  Manual v3 to v2 DAG conversion required (offline mode)
            --
            --  PostgreSQL:
            --  UPDATE serialized_dag SET data = jsonb_set((data::jsonb - 'client_defaults'), '{__version}', '2')::json
            --  WHERE id IN (SELECT id FROM serialized_dag WHERE data->>'__version' = '3' AND data_compressed IS NULL);
            --
            --  MySQL/SQLite:
            --  UPDATE serialized_dag SET data = JSON_SET(JSON_REMOVE(data, '$.client_defaults'), '$.__version', 2)
            --  WHERE JSON_EXTRACT(data, '$.__version') = '3' AND data_compressed IS NULL;
            --
            --  For compressed DAGs: run online migration.
            ------------
            """)
        )
        return

    import gzip
    import json

    connection = op.get_bind()
    dialect = connection.dialect.name

    if dialect == "postgresql":
        # PostgreSQL - pre-filter v3 DAGs to avoid parsing all rows
        connection.execute(
            sa.text("""
                UPDATE serialized_dag
                SET data = jsonb_set(
                    (data::jsonb - 'client_defaults'),
                    '{__version}',
                    '2'
                )::json
                WHERE id IN (
                    SELECT id FROM serialized_dag
                    WHERE data->>'__version' = '3'
                    AND data_compressed IS NULL
                )
            """)
        )
    elif dialect == "mysql":
        connection.execute(
            sa.text("""
                UPDATE serialized_dag
                SET data = JSON_SET(
                    JSON_REMOVE(data, '$.client_defaults'),
                    '$.__version',
                    2
                )
                WHERE JSON_EXTRACT(data, '$.__version') = '3'
                AND data_compressed IS NULL
            """)
        )
    else:
        json_functions_available = False
        try:
            connection.execute(sa.text("SELECT JSON_SET('{}', '$.test', 'value')")).fetchone()
            json_functions_available = True
            print("SQLite JSON functions detected, using optimized SQL approach")
        except Exception:
            print("SQLite JSON functions not available, using Python fallback for JSON processing")

        if json_functions_available:
            connection.execute(
                sa.text("""
                    UPDATE serialized_dag
                    SET data = JSON_SET(
                        JSON_REMOVE(data, '$.client_defaults'),
                        '$.__version',
                        2
                    )
                    WHERE JSON_EXTRACT(data, '$.__version') = '3'
                    AND data_compressed IS NULL
                """)
            )
        else:
            result = connection.execute(
                sa.text("""
                    SELECT id, data
                    FROM serialized_dag
                    WHERE data_compressed IS NULL
                """)
            )

            for row in result:
                dag_id, data_json = row
                try:
                    if data_json is None:
                        continue

                    dag_data = json.loads(data_json)

                    if dag_data.get("__version") != 3:
                        continue

                    if "client_defaults" in dag_data:
                        del dag_data["client_defaults"]
                    dag_data["__version"] = 2

                    new_json = json.dumps(dag_data)
                    connection.execute(
                        sa.text("UPDATE serialized_dag SET data = :data WHERE id = :id"),
                        {"data": new_json, "id": dag_id},
                    )

                except Exception as e:
                    print(f"Failed to downgrade uncompressed DAG {dag_id}: {e}")
                    continue
    try:
        result = connection.execute(
            sa.text("""
                SELECT id, data_compressed
                FROM serialized_dag
                WHERE data_compressed IS NOT NULL
            """)
        )

        for row in result:
            dag_id, compressed_data = row
            try:
                if compressed_data is None:
                    continue

                decompressed = gzip.decompress(compressed_data)
                dag_data = json.loads(decompressed)

                if dag_data.get("__version") != 3:
                    continue

                if "client_defaults" in dag_data:
                    del dag_data["client_defaults"]
                dag_data["__version"] = 2

                new_compressed = gzip.compress(json.dumps(dag_data).encode("utf-8"))
                connection.execute(
                    sa.text("UPDATE serialized_dag SET data_compressed = :data WHERE id = :id"),
                    {"data": new_compressed, "id": dag_id},
                )

            except Exception as e:
                print(f"Failed to downgrade compressed DAG {dag_id}: {e}")
                continue

    except Exception as e:
        print(f"Failed to process compressed DAGs during downgrade: {e}")
        raise
