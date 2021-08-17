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


"""kubernetes_resource_checkpointing

Revision ID: 33ae817a1ff4
Revises: 947454bf1dff
Create Date: 2017-09-11 15:26:47.598494

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = '33ae817a1ff4'
down_revision = 'd2ae31099d61'
branch_labels = None
depends_on = None

RESOURCE_TABLE = "kube_resource_version"


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)

    if RESOURCE_TABLE not in inspector.get_table_names():
        columns_and_constraints = [
            sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
            sa.Column("resource_version", sa.String(255)),
        ]

        # alembic creates an invalid SQL for mssql and mysql dialects
        if conn.dialect.name in {"mysql"}:
            columns_and_constraints.append(
                sa.CheckConstraint("one_row_id<>0", name="kube_resource_version_one_row_id")
            )
        elif conn.dialect.name not in {"mssql"}:
            columns_and_constraints.append(
                sa.CheckConstraint("one_row_id", name="kube_resource_version_one_row_id")
            )

        table = op.create_table(RESOURCE_TABLE, *columns_and_constraints)
        op.bulk_insert(table, [{"resource_version": ""}])


def downgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)

    if RESOURCE_TABLE in inspector.get_table_names():
        op.drop_table(RESOURCE_TABLE)
