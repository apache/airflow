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

"""Set conn_type as non-nullable

Revision ID: 8f966b9c467a
Revises: 3c20cacc0044
Create Date: 2020-06-08 22:36:34.534121

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = "8f966b9c467a"
down_revision = "3c20cacc0044"
branch_labels = None
depends_on = None


def upgrade():
    """Apply Set conn_type as non-nullable"""
    Base = declarative_base()

    class Connection(Base):
        __tablename__ = "connection"

        id = sa.Column(sa.Integer(), primary_key=True)
        conn_id = sa.Column(sa.String(250))
        conn_type = sa.Column(sa.String(500))

    # Generate run type for existing records
    connection = op.get_bind()
    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=connection)

    # imap_default was missing it's type, let's fix that up
    session.query(Connection).filter_by(conn_id="imap_default", conn_type=None).update(
        {Connection.conn_type: "imap"}, synchronize_session=False
    )
    session.commit()

    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("conn_type", existing_type=sa.VARCHAR(length=500), nullable=False)


def downgrade():
    """Unapply Set conn_type as non-nullable"""
    with op.batch_alter_table("connection", schema=None) as batch_op:
        batch_op.alter_column("conn_type", existing_type=sa.VARCHAR(length=500), nullable=True)
