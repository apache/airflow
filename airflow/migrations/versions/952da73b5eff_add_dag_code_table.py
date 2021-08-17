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

"""add dag_code table

Revision ID: 952da73b5eff
Revises: 852ae6c715af
Create Date: 2020-03-12 12:39:01.797462

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from airflow.models.dagcode import DagCode

revision = '952da73b5eff'
down_revision = '852ae6c715af'
branch_labels = None
depends_on = None


def upgrade():
    """Create DagCode Table."""
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class SerializedDagModel(Base):
        __tablename__ = 'serialized_dag'

        # There are other columns here, but these are the only ones we need for the SELECT/UPDATE we are doing
        dag_id = sa.Column(sa.String(250), primary_key=True)
        fileloc = sa.Column(sa.String(2000), nullable=False)
        fileloc_hash = sa.Column(sa.BigInteger, nullable=False)

    """Apply add source code table"""
    op.create_table(
        'dag_code',
        sa.Column('fileloc_hash', sa.BigInteger(), nullable=False, primary_key=True, autoincrement=False),
        sa.Column('fileloc', sa.String(length=2000), nullable=False),
        sa.Column('source_code', sa.UnicodeText(), nullable=False),
        sa.Column('last_updated', sa.TIMESTAMP(timezone=True), nullable=False),
    )

    conn = op.get_bind()
    if conn.dialect.name != 'sqlite':
        if conn.dialect.name == "mssql":
            op.drop_index('idx_fileloc_hash', 'serialized_dag')

        op.alter_column(
            table_name='serialized_dag', column_name='fileloc_hash', type_=sa.BigInteger(), nullable=False
        )
        if conn.dialect.name == "mssql":
            op.create_index('idx_fileloc_hash', 'serialized_dag', ['fileloc_hash'])

    sessionmaker = sa.orm.sessionmaker()
    session = sessionmaker(bind=conn)
    serialized_dags = session.query(SerializedDagModel).all()
    for dag in serialized_dags:
        dag.fileloc_hash = DagCode.dag_fileloc_hash(dag.fileloc)
        session.merge(dag)
    session.commit()


def downgrade():
    """Unapply add source code table"""
    op.drop_table('dag_code')
