
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



"""Add DAG-level retry fields.



Revision ID: 0101_3_2_0

Revises: 0100_3_2_0

Create Date: 2026-02-01 12:00:00.000000



"""



from __future__ import annotations



import sqlalchemy as sa

from alembic import op



# revision identifiers, used by Alembic.

revision = "0101_3_2_0"

down_revision = "0100_3_2_0"

branch_labels = None

depends_on = None

airflow_version = "3.2.0"





def upgrade():

    """Add DAG-level retry fields to dag_run and dag tables."""

    # Add columns to dag_run table

    with op.batch_alter_table("dag_run") as batch_op:

        batch_op.add_column(

            sa.Column("dag_try_number", sa.Integer(), nullable=False, server_default="0")

        )

        batch_op.add_column(

            sa.Column("dag_max_tries", sa.Integer(), nullable=False, server_default="-1")

        )



    # Add columns to dag table

    with op.batch_alter_table("dag") as batch_op:

        batch_op.add_column(

            sa.Column("max_dag_retries", sa.Integer(), nullable=False, server_default="0")

        )

        batch_op.add_column(

            sa.Column("dag_retry_delay", sa.Integer(), nullable=True)

        )





def downgrade():

    """Remove DAG-level retry fields from dag_run and dag tables."""

    # Remove columns from dag table

    with op.batch_alter_table("dag") as batch_op:

        batch_op.drop_column("dag_retry_delay")

        batch_op.drop_column("max_dag_retries")



    # Remove columns from dag_run table

    with op.batch_alter_table("dag_run") as batch_op:

        batch_op.drop_column("dag_max_tries")

        batch_op.drop_column("dag_try_number")

