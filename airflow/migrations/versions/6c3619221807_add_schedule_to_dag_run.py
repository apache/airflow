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

"""add schedule to dag_run

Revision ID: 6c3619221807
Revises: a56c9515abdc
Create Date: 2019-09-11 17:51:01.174446

"""

# revision identifiers, used by Alembic.
revision = '6c3619221807'
down_revision = 'a56c9515abdc'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy import orm, text


def upgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)
    op.add_column('dag_run', sa.Column('schedule_interval', sa.Text(), default=""))

    # get dags schedules and id's
    sql = text("select dag_id, schedule_interval from dag")
    dag_meta = session.execute(sql)
    session.commit()

    for d in dag_meta:
        op.execute(f""" UPDATE dag_run SET schedule_interval = '{d["schedule_interval"]}' WHERE dag_id='{d["dag_id"]}'""")

def downgrade():
    op.drop_column('dag_run', 'schedule_interval')

