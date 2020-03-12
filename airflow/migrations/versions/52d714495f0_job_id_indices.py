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

"""job_id indices

Revision ID: 52d714495f0
Revises: 338e90f54d61
Create Date: 2015-10-20 03:17:01.962542

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '52d714495f0'
down_revision = '338e90f54d61'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('idx_job_state_heartbeat', 'job',
                    ['state', 'latest_heartbeat'], unique=False)


def downgrade():
    op.drop_index('idx_job_state_heartbeat', table_name='job')
