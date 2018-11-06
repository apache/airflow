# -*- coding: utf-8 -*-
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

"""Add notification_sent column to sla_miss

Revision ID: bbc73705a13e
Revises: 4446e08588
Create Date: 2016-01-14 18:05:54.871682

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'bbc73705a13e'
down_revision = '4446e08588'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('sla_miss', sa.Column('notification_sent', sa.Boolean, default=False))


def downgrade():
    op.drop_column('sla_miss', 'notification_sent')
