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
"""add-k8s-yaml-to-rendered-templates

Revision ID: 45ba3f1493b9
Revises: 364159666cbd
Create Date: 2020-10-23 23:01:52.471442

"""
from __future__ import annotations

import sqlalchemy_jsonfield
from alembic import op
from sqlalchemy import Column

from airflow.settings import json

# revision identifiers, used by Alembic.
revision = '45ba3f1493b9'
down_revision = '364159666cbd'
branch_labels = None
depends_on = None
airflow_version = '2.0.0'

__tablename__ = "rendered_task_instance_fields"
k8s_pod_yaml = Column('k8s_pod_yaml', sqlalchemy_jsonfield.JSONField(json=json), nullable=True)


def upgrade():
    """Apply add-k8s-yaml-to-rendered-templates"""
    with op.batch_alter_table(__tablename__, schema=None) as batch_op:
        batch_op.add_column(k8s_pod_yaml)


def downgrade():
    """Unapply add-k8s-yaml-to-rendered-templates"""
    with op.batch_alter_table(__tablename__, schema=None) as batch_op:
        batch_op.drop_column('k8s_pod_yaml')
