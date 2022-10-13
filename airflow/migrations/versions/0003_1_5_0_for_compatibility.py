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
"""Maintain history for compatibility with earlier migrations

Revision ID: 13eb55f81627
Revises: 1507a7289a2f
Create Date: 2015-08-23 05:12:49.732174

"""
from __future__ import annotations

# revision identifiers, used by Alembic.
revision = '13eb55f81627'
down_revision = '1507a7289a2f'
branch_labels = None
depends_on = None
airflow_version = '1.5.0'


def upgrade():
    pass


def downgrade():
    pass
