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
"""Improve sql-server compatibility - removed due to deprecation of db backend

Revision ID: 83f031fd9f1c
Revises: ccde3e26fe78
Create Date: 2021-04-06 12:22:02.197726

"""
from __future__ import annotations

# revision identifiers, used by Alembic.
revision = "83f031fd9f1c"
down_revision = "ccde3e26fe78"
branch_labels = None
depends_on = None
airflow_version = "2.2.0"


def upgrade():
    """Improve compatibility with sql-server backend"""
    pass


def downgrade():
    """Reverse sql-server backend compatibility improvements"""
    pass
