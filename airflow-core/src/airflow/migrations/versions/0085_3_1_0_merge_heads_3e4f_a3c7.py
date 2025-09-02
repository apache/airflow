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

"""
merge heads 3e4f, a3c7

Revision ID: 2d6e7c3a3864
Revises: 3e4f92b7c1a2, a3c7f2b18d4e
Create Date: 2025-09-01 13:24:29.575087

"""
from __future__ import annotations

# revision identifiers, used by Alembic.
revision = "2d6e7c3a3864"
down_revision = ('3e4f92b7c1a2', 'a3c7f2b18d4e')
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


def upgrade():
    """Apply merge heads 3e4f, a3c7"""
    pass


def downgrade():
    """Unapply merge heads 3e4f, a3c7"""
    pass
