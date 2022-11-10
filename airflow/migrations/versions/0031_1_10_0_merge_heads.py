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
"""Merge migrations Heads

Revision ID: 05f30312d566
Revises: 86770d1215c0, 0e2a74e0fc9f
Create Date: 2018-06-17 10:47:23.339972

"""
from __future__ import annotations

# revision identifiers, used by Alembic.
revision = "05f30312d566"
down_revision = ("86770d1215c0", "0e2a74e0fc9f")
branch_labels = None
depends_on = None
airflow_version = "1.10.0"


def upgrade():
    pass


def downgrade():
    pass
