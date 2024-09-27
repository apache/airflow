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
placeholder migration.

Revision ID: 6709f7a774b9
Revises:
Create Date: 2024-09-03 17:06:38.040510

Note: This is a placeholder migration used to stamp the migration
when we create the migration from the ORM. Otherwise, it will run
without stamping the migration, leading to subsequent changes to
the tables not being migrated.
"""

from __future__ import annotations

# revision identifiers, used by Alembic.
revision = "6709f7a774b9"
down_revision = None
branch_labels = None
depends_on = None
fab_version = "1.4.0"


def upgrade() -> None: ...


def downgrade() -> None: ...
