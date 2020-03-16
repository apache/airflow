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

"""increase_connection_extra_field_size

Revision ID: da4f5a412d34
Revises: 7ba99d720ac4
Create Date: 2020-03-16 12:06:39.363963

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'da4f5a412d34'
down_revision = '7ba99d720ac4'
branch_labels = None
depends_on = None


def upgrade():
    """Apply increase_connection_extra_field_size"""
    pass


def downgrade():
    """Unapply increase_connection_extra_field_size"""
    pass
