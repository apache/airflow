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

"""merge 004c1210f153 and 74effc47d867

Revision ID: b0125267960b
Revises: 004c1210f153, 74effc47d867
Create Date: 2019-10-18 08:45:11.598735

"""

# revision identifiers, used by Alembic.
revision = 'b0125267960b'
down_revision = ('004c1210f153', '74effc47d867')
branch_labels = None
depends_on = None


def upgrade():
    """Apply merge 004c1210f153 and 74effc47d867"""
    pass


def downgrade():
    """Unapply merge 004c1210f153 and 74effc47d867"""
    pass
