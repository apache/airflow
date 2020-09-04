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

from __future__ import absolute_import

from airflow.models import Connection
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.db import provide_session


class ConnTypeIsNotNullableRule(BaseRule):

    title = "Connection.conn_type is not nullable"

    description = """\
The `conn_type` column in the `connection` table must contain content. Previously, this rule was \
enforced by application logic, but was not enforced by the database schema.

If you made any modifications to the table directly, make sure you don't have null in the conn_type column.\
"""

    @provide_session
    def check(self, session=None):
        invalid_connections = session.query(Connection).filter(Connection.conn_type.is_(None))
        return (
            'Connection<id={}", conn_id={}> have empty conn_type field.'.format(conn.id, conn.conn_id)
            for conn in invalid_connections
        )
