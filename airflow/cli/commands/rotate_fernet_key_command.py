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
"""Rotate Fernet key command"""
from airflow.models import Connection, Variable
from airflow.utils import cli as cli_utils
from airflow.utils.session import create_session


@cli_utils.action_cli
def rotate_fernet_key(args):
    """Rotates all encrypted connection credentials and variables"""
    with create_session() as session:
        for conn in session.query(Connection).filter(Connection.is_encrypted | Connection.is_extra_encrypted):
            conn.rotate_fernet_key()
        for var in session.query(Variable).filter(Variable.is_encrypted):
            var.rotate_fernet_key()
