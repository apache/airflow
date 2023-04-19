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

from pydantic import BaseModel as BaseModelPydantic,Field
from airflow.models.connection import Connection


class ConnectionPydantic(BaseModelPydantic):
    """Serializable representation of the Connection ORM SqlAlchemyModel used by internal API."""

    conn_id: str
    conn_type: str
    description: str
    host: str

    conn_schema:str= Field(alias="schema")
    login: str
    _password: str
    port: int
    is_encrypted: bool
    is_extra_encrypted: bool
    _extra: str

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        from_attributes = True
        orm_mode = True  # Pydantic 1.x compatibility.

    def to_orm_connection(self) -> Connection:
        return Connection(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            description=self.description,
            host=self.host,
            schema=self.conn_schema,
            login=self.login,
            password=self._password,
            port=self.port,
            extra=self._extra,
        )
