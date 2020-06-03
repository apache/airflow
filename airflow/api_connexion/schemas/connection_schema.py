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

from marshmallow_sqlalchemy import auto_field

from airflow.api_connexion.schemas.base_schema import BaseSchema
from airflow.models.connection import Connection


class ConnectionCollectionItemSchema(BaseSchema):

    """
    Schema for a connection item
    """
    class Meta:
        """ Meta """
        model = Connection

    COLLECTION_NAME = 'connections'
    FIELDS_FROM_NONE_TO_EMPTY_STRING = ['connection_id', 'conn_type', 'host', 'login', 'schema']
    FIELDS_FROM_NONE_TO_ZERO = ['port']

    conn_id = auto_field(dump_to='connection_id', load_from='connection_id')
    conn_type = auto_field()
    host = auto_field()
    login = auto_field()
    schema = auto_field()
    port = auto_field()


class ConnectionSchema(ConnectionCollectionItemSchema):  # pylint: disable=too-many-ancestors
    """
    Connection schema
    """
    FIELDS_FROM_NONE_TO_EMPTY_STRING = ConnectionCollectionItemSchema.\
        FIELDS_FROM_NONE_TO_EMPTY_STRING + ['extra']
    password = auto_field(load_only=True)
    extra = auto_field()


connection_schema = ConnectionSchema()
connection_collection_item_schema = ConnectionCollectionItemSchema()
connection_collection_schema = ConnectionCollectionItemSchema(many=True)
