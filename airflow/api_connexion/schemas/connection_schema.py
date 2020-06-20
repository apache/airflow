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
from typing import List, NamedTuple

from marshmallow import Schema, ValidationError, fields, validates_schema
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models.connection import Connection


class ConnectionCollectionItemSchema(SQLAlchemySchema):
    """
    Schema for a connection item
    """

    class Meta:
        """ Meta """
        model = Connection

    connection_id = auto_field('conn_id', required=True)
    conn_type = auto_field(required=True)
    host = auto_field()
    login = auto_field()
    schema = auto_field()
    port = auto_field()

    # Marshmallow 2 doesn't have support for excluding extra field
    # We will be able to remove this when we upgrade to marshmallow 3.
    # To remove it, we would need to set unknown=EXCLUDE in Meta
    @validates_schema(pass_original=True)
    def check_unknown_fields(self, data, original_data):
        """ Validates unknown field """
        unknown = set(original_data) - set(self.fields)
        if unknown:
            raise ValidationError(f'Extra arguments passed: {list(unknown)}')


class ConnectionSchema(ConnectionCollectionItemSchema):  # pylint: disable=too-many-ancestors
    """
    Connection schema
    """

    password = auto_field(load_only=True)
    extra = auto_field()


class ConnectionCollection(NamedTuple):
    """ List of Connections with meta"""
    connections: List[Connection]
    total_entries: int


class ConnectionCollectionSchema(Schema):
    """ Connection Collection Schema"""
    connections = fields.List(fields.Nested(ConnectionCollectionItemSchema))
    total_entries = fields.Int()


connection_schema = ConnectionSchema(strict=True)
connection_collection_item_schema = ConnectionCollectionItemSchema(strict=True)
connection_collection_schema = ConnectionCollectionSchema(strict=True)
