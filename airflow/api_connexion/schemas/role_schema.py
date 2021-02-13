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

from flask_appbuilder.security.sqla.models import Role
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class RoleCollectionItemSchema(SQLAlchemySchema):
    """Role item shema"""

    class Meta:
        """Meta"""

        model = Role

    id = auto_field(dump_only=True)
    name = auto_field()
    permissions = auto_field()


class RoleCollection(NamedTuple):
    """List of roles"""

    roles: List[Role]
    total_entries: int


class RoleCollectionSchema(Schema):
    """List of roles"""

    roles = fields.List(fields.Nested(RoleCollectionItemSchema))
    total_entries = fields.Int()


role_collection_item_schema = RoleCollectionSchema()
role_collection_schema = RoleCollectionSchema()
