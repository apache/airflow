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

from flask_appbuilder.security.sqla.models import Permission, PermissionView, Role, ViewMenu
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class ViewMenuCollectionItemSchema(SQLAlchemySchema):
    """ViewMenu Schema"""

    class Meta:
        """Meta"""

        model = ViewMenu

    id = auto_field()
    name = auto_field()


class ViewMenuCollection(NamedTuple):
    """ViewMenu list"""

    view_menus: List[ViewMenu]
    total_entries: int


class ViewMenuCollectionSchema(Schema):
    """ViewMenu list schema"""

    view_menus = fields.List(fields.Nested(ViewMenuCollectionItemSchema))
    total_entries = fields.Int()


class PermissionCollectionItemSchema(SQLAlchemySchema):
    """Permission Schema"""

    class Meta:
        """Meta"""

        model = Permission

    id = auto_field()
    name = auto_field()


class PermissionCollection(NamedTuple):
    """Permission Collection"""

    permissions: List[Permission]
    total_entries: int


class PermissionCollectionSchema(Schema):
    """Permissions list schema"""

    permissions = fields.List(fields.Nested(PermissionCollectionItemSchema))
    total_entries = fields.Int()


class PermissionViewSchema(SQLAlchemySchema):
    """Permission View Schema"""

    class Meta:
        """Meta"""

        model = PermissionView

    id = auto_field()
    permission = fields.Nested(PermissionCollectionItemSchema)
    view_menu = fields.Nested(ViewMenuCollectionItemSchema)


class RoleCollectionItemSchema(SQLAlchemySchema):
    """Role item shema"""

    class Meta:
        """Meta"""

        model = Role

    id = auto_field()
    name = auto_field()
    permissions = fields.List(fields.Nested(PermissionViewSchema))


class RoleCollection(NamedTuple):
    """List of roles"""

    roles: List[Role]
    total_entries: int


class RoleCollectionSchema(Schema):
    """List of roles"""

    roles = fields.List(fields.Nested(RoleCollectionItemSchema))
    total_entries = fields.Int()


role_collection_item_schema = RoleCollectionItemSchema()
role_collection_schema = RoleCollectionSchema()
