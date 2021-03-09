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

from flask_appbuilder.security.sqla.models import Permission, PermissionView, Role
from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field


class ActionCollectionItemSchema(SQLAlchemySchema):
    """Permission Action Schema"""

    class Meta:
        """Meta"""

        model = Permission

    name = auto_field()


class ActionCollection(NamedTuple):
    """Permission Action Collection"""

    actions: List[Permission]
    total_entries: int


class ActionCollectionSchema(Schema):
    """Permissions list schema"""

    actions = fields.List(fields.Nested(ActionCollectionItemSchema))
    total_entries = fields.Int()


class ActionResourceSchema(SQLAlchemySchema):
    """Permission View Schema"""

    class Meta:
        """Meta"""

        model = PermissionView

    permission = fields.Method("get_action_name", deserialize="get_related", data_key="action")
    view_menu = fields.Method('get_resource_name', deserialize="get_related", data_key="resource")

    @staticmethod
    def get_action_name(obj: PermissionView):
        """Get permission name"""
        return obj.permission.name

    @staticmethod
    def get_resource_name(obj: PermissionView):
        """Get resource name"""
        return obj.view_menu.name

    def get_related(self, value):
        """Load value as it is in the related model"""
        return {"name": value}


class RoleCollectionItemSchema(SQLAlchemySchema):
    """Role item schema"""

    class Meta:
        """Meta"""

        model = Role

    name = auto_field()
    permissions = fields.List(fields.Nested(ActionResourceSchema), data_key='actions')


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
action_collection_item_schema = ActionCollectionItemSchema()
action_collection_schema = ActionCollectionSchema()
