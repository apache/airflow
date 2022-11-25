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
from __future__ import annotations

from typing import NamedTuple

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.www.fab_security.sqla.models import Action, Permission, Resource, Role


class ActionSchema(SQLAlchemySchema):
    """Action Schema."""

    class Meta:
        """Meta."""

        model = Action

    name = auto_field()


class ResourceSchema(SQLAlchemySchema):
    """View menu Schema."""

    class Meta:
        """Meta."""

        model = Resource

    name = auto_field()


class ActionCollection(NamedTuple):
    """Action Collection."""

    actions: list[Action]
    total_entries: int


class ActionCollectionSchema(Schema):
    """Permissions list schema."""

    actions = fields.List(fields.Nested(ActionSchema))
    total_entries = fields.Int()


class ActionResourceSchema(SQLAlchemySchema):
    """Action View Schema."""

    class Meta:
        """Meta."""

        model = Permission

    action = fields.Nested(ActionSchema, data_key="action")
    resource = fields.Nested(ResourceSchema, data_key="resource")


class RoleSchema(SQLAlchemySchema):
    """Role item schema."""

    class Meta:
        """Meta."""

        model = Role

    name = auto_field()
    permissions = fields.List(fields.Nested(ActionResourceSchema), data_key="actions")


class RoleCollection(NamedTuple):
    """List of roles."""

    roles: list[Role]
    total_entries: int


class RoleCollectionSchema(Schema):
    """List of roles."""

    roles = fields.List(fields.Nested(RoleSchema))
    total_entries = fields.Int()


role_schema = RoleSchema()
role_collection_schema = RoleCollectionSchema()
action_collection_schema = ActionCollectionSchema()
