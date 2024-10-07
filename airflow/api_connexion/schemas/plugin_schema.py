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


class PluginSchema(Schema):
    """Plugin schema."""

    name = fields.String(dump_only=True)
    hooks = fields.List(fields.String(dump_only=True))
    executors = fields.List(fields.String(dump_only=True))
    macros = fields.List(fields.String(dump_only=True))
    flask_blueprints = fields.List(fields.String(dump_only=True))
    fastapi_apps = fields.List(fields.Dict(dump_only=True))
    appbuilder_views = fields.List(fields.Dict(dump_only=True))
    appbuilder_menu_items = fields.List(fields.Dict(dump_only=True))
    global_operator_extra_links = fields.List(fields.String(dump_only=True))
    operator_extra_links = fields.List(fields.String(dump_only=True))
    source = fields.String(dump_only=True)
    ti_deps = fields.List(fields.String(dump_only=True))
    listeners = fields.List(fields.String(dump_only=True))
    timetables = fields.List(fields.String(dump_only=True))


class PluginCollection(NamedTuple):
    """Plugin List."""

    plugins: list
    total_entries: int


class PluginCollectionSchema(Schema):
    """Plugin Collection List."""

    plugins = fields.List(fields.Nested(PluginSchema))
    total_entries = fields.Int()


plugin_schema = PluginSchema()
plugin_collection_schema = PluginCollectionSchema()
