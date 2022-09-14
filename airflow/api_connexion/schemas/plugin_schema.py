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
    """Plugin schema"""

    name = fields.String()
    hooks = fields.List(fields.String())
    executors = fields.List(fields.String())
    macros = fields.List(fields.Dict())
    flask_blueprints = fields.List(fields.Dict())
    appbuilder_views = fields.List(fields.Dict())
    appbuilder_menu_items = fields.List(fields.Dict())
    global_operator_extra_links = fields.List(fields.Dict())
    operator_extra_links = fields.List(fields.Dict())
    source = fields.String()


class PluginCollection(NamedTuple):
    """Plugin List"""

    plugins: list
    total_entries: int


class PluginCollectionSchema(Schema):
    """Plugin Collection List"""

    plugins = fields.List(fields.Nested(PluginSchema))
    total_entries = fields.Int()


plugin_schema = PluginSchema()
plugin_collection_schema = PluginCollectionSchema()
