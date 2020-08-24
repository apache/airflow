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

from marshmallow import Schema, fields


class ConfigOptionSchema(Schema):
    """Config Option Schema"""
    key = fields.String(required=True)
    value = fields.String(required=True)


class ConfigOption(NamedTuple):
    """Config option"""
    key: str
    value: str


class ConfigSectionSchema(Schema):
    """Config Section Schema"""
    name = fields.String(required=True)
    options = fields.List(fields.Nested(ConfigOptionSchema))


class ConfigSection(NamedTuple):
    """List of config options within a section"""
    name: str
    options: List[ConfigOption]


class ConfigSchema(Schema):
    """Config Schema"""
    sections = fields.List(fields.Nested(ConfigSectionSchema))


class Config(NamedTuple):
    """List of config sections with their options"""
    sections: List[ConfigSection]


config_schema = ConfigSchema()
