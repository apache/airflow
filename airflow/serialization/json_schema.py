# -*- coding: utf-8 -*-
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

"""jsonschema for validating serialized DAG and operator."""

import pkgutil
from typing import Iterable

import jsonschema
from typing_extensions import Protocol

from airflow.exceptions import AirflowException
from airflow.settings import json


class Validator(Protocol):
    """
    This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)
    due to the way ``Draft7Validator`` is created. They are created or do not inherit
    from proper classes. Hence you can not have ``type: Draft7Validator``.
    """

    # pylint: disable=unused-argument
    def is_valid(self, instance):
        # type: (...) -> bool
        """Check if the instance is valid under the current schema"""
        pass

    def validate(self, instance):
        # type: (...) -> bool
        """Check if the instance is valid under the current schema, raising validation error if not"""
        pass

    def iter_errors(self, instance):
        # type: (...)  -> Iterable[jsonschema.exceptions.ValidationError]
        """Lazily yield each of the validation errors in the given instance"""
        pass


def load_dag_schema_dict():
    # type: () -> dict
    """
    Load & return Json Schema for DAG as Python dict
    """
    schema_file_name = 'schema.json'
    schema_file = pkgutil.get_data(__name__, schema_file_name)

    if schema_file is None:
        raise AirflowException("Schema file {} does not exists".format(schema_file_name))

    schema = json.loads(schema_file.decode())
    return schema


def load_dag_schema():
    # type: () -> Validator
    """
    Load & Validate Json Schema for DAG
    """
    schema = load_dag_schema_dict()
    jsonschema.Draft7Validator.check_schema(schema)
    return jsonschema.Draft7Validator(schema)
