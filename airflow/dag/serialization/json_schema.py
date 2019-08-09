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

from typing import Any, Dict, Optional

from airflow.dag.serialization.enum import DagAttributeTypes as DAT, Encoding


def make_object_schema(
        var_schema: Optional[dict] = None, type_enum: Optional[list] = None) -> dict:
    """jsonschema of an encoded object.
    :param var_schema: Json schema of variable
    :param type_enum: Used to restrict a value to a fixed set of values.
    """
    schema = {
        'type': 'object',
        'properties': {
            Encoding.TYPE.value: {'type': 'string'}
        },
        'required': [
            Encoding.TYPE.value,
            Encoding.VAR.value
        ]
    }   # type: Dict[str, Any]
    if var_schema is not None:
        schema['properties'][Encoding.VAR.value] = var_schema

    if type_enum is not None:
        schema['properties'][Encoding.TYPE.value]['enum'] = type_enum

    return schema


def make_operator_schema() -> dict:
    """jsonschema of a serialized operator."""
    return make_object_schema(
        var_schema={
            'type': 'object',
            'properties': {
                'task_id': {'type': 'string'},
                'owner': {'type': 'string'},
                'start_date': make_object_schema(
                    var_schema={'type': 'string'},
                    type_enum=[DAT.DATETIME.value]),
                'trigger_rule': {'type': 'string'},
                'depends_on_past': {'type': 'boolean'},
                'wait_for_downstream': {'type': 'boolean'},
                'retries': {'type': 'number'},
                'queue': {'type': 'string'},
                'pool': {'type': 'string'},
                'retry_delay': make_object_schema(
                    var_schema={'type': 'number'},
                    type_enum=[DAT.TIMEDELTA.value]),
                'retry_exponential_backoff': {'type': 'boolean'},
                'params': make_object_schema(
                    var_schema={'type': 'object'},
                    type_enum=[DAT.DICT.value]),
                'priority_weight': {'type': 'number'},
                'weight_rule': {'type': 'string'},
                'executor_config': make_object_schema(
                    var_schema={'type': 'object'},
                    type_enum=[DAT.DICT.value]),
                'do_xcom_push': {'type': 'boolean'},
                # _dag field must be a dag_id.
                '_dag': make_object_schema(
                    var_schema={'type': 'string'},
                    type_enum=[DAT.DAG.value]),
                'ui_color': {'type': 'string'},
                'ui_fgcolor': {'type': 'string'},
                'template_fields': {
                    'anyOf': [
                        {
                            'type': 'array',
                            'items': {'type': 'string'}
                        },
                        make_object_schema(
                            var_schema={'type': 'array'},
                            type_enum=[DAT.TUPLE.value]),
                    ]
                }
            },
            'required': [
                'task_id', 'owner', 'start_date', '_dag',
                'ui_color', 'ui_fgcolor', 'template_fields']
        },
        type_enum=[DAT.OP.value])


def make_dag_schema() -> dict:
    """jsonschema of a serialized DAG."""
    return make_object_schema(
        var_schema={
            'type': 'object',
            'properties': {
                'default_args': make_object_schema(var_schema={'type': 'object'}),
                'params': make_object_schema(var_schema={'type': 'object'}),
                '_dag_id': {'type': 'string'},
                'task_dict': make_object_schema(
                    var_schema={
                        'type': 'object',
                        'additionalProperties': make_operator_schema()
                    },
                    type_enum=[DAT.DICT.value]),
                'timezone': make_object_schema(
                    var_schema={'type': 'string'},
                    type_enum=[DAT.TIMEZONE.value]),
                'schedule_interval': {
                    'anyOf': [
                        {'type': 'string'},  # cron format.
                        make_object_schema(
                            var_schema={'type': 'number'},
                            type_enum=[DAT.TIMEDELTA.value])]
                },
                'catchup': {'type': 'boolean'},
                'is_subdag': {'type': 'boolean'}
            },
            'required': [
                'default_args', 'params',
                '_dag_id', 'fileloc', 'task_dict']
        },
        type_enum=[DAT.DAG.value]
    )
