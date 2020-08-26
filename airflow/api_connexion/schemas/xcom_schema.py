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
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models import XCom


class XComCollectionItemSchema(SQLAlchemySchema):
    """
    Schema for a xcom item
    """

    class Meta:
        """ Meta """
        model = XCom

    key = auto_field()
    timestamp = auto_field()
    execution_date = auto_field()
    task_id = auto_field()
    dag_id = auto_field()


class XComSchema(XComCollectionItemSchema):
    """
    XCom schema
    """

    value = auto_field()


class XComCollection(NamedTuple):
    """ List of XComs with meta"""
    xcom_entries: List[XCom]
    total_entries: int


class XComCollectionSchema(Schema):
    """ XCom Collection Schema"""
    xcom_entries = fields.List(fields.Nested(XComCollectionItemSchema))
    total_entries = fields.Int()


xcom_schema = XComSchema()
xcom_collection_item_schema = XComCollectionItemSchema()
xcom_collection_schema = XComCollectionSchema()
