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
from marshmallow import post_dump
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

from airflow.models import XCom


class XComCollectionItemSchema(SQLAlchemySchema):

    class Meta:
        """ Meta """
        model = XCom

    COLLECTION_NAME = 'xcom_entries'
    FIELDS_FROM_NONE_TO_EMPTY_STRING = ['key', 'task_id', 'dag_id']

    key = auto_field()
    timestamp = auto_field()
    execution_date = auto_field()
    task_id = auto_field()
    dag_id = auto_field()

    @post_dump(pass_many=True)
    def wrap_with_envelope(self, data, many, **kwargs):
        """
        :param data: Deserialized data
        :param many: Collection or an item
        """
        if many:
            data = self._process_list_data(data)
            return {self.COLLECTION_NAME: data, 'total_entries': len(data)}
        data = self._process_data(data)
        return data

    def _process_list_data(self, data):
        return [self._process_data(x) for x in data]

    def _process_data(self, data):
        for key in self.FIELDS_FROM_NONE_TO_EMPTY_STRING:
            if not data[key]:
                data.update({key: ''})
        return data


class XComSchema(XComCollectionItemSchema):

    value = auto_field()


xcom_schema = XComSchema()
xcom_collection_item_schema = XComCollectionItemSchema()
xcom_collection_schema = XComCollectionItemSchema(many=True)
