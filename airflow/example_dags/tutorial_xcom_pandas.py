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

# pylint: disable=missing-function-docstring
"""
### Custom Xcom back-end to support Pandas

This is a simple example to illustrate an option to extend Xcoms for use
with Pandas dataframes by storing Pandas within the local file system and
passing them around between tasks.
Clearly, this would only work on the same processing node, but this is
intended to generalized to storage on S3, GCS, or equivalent shared volumes for
distributed task execution.
"""

import dbm
import json
import os
import pathlib
import random
import string
from typing import Any

import pandas as pd

from airflow.models.xcom import BaseXCom

CUSTOM_XCOM_DATA_DIR = 'tmp_xcom_data'


class CustomXcomLocalForPandas(BaseXCom):
    """Example Custom Xcom persistence class - extends base to support Pandas Dataframes."""

    @staticmethod
    def init_dir(xcom_data_path):
        pathlib.Path(xcom_data_path).mkdir(parents=True, exist_ok=True)
        print("Directory %s created" % xcom_data_path)

        xcom_data_index = os.path.join(xcom_data_path, 'xcom_index')
        return xcom_data_index

    @staticmethod
    def get_random_string():
        random_result = ''.join(random.choices(string.ascii_uppercase + string.digits, k=32))
        return random_result

    @staticmethod
    def write_value_file(filename, obj):
        try:
            if isinstance(obj, pd.DataFrame):
                obj_type = 'dataframe'
                obj_json = obj.to_json(orient="split")
            else:
                obj_type = 'native'
                obj_json = json.dumps(obj)
        except TypeError:
            print("Could not serialize value into JSON")

        print('Writing object of type: ' + obj_type)
        try:
            value_data_file = open(filename, 'w')
            value_data_file.write(obj_json)
            value_data_file.close()
        except OSError:
            print("Could not create file and write value")

        return obj_type

    @staticmethod
    def read_value_file(filename, obj_type):
        print('Reading object of type: ' + obj_type + ' from ' + filename)
        try:
            value_data_file = open(filename)
            data = value_data_file.read()
            value_data_file.close()
        except OSError:
            print("Could not open file and read value")

        try:
            if obj_type == 'dataframe':
                value = pd.read_json(data, orient='split')
                if isinstance(value, pd.DataFrame):
                    print('Success in reading dataframe')
                else:
                    print('Failed in reading dataframe')
                print(value)
            else:
                value = json.loads(data)
        except TypeError:
            print("Could not deserialize value from JSON")

        return value

    @staticmethod
    def write_value(value):
        xcom_data_path = os.path.join('/tmp', CUSTOM_XCOM_DATA_DIR)
        xcom_data_index = CustomXcomLocalForPandas.init_dir(xcom_data_path)

        key_str = CustomXcomLocalForPandas.get_random_string()
        print('Storing object with key_str = ' + key_str + ', and value = ')
        print(value)
        object_type = CustomXcomLocalForPandas.write_value_file(os.path.join(xcom_data_path, key_str), value)

        index_db = dbm.open(xcom_data_index, 'c')
        index_db[key_str] = object_type
        index_db.close()

        return key_str

    @staticmethod
    def read_value(key_str_value):
        xcom_data_path = os.path.join('/tmp', CUSTOM_XCOM_DATA_DIR)
        xcom_data_index = CustomXcomLocalForPandas.init_dir(xcom_data_path)

        read_db = dbm.open(xcom_data_index, 'r')

        type_str = read_db.get(key_str_value).decode()
        value = CustomXcomLocalForPandas.read_value_file(
            os.path.join(xcom_data_path, key_str_value), type_str
        )

        read_db.close()
        return value

    @staticmethod
    def serialize_value(value: Any):
        key_str_value = CustomXcomLocalForPandas.write_value(value)
        return key_str_value.encode('UTF-8')

    @staticmethod
    def deserialize_value(result) -> Any:
        key_str_value = result.value.decode('UTF-8')
        value = CustomXcomLocalForPandas.read_value(key_str_value)
        return value
