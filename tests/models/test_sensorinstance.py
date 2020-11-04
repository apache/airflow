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

import unittest

from airflow.models import SensorInstance
from airflow.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensor
from airflow.sensors.python import PythonSensor


class SensorInstanceTest(unittest.TestCase):
    def test_get_classpath(self):
        # Test the classpath in/out airflow
        obj1 = NamedHivePartitionSensor(partition_names=['test_partition'], task_id='meta_partition_test_1')
        obj1_classpath = SensorInstance.get_classpath(obj1)
        obj1_importpath = (
            "airflow.providers.apache.hive.sensors.named_hive_partition.NamedHivePartitionSensor"
        )

        self.assertEqual(obj1_classpath, obj1_importpath)

        def test_callable():
            return

        obj3 = PythonSensor(python_callable=test_callable, task_id='python_sensor_test')
        obj3_classpath = SensorInstance.get_classpath(obj3)
        obj3_importpath = "airflow.sensors.python.PythonSensor"

        self.assertEqual(obj3_classpath, obj3_importpath)
