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
from datetime import datetime

from airflow.macros import hive


class TestHive(unittest.TestCase):
    def test_closest_ds_partition(self):
        date1 = datetime.strptime('2017-04-24', '%Y-%m-%d')
        date2 = datetime.strptime('2017-04-25', '%Y-%m-%d')
        date3 = datetime.strptime('2017-04-26', '%Y-%m-%d')
        date4 = datetime.strptime('2017-04-28', '%Y-%m-%d')
        date5 = datetime.strptime('2017-04-29', '%Y-%m-%d')
        target_dt = datetime.strptime('2017-04-27', '%Y-%m-%d')
        date_list = [date1, date2, date3, date4, date5]

        self.assertEqual("2017-04-26", str(hive._closest_date(target_dt, date_list, True)))
        self.assertEqual("2017-04-28", str(hive._closest_date(target_dt, date_list, False)))

        # when before is not set, the closest date should be returned
        self.assertEqual("2017-04-26", str(hive._closest_date(target_dt, [date1, date2, date3, date5], None)))
        self.assertEqual("2017-04-28", str(hive._closest_date(target_dt, [date1, date2, date4, date5])))
        self.assertEqual("2017-04-26", str(hive._closest_date(target_dt, date_list)))


if __name__ == '__main__':
    unittest.main()
