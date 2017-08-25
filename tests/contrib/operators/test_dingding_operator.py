# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import unittest
import datetime
from airflow import configuration, DAG, operators

DEFAULT_DATE = datetime.datetime(2017, 8, 22)
TEST_DAG_ID = 'unit_test_dingding'
DINGDING_TOKEN = "91fd22f6d9b65091528457e925f15a91e511ccec7ec88693d7d462fcd66cd9a7"
configuration.load_test_config()


class DingDingAPIOperatorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_dingding_link_operator(self):
        import airflow.operators.dingding_operator
        t = operators.dingding_operator.DingDingAPILinkOperator(
            task_id='send_link_msg',
            token=DINGDING_TOKEN,
            title="This is msg title",
            text="This is detail msg",
            messageUrl="https://airflow.incubator.apache.org/",
            dag=self.dag)
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True)

    def test_dingding_text_operator(self):
        import airflow.operators.dingding_operator
        t = operators.dingding_operator.DingDingAPITextOperator(
            task_id='send_text_msg',
            token=DINGDING_TOKEN,
            content="This is my airflow msg",
            isAtAll=True,
            dag=self.dag)
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True)


if __name__ == '__main__':
    unittest.main()
