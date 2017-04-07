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
#

import unittest
import datetime
from mock import Mock
from mock import patch

from airflow import DAG, configuration
from airflow.contrib.sensors.jira_sensor import JiraTicketSensor
from airflow import models
from airflow.utils import db

DEFAULT_DATE = datetime.datetime(2017, 1, 1)
jira_client_mock = Mock(
        name="jira_client_for_test"
)

minimal_test_ticket = {
    "id": "911539",
    "self": "https://sandbox.localhost/jira/rest/api/2/issue/911539",
    "key": "TEST-1226",
    "fields": {
        "labels": [
            "test-label-1",
            "test-label-2"
        ],
        "description": "this is a test description",
    }
}


class TestJiraSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag
        db.merge_conn(
                models.Connection(
                        conn_id='jira_default', conn_type='jira',
                        host='https://localhost/jira/', port=443,
                        extra='{"verify": "False", "project": "AIRFLOW"}'))

    @patch("airflow.contrib.hooks.jira_hook.JIRA",
           autospec=True, return_value=jira_client_mock)
    def test_issue_label_set(self, jira_mock):
        jira_mock.return_value.issue.return_value = minimal_test_ticket

        ticket_label_sensor = JiraTicketSensor(task_id='search-ticket-test',
                                               ticket_id='TEST-1226',
                                               field_checker_func=
                                               TestJiraSensor.field_checker_func,
                                               timeout=518400,
                                               poke_interval=10,
                                               dag=self.dag)

        ticket_label_sensor.run(start_date=DEFAULT_DATE,
                                end_date=DEFAULT_DATE, ignore_ti_state=True)

        self.assertTrue(jira_mock.called)
        self.assertTrue(jira_mock.return_value.issue.called)

    @staticmethod
    def field_checker_func(context, issue):
        return "test-label-1" in issue['fields']['labels']


if __name__ == '__main__':
    unittest.main()
