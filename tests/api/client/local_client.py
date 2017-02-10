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

import json
import unittest
import datetime

from mock import patch

from airflow import AirflowException
from airflow import models

from airflow.api.client.local_client import Client
from airflow.utils.state import State

EXECDATE = datetime.datetime.now()
EXECDATE_NOFRACTIONS = EXECDATE.replace(microsecond=0)
EXECDATE_ISO = EXECDATE_NOFRACTIONS.isoformat()

real_datetime_class = datetime.datetime


def mock_datetime_now(target, dt):
    class DatetimeSubclassMeta(type):
        @classmethod
        def __instancecheck__(mcs, obj):
            return isinstance(obj, real_datetime_class)

    class BaseMockedDatetime(real_datetime_class):
        @classmethod
        def now(cls, tz=None):
            return target.replace(tzinfo=tz)

        @classmethod
        def utcnow(cls):
            return target

    # Python2 & Python3 compatible metaclass
    MockedDatetime = DatetimeSubclassMeta('datetime', (BaseMockedDatetime,), {})

    return patch.object(dt, 'datetime', MockedDatetime)


class TestLocalClient(unittest.TestCase):
    def setUp(self):
        self.client = Client(api_base_url=None, auth=None)

    @patch.object(models.DAG, 'create_dagrun')
    def test_trigger_dag(self, mock):
        client = self.client

        # non existent
        with self.assertRaises(AirflowException):
            client.trigger_dag(dag_id="blablabla")

        import airflow.api.common.experimental.trigger_dag
        with mock_datetime_now(EXECDATE, airflow.api.common.experimental.trigger_dag.datetime):
            # no execution date, execution date should be set automatically
            client.trigger_dag(dag_id="test_start_date_scheduling")
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # execution date with microseconds cutoff
            client.trigger_dag(dag_id="test_start_date_scheduling", execution_date=EXECDATE)
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # run id
            run_id = "my_run_id"
            client.trigger_dag(dag_id="test_start_date_scheduling", run_id=run_id)
            mock.assert_called_once_with(run_id=run_id,
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # test conf
            conf = '{"name": "John"}'
            client.trigger_dag(dag_id="test_start_date_scheduling", conf=conf)
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=json.loads(conf),
                                         external_trigger=True)
            mock.reset_mock()

            # this is a unit test only, cannot verify existing dag run
