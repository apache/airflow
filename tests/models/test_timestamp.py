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

import pendulum
import pytest
from freezegun import freeze_time

from airflow.models import DAG, Log, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State


@pytest.yield_fixture(name="clear_db_fixture")
@provide_session
def clear_db(session=None):
    session.query(Log).delete()
    session.query(TaskInstance).delete()
    yield session
    session.query(Log).delete()
    session.query(TaskInstance).delete()
    session.commit()


def add_log(execdate, session, timezone_override=None):
    dag = DAG(dag_id='logging',
              default_args={'start_date': execdate})
    task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')
    task_instance = TaskInstance(task=task, execution_date=execdate, state='success')
    session.merge(task_instance)
    log = Log(State.RUNNING, task_instance)
    if timezone_override:
        log.dttm = log.dttm.astimezone(timezone_override)
    session.add(log)
    session.commit()
    return log


def test_timestamp_behaviour(clear_db_fixture):
    for session in clear_db_fixture:
        execdate = timezone.utcnow()
        with freeze_time(execdate):
            current_time = timezone.utcnow()
            old_log = add_log(execdate, session)
            session.expunge(old_log)
            log_time = session.query(Log).one().dttm
            assert log_time == current_time
            assert log_time.tzinfo.name == 'UTC'


def test_timestamp_behaviour_with_timezone(clear_db_fixture):
    for session in clear_db_fixture:
        execdate = timezone.utcnow()
        with freeze_time(execdate):
            current_time = timezone.utcnow()
            old_log = add_log(execdate, session, timezone_override=pendulum.timezone('Europe/Warsaw'))
            session.expunge(old_log)
            # No matter what timezone we set - we should always get back UTC
            log_time = session.query(Log).one().dttm
            assert log_time == current_time
            assert old_log.dttm.tzinfo.name != 'UTC'
            assert log_time.tzinfo.name == 'UTC'
            assert old_log.dttm.astimezone(pendulum.timezone('UTC')) == log_time
