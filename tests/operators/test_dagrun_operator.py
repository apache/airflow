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

import pathlib
import tempfile
from datetime import datetime
from unittest import TestCase

from airflow.models import DAG, DagModel, DagRun, Log, TaskInstance
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.session import create_session

DEFAULT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
TEST_DAG_ID = "testdag"
TRIGGERED_DAG_ID = "triggerdag"
DAG_SCRIPT = (
    "from datetime import datetime\n\n"
    "from airflow.models import DAG\n"
    "from airflow.operators.dummy_operator import DummyOperator\n\n"
    "dag = DAG(\n"
    'dag_id="{dag_id}", \n'
    'default_args={{"start_date": datetime(2019, 1, 1)}}, \n'
    "schedule_interval=None,\n"
    ")\n"
    'task = DummyOperator(task_id="test", dag=dag)'
).format(dag_id=TRIGGERED_DAG_ID)


class TestDagRunOperator(TestCase):
    def setUp(self):
        # Airflow relies on reading the DAG from disk when triggering it.
        # Therefore write a temp file holding the DAG to trigger.
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            self._tmpfile = f.name
            f.write(DAG_SCRIPT)
            f.flush()

        with create_session() as session:
            session.add(DagModel(dag_id=TRIGGERED_DAG_ID, fileloc=self._tmpfile))
            session.commit()

        self.dag = DAG(TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})

    def tearDown(self):
        """Cleanup state after testing in DB."""
        with create_session() as session:
            session.query(Log).filter(Log.dag_id == TEST_DAG_ID).delete(synchronize_session=False)
            for dbmodel in [DagModel, DagRun, TaskInstance]:
                session.query(dbmodel).filter(dbmodel.dag_id == TRIGGERED_DAG_ID).delete(
                    synchronize_session=False
                )

        pathlib.Path(self._tmpfile).unlink()

    def test_trigger_dagrun(self):
        """Test TriggerDagRunOperator."""
        task = TriggerDagRunOperator(task_id="test_task", trigger_dag_id=TRIGGERED_DAG_ID, dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            self.assertEqual(len(dagruns), 1)
            self.assertTrue(dagruns[0].external_trigger)

    def test_trigger_dagrun_with_execution_date(self):
        """Test TriggerDagRunOperator with custom execution_date."""
        utc_now = timezone.utcnow()
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=utc_now,
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            self.assertEqual(len(dagruns), 1)
            self.assertTrue(dagruns[0].external_trigger)
            self.assertEqual(dagruns[0].execution_date, utc_now)

    def test_trigger_dagrun_with_templated_execution_date(self):
        """Test TriggerDagRunOperator with templated execution_date."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date="{{ execution_date }}",
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            self.assertEqual(len(dagruns), 1)
            self.assertTrue(dagruns[0].external_trigger)
            self.assertEqual(dagruns[0].execution_date, DEFAULT_DATE)

    def test_trigger_dagrun_operator_conf(self):
        """Test passing conf to the triggered DagRun."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            conf={"foo": "bar"},
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            self.assertEqual(len(dagruns), 1)
            self.assertTrue(dagruns[0].conf, {"foo": "bar"})

    def test_trigger_dagrun_operator_templated_conf(self):
        """Test passing a templated conf to the triggered DagRun."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            conf={"foo": "{{ dag.dag_id }}"},
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            self.assertEqual(len(dagruns), 1)
            self.assertTrue(dagruns[0].conf, {"foo": TEST_DAG_ID})
