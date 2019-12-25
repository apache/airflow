# -*- coding: utf-8 -*-
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

import inspect
import os
import shutil
import textwrap
import unittest
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile, mkdtemp
from unittest.mock import ANY, patch

import airflow.example_dags
from airflow import models
from airflow.configuration import conf
from airflow.models import DagBag, DagModel, TaskInstance as TI
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.utils.db import create_session
from airflow.utils.state import State
from tests.models import DEFAULT_DATE, TEST_DAGS_FOLDER
from tests.test_utils.config import conf_vars


class TestDagBag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.empty_dir = mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.empty_dir)

    def test_get_existing_dag(self):
        """
        Test that we're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=True)

        some_expected_dag_ids = ["example_bash_operator",
                                 "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            self.assertIsNotNone(dag)
            self.assertEqual(dag_id, dag.dag_id)

        self.assertGreaterEqual(dagbag.size(), 7)

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        non_existing_dag_id = "non_existing_dag_id"
        self.assertIsNone(dagbag.get_dag(non_existing_dag_id))

    def test_dont_load_example(self):
        """
        test that the example are not loaded
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        self.assertEqual(dagbag.size(), 0)

    def test_safe_mode_heuristic_match(self):
        """With safe mode enabled, a file matching the discovery heuristics
        should be discovered.
        """
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py") as f:
            f.write(b"# airflow")
            f.write(b"# DAG")
            f.flush()

            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=True)

            self.assertEqual(len(dagbag.dagbag_stats), 1)
            self.assertEqual(
                dagbag.dagbag_stats[0].file,
                "/{}".format(os.path.basename(f.name)))

    def test_safe_mode_heuristic_mismatch(self):
        """With safe mode enabled, a file not matching the discovery heuristics
        should not be discovered.
        """
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py"):
            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=True)
            self.assertEqual(len(dagbag.dagbag_stats), 0)

    def test_safe_mode_disabled(self):
        """With safe mode disabled, an empty python file should be discovered.
        """
        with NamedTemporaryFile(dir=self.empty_dir, suffix=".py") as f:
            with conf_vars({('core', 'dags_folder'): self.empty_dir}):
                dagbag = models.DagBag(include_examples=False, safe_mode=False)
            self.assertEqual(len(dagbag.dagbag_stats), 1)
            self.assertEqual(
                dagbag.dagbag_stats[0].file,
                "/{}".format(os.path.basename(f.name)))

    def test_process_file_that_contains_multi_bytes_char(self):
        """
        test that we're able to parse file that contains multi-byte char
        """
        f = NamedTemporaryFile()
        f.write('\u3042'.encode())  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        self.assertEqual([], dagbag.process_file(f.name))

    def test_zip_skip_log(self):
        """
        test the loading of a DAG from within a zip file that skips another file because
        it doesn't have "airflow" and "DAG"
        """
        from unittest.mock import Mock
        with patch('airflow.models.DagBag.log') as log_mock:
            log_mock.info = Mock()
            test_zip_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
            dagbag = models.DagBag(dag_folder=test_zip_path, include_examples=False)

            self.assertTrue(dagbag.has_logged)
            log_mock.info.assert_any_call("File %s assumed to contain no DAGs. Skipping.",
                                          test_zip_path)

    def test_zip(self):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))
        self.assertTrue(dagbag.get_dag("test_zip_dag"))

    def test_process_file_cron_validity_check(self):
        """
        test if an invalid cron expression
        as schedule interval can be identified
        """
        invalid_dag_files = ["test_invalid_cron.py", "test_zip_invalid_cron.zip"]
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        self.assertEqual(len(dagbag.import_errors), 0)
        for file in invalid_dag_files:
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, file))
        self.assertEqual(len(dagbag.import_errors), len(invalid_dag_files))

    @patch.object(DagModel, 'get_current')
    def test_get_dag_without_refresh(self, mock_dagmodel):
        """
        Test that, once a DAG is loaded, it doesn't get refreshed again if it
        hasn't been expired.
        """
        dag_id = 'example_bash_operator'

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = None
        mock_dagmodel.return_value.fileloc = 'foo'

        class _TestDagBag(models.DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if os.path.basename(filepath) == 'example_bash_operator.py':
                    _TestDagBag.process_file_calls += 1
                super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(include_examples=True)
        dagbag.process_file_calls

        # Should not call process_file again, since it's already loaded during init.
        self.assertEqual(1, dagbag.process_file_calls)
        self.assertIsNotNone(dagbag.get_dag(dag_id))
        self.assertEqual(1, dagbag.process_file_calls)

    def test_get_dag_fileloc(self):
        """
        Test that fileloc is correctly set when we load example DAGs,
        specifically SubDAGs and packaged DAGs.
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=True)
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))

        expected = {
            'example_bash_operator': 'airflow/example_dags/example_bash_operator.py',
            'example_subdag_operator': 'airflow/example_dags/example_subdag_operator.py',
            'example_subdag_operator.section-1': 'airflow/example_dags/subdags/subdag.py',
            'test_zip_dag': 'dags/test_zip.zip/test_zip.py'
        }

        for dag_id, path in expected.items():
            dag = dagbag.get_dag(dag_id)
            self.assertTrue(dag.fileloc.endswith(path))

    @patch.object(DagModel, "get_current")
    def test_refresh_py_dag(self, mock_dagmodel):
        """
        Test that we can refresh an ordinary .py DAG
        """
        example_dags_folder = airflow.example_dags.__path__[0]

        dag_id = "example_bash_operator"
        fileloc = os.path.realpath(
            os.path.join(example_dags_folder, "example_bash_operator.py")
        )

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(
            tzinfo=timezone.utc
        )
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath == fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=self.empty_dir, include_examples=True)

        self.assertEqual(1, dagbag.process_file_calls)
        dag = dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        self.assertEqual(dag_id, dag.dag_id)
        self.assertEqual(2, dagbag.process_file_calls)

    @patch.object(DagModel, "get_current")
    def test_refresh_packaged_dag(self, mock_dagmodel):
        """
        Test that we can refresh a packaged DAG
        """
        dag_id = "test_zip_dag"
        fileloc = os.path.realpath(
            os.path.join(TEST_DAGS_FOLDER, "test_zip.zip/test_zip.py")
        )

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = datetime.max.replace(
            tzinfo=timezone.utc
        )
        mock_dagmodel.return_value.fileloc = fileloc

        class _TestDagBag(DagBag):
            process_file_calls = 0

            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if filepath in fileloc:
                    _TestDagBag.process_file_calls += 1
                return super().process_file(filepath, only_if_updated, safe_mode)

        dagbag = _TestDagBag(dag_folder=os.path.realpath(TEST_DAGS_FOLDER), include_examples=False)

        self.assertEqual(1, dagbag.process_file_calls)
        dag = dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        self.assertEqual(dag_id, dag.dag_id)
        self.assertEqual(2, dagbag.process_file_calls)

    def process_dag(self, create_dag):
        """
        Helper method to process a file generated from the input create_dag function.
        """
        # write source to file
        source = textwrap.dedent(''.join(
            inspect.getsource(create_dag).splitlines(True)[1:-1]))
        f = NamedTemporaryFile()
        f.write(source.encode('utf8'))
        f.flush()

        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)
        found_dags = dagbag.process_file(f.name)
        return dagbag, found_dags, f.name

    def validate_dags(self, expected_parent_dag, actual_found_dags, actual_dagbag,
                      should_be_found=True):
        expected_dag_ids = list(map(lambda dag: dag.dag_id, expected_parent_dag.subdags))
        expected_dag_ids.append(expected_parent_dag.dag_id)

        actual_found_dag_ids = list(map(lambda dag: dag.dag_id, actual_found_dags))

        for dag_id in expected_dag_ids:
            actual_dagbag.log.info('validating %s' % dag_id)
            self.assertEqual(
                dag_id in actual_found_dag_ids, should_be_found,
                'dag "%s" should %shave been found after processing dag "%s"' %
                (dag_id, '' if should_be_found else 'not ', expected_parent_dag.dag_id)
            )
            self.assertEqual(
                dag_id in actual_dagbag.dags, should_be_found,
                'dag "%s" should %sbe in dagbag.dags after processing dag "%s"' %
                (dag_id, '' if should_be_found else 'not ', expected_parent_dag.dag_id)
            )

    def test_load_subdags(self):
        # Define Dag to load
        def standard_subdag():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime  # pylint: disable=redefined-outer-name,reimported
            dag_name = 'master'
            default_args = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                dag_name,
                default_args=default_args)

            # master:
            #     A -> opSubDag_0
            #          master.opsubdag_0:
            #              -> subdag_0.task
            #     A -> opSubDag_1
            #          master.opsubdag_1:
            #              -> subdag_1.task

            with dag:
                def subdag_0():
                    subdag_0 = DAG('master.op_subdag_0', default_args=default_args)
                    DummyOperator(task_id='subdag_0.task', dag=subdag_0)
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('master.op_subdag_1', default_args=default_args)
                    DummyOperator(task_id='subdag_1.task', dag=subdag_1)
                    return subdag_1

                op_subdag_0 = SubDagOperator(
                    task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(
                    task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)
            return dag

        test_dag = standard_subdag()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(test_dag.subdags), 2)

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(standard_subdag)

        # Validate correctness
        # all dags from test_dag should be listed
        self.validate_dags(test_dag, found_dags, dagbag)

        # Define Dag to load
        def nested_subdags():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime  # pylint: disable=redefined-outer-name,reimported
            dag_name = 'master'
            default_args = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                dag_name,
                default_args=default_args)

            # master:
            #     A -> op_subdag_0
            #          master.op_subdag_0:
            #              -> opSubDag_A
            #                 master.op_subdag_0.opSubdag_A:
            #                     -> subdag_a.task
            #              -> opSubdag_B
            #                 master.op_subdag_0.opSubdag_B:
            #                     -> subdag_b.task
            #     A -> op_subdag_1
            #          master.op_subdag_1:
            #              -> opSubdag_C
            #                 master.op_subdag_1.opSubdag_C:
            #                     -> subdag_c.task
            #              -> opSubDag_D
            #                 master.op_subdag_1.opSubdag_D:
            #                     -> subdag_d.task

            with dag:
                def subdag_a():
                    subdag_a = DAG(
                        'master.op_subdag_0.opSubdag_A', default_args=default_args)
                    DummyOperator(task_id='subdag_a.task', dag=subdag_a)
                    return subdag_a

                def subdag_b():
                    subdag_b = DAG(
                        'master.op_subdag_0.opSubdag_B', default_args=default_args)
                    DummyOperator(task_id='subdag_b.task', dag=subdag_b)
                    return subdag_b

                def subdag_c():
                    subdag_c = DAG(
                        'master.op_subdag_1.opSubdag_C', default_args=default_args)
                    DummyOperator(task_id='subdag_c.task', dag=subdag_c)
                    return subdag_c

                def subdag_d():
                    subdag_d = DAG(
                        'master.op_subdag_1.opSubdag_D', default_args=default_args)
                    DummyOperator(task_id='subdag_d.task', dag=subdag_d)
                    return subdag_d

                def subdag_0():
                    subdag_0 = DAG('master.op_subdag_0', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_a())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_b())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('master.op_subdag_1', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_c())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_d())
                    return subdag_1

                op_subdag_0 = SubDagOperator(
                    task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(
                    task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)

            return dag

        test_dag = nested_subdags()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(test_dag.subdags), 6)

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(nested_subdags)

        # Validate correctness
        # all dags from test_dag should be listed
        self.validate_dags(test_dag, found_dags, dagbag)

    def test_skip_cycle_dags(self):
        """
        Don't crash when loading an invalid (contains a cycle) DAG file.
        Don't load the dag into the DagBag either
        """

        # Define Dag to load
        def basic_cycle():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            import datetime  # pylint: disable=redefined-outer-name,reimported
            dag_name = 'cycle_dag'
            default_args = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                dag_name,
                default_args=default_args)

            # A -> A
            with dag:
                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_a)

            return dag

        test_dag = basic_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(test_dag.subdags), 0)

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(basic_cycle)

        # #Validate correctness
        # None of the dags should be found
        self.validate_dags(test_dag, found_dags, dagbag, should_be_found=False)
        self.assertIn(file_path, dagbag.import_errors)

        # Define Dag to load
        def nested_subdag_cycle():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime  # pylint: disable=redefined-outer-name,reimported
            dag_name = 'nested_cycle'
            default_args = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                dag_name,
                default_args=default_args)

            # cycle:
            #     A -> op_subdag_0
            #          cycle.op_subdag_0:
            #              -> opSubDag_A
            #                 cycle.op_subdag_0.opSubdag_A:
            #                     -> subdag_a.task
            #              -> opSubdag_B
            #                 cycle.op_subdag_0.opSubdag_B:
            #                     -> subdag_b.task
            #     A -> op_subdag_1
            #          cycle.op_subdag_1:
            #              -> opSubdag_C
            #                 cycle.op_subdag_1.opSubdag_C:
            #                     -> subdag_c.task -> subdag_c.task  >Invalid Loop<
            #              -> opSubDag_D
            #                 cycle.op_subdag_1.opSubdag_D:
            #                     -> subdag_d.task

            with dag:
                def subdag_a():
                    subdag_a = DAG(
                        'nested_cycle.op_subdag_0.opSubdag_A', default_args=default_args)
                    DummyOperator(task_id='subdag_a.task', dag=subdag_a)
                    return subdag_a

                def subdag_b():
                    subdag_b = DAG(
                        'nested_cycle.op_subdag_0.opSubdag_B', default_args=default_args)
                    DummyOperator(task_id='subdag_b.task', dag=subdag_b)
                    return subdag_b

                def subdag_c():
                    subdag_c = DAG(
                        'nested_cycle.op_subdag_1.opSubdag_C', default_args=default_args)
                    op_subdag_c_task = DummyOperator(
                        task_id='subdag_c.task', dag=subdag_c)
                    # introduce a loop in opSubdag_C
                    op_subdag_c_task.set_downstream(op_subdag_c_task)
                    return subdag_c

                def subdag_d():
                    subdag_d = DAG(
                        'nested_cycle.op_subdag_1.opSubdag_D', default_args=default_args)
                    DummyOperator(task_id='subdag_d.task', dag=subdag_d)
                    return subdag_d

                def subdag_0():
                    subdag_0 = DAG('nested_cycle.op_subdag_0', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_a())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_b())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('nested_cycle.op_subdag_1', default_args=default_args)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_c())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_d())
                    return subdag_1

                op_subdag_0 = SubDagOperator(
                    task_id='op_subdag_0', dag=dag, subdag=subdag_0())
                op_subdag_1 = SubDagOperator(
                    task_id='op_subdag_1', dag=dag, subdag=subdag_1())

                op_a = DummyOperator(task_id='A')
                op_a.set_downstream(op_subdag_0)
                op_a.set_downstream(op_subdag_1)

            return dag

        test_dag = nested_subdag_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(test_dag.subdags), 6)

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(nested_subdag_cycle)

        # Validate correctness
        # None of the dags should be found
        self.validate_dags(test_dag, found_dags, dagbag, should_be_found=False)
        self.assertIn(file_path, dagbag.import_errors)

    def test_process_file_with_none(self):
        """
        test that process_file can handle Nones
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=False)

        self.assertEqual([], dagbag.process_file(None))

    @patch.object(TI, 'handle_failure')
    def test_kill_zombies(self, mock_ti_handle_failure):
        """
        Test that kill zombies call TIs failure handler with proper context
        """
        dagbag = models.DagBag(dag_folder=self.empty_dir, include_examples=True)
        with create_session() as session:
            session.query(TI).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TI(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            zombies = [SimpleTaskInstance(ti)]
            dagbag.kill_zombies(zombies)
            mock_ti_handle_failure.assert_called_once_with(
                ANY,
                conf.getboolean('core', 'unit_test_mode'),
                ANY
            )

    def test_deactivate_unknown_dags(self):
        """
        Test that dag_ids not passed into deactivate_unknown_dags
        are deactivated when function is invoked
        """
        dagbag = DagBag(include_examples=True)
        dag_id = "test_deactivate_unknown_dags"
        expected_active_dags = dagbag.dags.keys()

        model_before = DagModel(dag_id=dag_id, is_active=True)
        with create_session() as session:
            session.merge(model_before)

        models.DAG.deactivate_unknown_dags(expected_active_dags)

        after_model = DagModel.get_dagmodel(dag_id)
        self.assertTrue(model_before.is_active)
        self.assertFalse(after_model.is_active)

        # clean up
        with create_session() as session:
            session.query(DagModel).filter(DagModel.dag_id == 'test_deactivate_unknown_dags').delete()
