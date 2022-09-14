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
"""Unit tests for RenderedTaskInstanceFields."""
from __future__ import annotations

import os
from collections import Counter
from datetime import date, timedelta
from unittest import mock

import pytest
from sqlalchemy.orm.session import make_transient

from airflow import settings
from airflow.configuration import TEST_DAGS_FOLDER
from airflow.models import Variable
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.operators.bash import BashOperator
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields

DEFAULT_DATE = datetime(2018, 1, 1)
EXECUTION_DATE = datetime(2019, 1, 1)


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TestRenderedTaskInstanceFields:
    """Unit tests for RenderedTaskInstanceFields."""

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()
        clear_rendered_ti_fields()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    @pytest.mark.parametrize(
        "templated_field, expected_rendered_field",
        [
            (None, None),
            ([], []),
            ({}, {}),
            ("test-string", "test-string"),
            ({"foo": "bar"}, {"foo": "bar"}),
            ("{{ task.task_id }}", "test"),
            (date(2018, 12, 6), "2018-12-06"),
            (datetime(2018, 12, 6, 10, 55), "2018-12-06 10:55:00+00:00"),
            (
                ClassWithCustomAttributes(
                    att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                ),
                "ClassWithCustomAttributes({'att1': 'test', 'att2': '{{ task.task_id }}', "
                "'template_fields': ['att1']})",
            ),
            (
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ task.task_id }}", att4="{{ task.task_id }}", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
                "ClassWithCustomAttributes({'nested1': ClassWithCustomAttributes("
                "{'att1': 'test', 'att2': '{{ task.task_id }}', 'template_fields': ['att1']}), "
                "'nested2': ClassWithCustomAttributes("
                "{'att3': '{{ task.task_id }}', 'att4': '{{ task.task_id }}', 'template_fields': ['att3']}), "
                "'template_fields': ['nested1']})",
            ),
        ],
    )
    def test_get_templated_fields(self, templated_field, expected_rendered_field, dag_maker):
        """
        Test that template_fields are rendered correctly, stored in the Database,
        and are correctly fetched using RTIF.get_templated_fields
        """
        with dag_maker("test_serialized_rendered_fields"):
            task = BashOperator(task_id="test", bash_command=templated_field)
            task_2 = BashOperator(task_id="test2", bash_command=templated_field)
        dr = dag_maker.create_dagrun()

        session = dag_maker.session

        ti, ti2 = dr.task_instances
        ti.task = task
        ti2.task = task_2
        rtif = RTIF(ti=ti)

        assert ti.dag_id == rtif.dag_id
        assert ti.task_id == rtif.task_id
        assert ti.run_id == rtif.run_id
        assert expected_rendered_field == rtif.rendered_fields.get("bash_command")

        session.add(rtif)
        session.flush()

        assert {"bash_command": expected_rendered_field, "env": None} == RTIF.get_templated_fields(
            ti=ti, session=session
        )
        # Test the else part of get_templated_fields
        # i.e. for the TIs that are not stored in RTIF table
        # Fetching them will return None
        assert RTIF.get_templated_fields(ti=ti2) is None

    @pytest.mark.parametrize(
        "rtif_num, num_to_keep, remaining_rtifs, expected_query_count",
        [
            (0, 1, 0, 1),
            (1, 1, 1, 1),
            (1, 0, 1, 0),
            (3, 1, 1, 1),
            (4, 2, 2, 1),
            (5, 2, 2, 1),
        ],
    )
    def test_delete_old_records(
        self, rtif_num, num_to_keep, remaining_rtifs, expected_query_count, dag_maker
    ):
        """
        Test that old records are deleted from rendered_task_instance_fields table
        for a given task_id and dag_id.
        """
        session = settings.Session()
        with dag_maker("test_delete_old_records") as dag:
            task = BashOperator(task_id="test", bash_command="echo {{ ds }}")
        rtif_list = []
        for num in range(rtif_num):
            dr = dag_maker.create_dagrun(run_id=str(num), execution_date=dag.start_date + timedelta(days=num))
            ti = dr.task_instances[0]
            ti.task = task
            rtif_list.append(RTIF(ti))

        session.add_all(rtif_list)
        session.flush()

        result = session.query(RTIF).filter(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id).all()

        for rtif in rtif_list:
            assert rtif in result

        assert rtif_num == len(result)

        # Verify old records are deleted and only 'num_to_keep' records are kept
        # For other DBs,an extra query is fired in RenderedTaskInstanceFields.delete_old_records
        expected_query_count_based_on_db = (
            expected_query_count + 1
            if session.bind.dialect.name == "mssql" and expected_query_count != 0
            else expected_query_count
        )

        with assert_queries_count(expected_query_count_based_on_db):
            RTIF.delete_old_records(task_id=task.task_id, dag_id=task.dag_id, num_to_keep=num_to_keep)
        result = session.query(RTIF).filter(RTIF.dag_id == dag.dag_id, RTIF.task_id == task.task_id).all()
        assert remaining_rtifs == len(result)

    @pytest.mark.parametrize(
        "num_runs, num_to_keep, remaining_rtifs, expected_query_count",
        [
            (3, 1, 1, 1),
            (4, 2, 2, 1),
            (5, 2, 2, 1),
        ],
    )
    def test_delete_old_records_mapped(
        self, num_runs, num_to_keep, remaining_rtifs, expected_query_count, dag_maker, session
    ):
        """
        Test that old records are deleted from rendered_task_instance_fields table
        for a given task_id and dag_id with mapped tasks.
        """
        with dag_maker("test_delete_old_records", session=session) as dag:
            mapped = BashOperator.partial(task_id="mapped").expand(bash_command=['a', 'b'])
        for num in range(num_runs):
            dr = dag_maker.create_dagrun(
                run_id=f'run_{num}', execution_date=dag.start_date + timedelta(days=num)
            )

            mapped.expand_mapped_task(dr.run_id, session=dag_maker.session)
            session.refresh(dr)
            for ti in dr.task_instances:
                ti.task = dag.get_task(ti.task_id)
                session.add(RTIF(ti))
        session.flush()

        result = session.query(RTIF).filter(RTIF.dag_id == dag.dag_id).all()
        assert len(result) == num_runs * 2

        # Verify old records are deleted and only 'num_to_keep' records are kept
        # For other DBs,an extra query is fired in RenderedTaskInstanceFields.delete_old_records
        expected_query_count_based_on_db = (
            expected_query_count + 1
            if session.bind.dialect.name == "mssql" and expected_query_count != 0
            else expected_query_count
        )

        with assert_queries_count(expected_query_count_based_on_db):
            RTIF.delete_old_records(
                task_id=mapped.task_id, dag_id=dr.dag_id, num_to_keep=num_to_keep, session=session
            )
        result = session.query(RTIF).filter_by(dag_id=dag.dag_id, task_id=mapped.task_id).all()
        rtif_num_runs = Counter(rtif.run_id for rtif in result)
        assert len(rtif_num_runs) == remaining_rtifs
        # Check that we have _all_ the data for each row
        assert len(result) == remaining_rtifs * 2

    def test_write(self, dag_maker):
        """
        Test records can be written and overwritten
        """
        Variable.set(key="test_key", value="test_val")

        session = settings.Session()
        result = session.query(RTIF).all()
        assert [] == result

        with dag_maker("test_write"):
            task = BashOperator(task_id="test", bash_command="echo {{ var.value.test_key }}")

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task

        rtif = RTIF(ti)
        rtif.write()
        result = (
            session.query(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields)
            .filter(
                RTIF.dag_id == rtif.dag_id,
                RTIF.task_id == rtif.task_id,
                RTIF.run_id == rtif.run_id,
            )
            .first()
        )
        assert ('test_write', 'test', {'bash_command': 'echo test_val', 'env': None}) == result

        # Test that overwrite saves new values to the DB
        Variable.delete("test_key")
        Variable.set(key="test_key", value="test_val_updated")
        self.clean_db()
        with dag_maker("test_write"):
            updated_task = BashOperator(task_id="test", bash_command="echo {{ var.value.test_key }}")
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = updated_task
        rtif_updated = RTIF(ti)
        rtif_updated.write()

        result_updated = (
            session.query(RTIF.dag_id, RTIF.task_id, RTIF.rendered_fields)
            .filter(
                RTIF.dag_id == rtif_updated.dag_id,
                RTIF.task_id == rtif_updated.task_id,
                RTIF.run_id == rtif_updated.run_id,
            )
            .first()
        )
        assert (
            'test_write',
            'test',
            {'bash_command': 'echo test_val_updated', 'env': None},
        ) == result_updated

    @mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
    @mock.patch('airflow.utils.log.secrets_masker.redact', autospec=True, side_effect=lambda d, _=None: d)
    def test_get_k8s_pod_yaml(self, redact, dag_maker):
        """
        Test that k8s_pod_yaml is rendered correctly, stored in the Database,
        and are correctly fetched using RTIF.get_k8s_pod_yaml
        """
        with dag_maker("test_get_k8s_pod_yaml") as dag:
            task = BashOperator(task_id="test", bash_command="echo hi")
        dr = dag_maker.create_dagrun()
        dag.fileloc = TEST_DAGS_FOLDER + '/test_get_k8s_pod_yaml.py'

        ti = dr.task_instances[0]
        ti.task = task

        render_k8s_pod_yaml = mock.patch.object(
            ti, 'render_k8s_pod_yaml', return_value={"I'm a": "pod"}
        ).start()

        rtif = RTIF(ti=ti)

        assert ti.dag_id == rtif.dag_id
        assert ti.task_id == rtif.task_id
        assert ti.run_id == rtif.run_id

        expected_pod_yaml = {"I'm a": "pod"}

        assert rtif.k8s_pod_yaml == render_k8s_pod_yaml.return_value
        # K8s pod spec dict was passed to redact
        redact.assert_any_call(rtif.k8s_pod_yaml)

        with create_session() as session:
            session.add(rtif)
            session.flush()

            assert expected_pod_yaml == RTIF.get_k8s_pod_yaml(ti=ti, session=session)
            make_transient(ti)
            # "Delete" it from the DB
            session.rollback()

            # Test the else part of get_k8s_pod_yaml
            # i.e. for the TIs that are not stored in RTIF table
            # Fetching them will return None
            assert RTIF.get_k8s_pod_yaml(ti=ti, session=session) is None

    @mock.patch.dict(os.environ, {"AIRFLOW_VAR_API_KEY": "secret"})
    @mock.patch('airflow.utils.log.secrets_masker.redact', autospec=True)
    def test_redact(self, redact, dag_maker):
        with dag_maker("test_ritf_redact"):
            task = BashOperator(
                task_id="test",
                bash_command="echo {{ var.value.api_key }}",
                env={'foo': 'secret', 'other_api_key': 'masked based on key name'},
            )
        dr = dag_maker.create_dagrun()
        redact.side_effect = [
            'val 1',
            'val 2',
        ]

        ti = dr.task_instances[0]
        ti.task = task
        rtif = RTIF(ti=ti)
        assert rtif.rendered_fields == {
            'bash_command': 'val 1',
            'env': 'val 2',
        }
