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

from __future__ import annotations

from unittest.mock import ANY, Mock, patch

from airflow.providers.sas.operators.sas_studioflow import (
    SASStudioFlowOperator,
    _create_or_connect_to_session,
    _dump_logs,
    _generate_flow_code,
    _run_job_and_wait,
)


class TestSasStudioFlowOperator:
    """
    Test class for SASStudioFlow
    """

    @patch("airflow.providers.sas.operators.sas_studioflow._dump_logs")
    @patch("airflow.providers.sas.operators.sas_studioflow._run_job_and_wait")
    @patch("airflow.providers.sas.operators.sas_studioflow._generate_flow_code")
    @patch("airflow.providers.sas.operators.sas_studioflow.create_session_for_connection")
    def test_execute_sas_studio_flow_operator_basic(
        self, session_mock, flow_mock, runjob_mock, dumplogs_mock
    ):
        flow_mock.return_value = {"code": "test code"}
        runjob_mock.return_value = {
            "id": "jobid1",
            "state": "completed",
            "links": [{"rel": "log", "uri": "log/uri"}],
        }

        environment_vars = {"env1": "val1", "env2": "val2"}
        operator = SASStudioFlowOperator(
            task_id="demo_studio_flow_1.flw",
            flow_path_type="content",
            flow_path="/Public/Airflow/demo_studio_flow_1.flw",
            flow_exec_log=True,
            airflow_connection_name="SAS",
            compute_context="SAS Studio compute context",
            flow_codegen_init_code=False,
            flow_codegen_wrap_code=False,
            env_vars=environment_vars,
        )

        operator.execute(context={})

        session_mock.assert_called_with("SAS")
        flow_mock.assert_called_with(
            ANY, "content", "/Public/Airflow/demo_studio_flow_1.flw", False, False, None, ANY
        )

    def test_execute_sas_studio_flow_create_or_connect(self):
        session = Mock()
        req_ret = Mock()
        session.get.return_value = req_ret
        req_ret.json.return_value = {"count": 1, "items": ["dummy"]}
        req_ret.status_code = 200
        r = _create_or_connect_to_session(session, "context", "name")
        assert r == "dummy"

    def test_execute_sas_studio_flow_create_or_connect_new(self):
        session = Mock()
        req_ret1 = Mock()
        req_ret2 = Mock()
        session.get.side_effect = [req_ret1, req_ret2]
        session.headers = {}
        post_ret = Mock()
        session.post.return_value = post_ret
        post_ret.status_code = 201
        post_ret.json.return_value = {"a": "b"}
        req_ret1.json.return_value = {"count": 0}
        req_ret1.status_code = 200
        req_ret2.json.return_value = {"count": 1, "items": [{"id": "10"}]}
        req_ret2.status_code = 200
        r = _create_or_connect_to_session(session, "context", "name")
        assert r == {"a": "b"}

    def test_execute_sas_studio_flow_operator_gen_code(self):
        session = Mock()
        req_ret = Mock()
        session.post.return_value = req_ret
        req_ret.json.return_value = {"code": "code val"}
        req_ret.status_code = 200

        r = _generate_flow_code(session, "content", "/path", True, True, None)
        session.post.assert_called_with(
            "/studioDevelopment/code",
            json={
                "reference": {
                    "mediaType": "application/vnd.sas.dataflow",
                    "type": "content",
                    "path": "/path",
                },
                "initCode": True,
                "wrapperCode": True,
            },
        )
        assert r == {"code": "code val"}

    @patch("airflow.providers.sas.operators.sas_studioflow._create_or_connect_to_session")
    def test_execute_sas_studio_flow_operator_gen_code_compute(self, c_mock):
        session = Mock()
        req_ret = Mock()
        session.post.return_value = req_ret
        req_ret.json.return_value = {"code": "code val"}
        req_ret.status_code = 200
        c_mock.return_value = {"id": "abc"}

        r = _generate_flow_code(session, "compute", "/path", True, True, None)

        c_mock.assert_called_with(ANY, "SAS Studio compute context", "Airflow-Session")

        session.post.assert_called_with(
            "/studioDevelopment/code?sessionId=abc",
            json={
                "reference": {
                    "mediaType": "application/vnd.sas.dataflow",
                    "type": "compute",
                    "path": "/path",
                },
                "initCode": True,
                "wrapperCode": True,
            },
        )
        assert r == {"code": "code val"}

    def test_execute_sas_studio_flow_run_job(self):
        session_mock = Mock()
        session_mock.post.return_value.status_code = 201
        session_mock.post.return_value.json.return_value = {"id": "1", "state": "completed"}
        req = {"a": "b"}
        r = _run_job_and_wait(session_mock, req, 1)
        session_mock.post.assert_called_with("/jobExecution/jobs", json={"a": "b"})
        assert r == {"id": "1", "state": "completed"}

    def test_execute_sas_studio_flow_get_logs(self):
        session_mock = Mock()
        session_mock.get.return_value.status_code = 200
        session_mock.get.return_value.text = """
            {"items": [{"type":"INFO", "line":"line value"}]}
            """
        req = {"links": [{"rel": "log", "uri": "log/uri"}]}
        _dump_logs(session_mock, req)
        session_mock.get.assert_called_with("log/uri/content")
