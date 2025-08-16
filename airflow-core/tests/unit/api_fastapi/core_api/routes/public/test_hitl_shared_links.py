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

import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY

import attrs
import pytest
from fastapi import status
from sqlalchemy import select

from airflow._shared.timezones.timezone import convert_to_utc
from airflow.api_fastapi.core_api.services.public.hitl_shared_links import (
    HITLSharedLinkConfig,
    HITLSharedLinkData,
)
from airflow.models.hitl import HITL_LINK_TYPE, HITLDetail
from airflow.models.taskinstance import TaskInstance as TI

from tests_common.pytest_plugin import time_machine
from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import CreateTaskInstance, TaskInstance

pytestmark = pytest.mark.db_test

TEST_SERVER_PREFIX = "http://testserver/api/v2"

DAG_ID = "test_hitl_shared_link_dag"
DAG_RUN_ID = "run_id"
TASK_ID = "sample_hitl_task"

CURRENT_TIME = convert_to_utc(datetime.datetime(2025, 8, 14, 0, 0, 0))
ONE_DAY_BEFORE_CURRENT = CURRENT_TIME - datetime.timedelta(days=1)
ONE_DAY_AFTER_CURRENT = CURRENT_TIME + datetime.timedelta(days=1)


@pytest.fixture
def enabled():
    with conf_vars({("api", "hitl_enable_shared_links"): True}):
        yield


@pytest.fixture
def sample_ti(create_task_instance: CreateTaskInstance, session: Session) -> TaskInstance:
    ti = create_task_instance(dag_id=DAG_ID, run_id=DAG_RUN_ID, task_id=TASK_ID, session=session)
    session.commit()
    return ti


@pytest.mark.parametrize("map_index", [None, -1])
class TestGenerateSharedLink:
    """Tests for /hitlSharedLinks/generate endpoints"""

    def gen_endpoint(self, map_index: int | None) -> str:
        base = f"/hitlSharedLinks/generate/{DAG_ID}/{DAG_RUN_ID}/{TASK_ID}"
        return base if map_index is None else f"{base}/{map_index}"

    @pytest.mark.parametrize(
        "link_type, chosen_options_in_payload, params_input_payload",
        (("redirect", None, None), ("respond", ["option 1"], None)),
    )
    @time_machine.travel(CURRENT_TIME, tick=False)
    def test_should_respond_201(
        self,
        test_client: TestClient,
        sample_ti: TaskInstance,
        link_type: HITL_LINK_TYPE,
        chosen_options_in_payload: list[str] | None,
        params_input_payload: dict[str, Any] | None,
        map_index: int | None,
    ) -> None:
        endpoint = self.gen_endpoint(map_index)
        secret_key = test_client.app.state.secret_key  # type: ignore[attr-defined]

        expected_data = HITLSharedLinkData(
            ti_id=sample_ti.id,
            dag_id=DAG_ID,
            dag_run_id=DAG_RUN_ID,
            task_id=TASK_ID,
            map_index=map_index,
            shared_link_config=HITLSharedLinkConfig(
                link_type=link_type,
                expires_at=ONE_DAY_AFTER_CURRENT.isoformat(),
                chosen_options=chosen_options_in_payload or [],
                params_input=params_input_payload or {},
            ),
        )

        resp = test_client.post(
            endpoint,
            json={
                "link_type": link_type,
                "expires_at": None,
                "chosen_options": chosen_options_in_payload,
                "params_input": params_input_payload,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        url = resp.json()["url"]
        token = url.split("/")[-1]
        assert url.startswith(f"{TEST_SERVER_PREFIX}/hitlSharedLinks/{link_type}/")
        assert attrs.asdict(HITLSharedLinkData.decode(secret_key=secret_key, token=token)) == expected_data

    @pytest.mark.parametrize(
        "invalid_payload",
        [
            {"link_type": "respond", "expires_at": None, "chosen_options": None, "params_input": None},
            {
                "link_type": "redirect",
                "expires_at": None,
                "chosen_options": ["option 1"],
                "params_input": None,
            },
            {
                "link_type": "redirect",
                "expires_at": None,
                "chosen_options": None,
                "params_input": {"test": "test"},
            },
            {
                "link_type": "redirect",
                "expires_at": ONE_DAY_BEFORE_CURRENT.isoformat(),
                "chosen_options": None,
                "params_input": None,
            },
        ],
    )
    def test_should_respond_400_with_invalid_payload(
        self,
        test_client: TestClient,
        invalid_payload: dict[str, Any],
        map_index: int | None,
    ) -> None:
        resp = test_client.post(self.gen_endpoint(map_index), json=invalid_payload)
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.usefixtures("sample_ti")
    def test_should_respond_401(self, unauthenticated_test_client: TestClient, map_index: int | None) -> None:
        resp = unauthenticated_test_client.post(
            self.gen_endpoint(map_index),
            json={"link_type": "redirect", "expires_at": None, "chosen_options": None, "params_input": None},
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED

    @pytest.mark.usefixtures("sample_ti")
    def test_should_respond_403(self, unauthorized_test_client: TestClient, map_index: int | None) -> None:
        resp = unauthorized_test_client.post(
            self.gen_endpoint(map_index),
            json={"link_type": "redirect", "expires_at": None, "chosen_options": None, "params_input": None},
        )
        assert resp.status_code == status.HTTP_403_FORBIDDEN

    def test_should_respond_404(self, test_client: TestClient, map_index: int | None) -> None:
        resp = test_client.post(
            self.gen_endpoint(map_index),
            json={"link_type": "redirect", "expires_at": None, "chosen_options": None, "params_input": None},
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_should_respond_422_with_invalid_payload(
        self, test_client: TestClient, map_index: int | None
    ) -> None:
        resp = test_client.post(
            self.gen_endpoint(map_index),
            json={
                "link_type": "no_such_type",
                "expires_at": None,
                "chosen_options": None,
                "params_input": None,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestHitlSharedLinkRedirect:
    @pytest.fixture
    def redirect_token(self, test_client: TestClient, sample_ti: TaskInstance) -> str:
        """Generate a redirect token"""
        resp = test_client.post(
            f"/hitlSharedLinks/generate/{DAG_ID}/{DAG_RUN_ID}/{TASK_ID}",
            json={
                "link_type": "redirect",
                "expires_at": None,
                "chosen_options": None,
                "params_input": None,
            },
        )
        url = resp.json()["url"]
        return url.split("/")[-1]

    @pytest.mark.usefixtures("sample_ti")
    def test_should_respond_307(self, test_client: TestClient, redirect_token: str) -> None:
        resp = test_client.get(
            f"/hitlSharedLinks/redirect/{redirect_token}",
            follow_redirects=False,
        )
        assert resp.status_code == status.HTTP_307_TEMPORARY_REDIRECT
        assert (
            resp.headers["location"]
            == f"http://testserver/dags/{DAG_ID}/runs/{DAG_RUN_ID}/tasks/{TASK_ID}/required_actions"
        )

        # decode token to verify correctness
        shared_data_dict = attrs.asdict(
            HITLSharedLinkData.decode(
                secret_key=test_client.app.state.secret_key,  # type: ignore[attr-defined]
                token=redirect_token,
            )
        )
        assert shared_data_dict["dag_id"] == DAG_ID
        assert shared_data_dict["dag_run_id"] == DAG_RUN_ID
        assert shared_data_dict["task_id"] == TASK_ID
        assert shared_data_dict["shared_link_config"] == {
            "link_type": "redirect",
            "chosen_options": [],
            "params_input": {},
            "expires_at": ANY,
        }

    def test_should_respond_400_with_wrong_secret_key(
        self, test_client: TestClient, redirect_token: str
    ) -> None:
        # wrong secret key with the right length
        # random generated
        test_client.app.state.secret_key = "WuS1iXVNE19OxzHzVL03_3DLRiB5W4r74uxIrl0iZfs"  # type: ignore[attr-defined]

        resp = test_client.get(
            f"/hitlSharedLinks/redirect/{redirect_token}",
            follow_redirects=False,
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert resp.json() == {"detail": "AESGCM key must be 128, 192, or 256 bits."}

    def test_should_respond_400_without_ti(
        self,
        test_client: TestClient,
        redirect_token: str,
        session: Session,
        sample_ti: TaskInstance,
        create_task_instance: CreateTaskInstance,
    ) -> None:
        session.delete(sample_ti.dag_run)
        session.delete(sample_ti)
        session.commit()

        create_task_instance(dag_id=DAG_ID, run_id=DAG_RUN_ID, task_id=TASK_ID, session=session)
        session.commit()

        resp = test_client.get(
            f"/hitlSharedLinks/redirect/{redirect_token}",
            follow_redirects=False,
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert resp.json() == {"detail": "task instance id does not match"}

    def test_should_respond_400_with_invalid_token(self, test_client: TestClient) -> None:
        resp = test_client.get("/hitlSharedLinks/redirect/invalidtoken")
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert resp.json() == {"detail": "Token too short"}

    @pytest.mark.usefixtures("sample_ti")
    def test_should_respond_400_with_invalid_link_type(self, test_client: TestClient) -> None:
        resp = test_client.post(
            f"/hitlSharedLinks/generate/{DAG_ID}/{DAG_RUN_ID}/{TASK_ID}",
            json={
                "link_type": "respond",
                "expires_at": None,
                "chosen_options": ["option 1"],
                "params_input": None,
            },
        )
        url = resp.json()["url"]
        token = url.split("/")[-1]

        resp = test_client.get(
            f"/hitlSharedLinks/redirect/{token}",
            follow_redirects=False,
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert resp.json() == {"detail": "Unexpected link_type 'respond'"}


class TestHitlSharedLinkRespond:
    @pytest.fixture
    def sample_hitl_detail(self, sample_ti: TaskInstance, session: Session) -> HITLDetail:
        hitl_detail_model = HITLDetail(
            ti_id=sample_ti.id,
            options=["Approve", "Reject"],
            subject="This is subject",
            body="this is body",
            defaults=["Approve"],
            multiple=False,
            params={"input_1": 1},
        )
        session.add(hitl_detail_model)
        session.commit()

        return hitl_detail_model

    @pytest.fixture
    def respond_token(self, test_client: TestClient, sample_ti: TaskInstance) -> str:
        """Generate a respond token"""

        resp = test_client.post(
            f"/hitlSharedLinks/generate/{DAG_ID}/{DAG_RUN_ID}/{TASK_ID}",
            json={
                "link_type": "respond",
                "expires_at": None,
                "chosen_options": ["Reject"],
                "params_input": {"input_1": 42},
            },
        )
        url = resp.json()["url"]
        return url.split("/")[-1]

    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200(
        self,
        test_client: TestClient,
        respond_token: str,
        sample_ti: TaskInstance,
        session: Session,
    ) -> None:
        resp = test_client.post(f"/hitlSharedLinks/respond/{respond_token}")
        assert resp.status_code == status.HTTP_200_OK

        # decode token to verify correctness
        shared_data_dict = attrs.asdict(
            HITLSharedLinkData.decode(
                secret_key=test_client.app.state.secret_key,  # type: ignore[attr-defined]
                token=respond_token,
            )
        )
        assert shared_data_dict["dag_id"] == DAG_ID
        assert shared_data_dict["dag_run_id"] == DAG_RUN_ID
        assert shared_data_dict["task_id"] == TASK_ID
        assert shared_data_dict["shared_link_config"] == {
            "chosen_options": ["Reject"],
            "expires_at": ANY,
            "link_type": "respond",
            "params_input": {"input_1": 42},
        }
        session.commit()

        hitl_detail = session.scalar(select(HITLDetail).where(TI.id == sample_ti.id))

        assert hitl_detail.response_at is not None
        assert hitl_detail.user_id == "test"
        assert hitl_detail.chosen_options == ["Reject"]
        assert hitl_detail.params_input == {"input_1": 42}

    def test_should_respond_400_with_wrong_secret_key(
        self, test_client: TestClient, respond_token: str
    ) -> None:
        test_client.app.state.secret_key = "WuS1iXVNE19OxzHzVL03_3DLRiB5W4r74uxIrl0iZfs"  # type: ignore[attr-defined]

        resp = test_client.post(
            f"/hitlSharedLinks/respond/{respond_token}",
            json={"chosen_options": ["option 1"], "params_input": {"input": 42}},
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    def test_should_respond_400_with_invalid_token(self, test_client: TestClient) -> None:
        resp = test_client.post(
            "/hitlSharedLinks/respond/invalidtoken",
            json={"chosen_options": ["option 1"], "params_input": {"input": 42}},
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.usefixtures("sample_ti")
    def test_should_respond_400_with_invalid_link_type(self, test_client: TestClient) -> None:
        # generate redirect token instead of respond
        resp = test_client.post(
            f"/hitlSharedLinks/generate/{DAG_ID}/{DAG_RUN_ID}/{TASK_ID}",
            json={
                "link_type": "redirect",
                "expires_at": None,
                "chosen_options": None,
                "params_input": None,
            },
        )
        url = resp.json()["url"]
        token = url.split("/")[-1]

        resp = test_client.post(
            f"/hitlSharedLinks/respond/{token}",
            json={"chosen_options": [], "params_input": {}},
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert resp.json() == {"detail": "Unexpected link_type 'redirect'"}
