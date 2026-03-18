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
from __future__ import annotations

from datetime import datetime
from unittest import mock

import pytest

from airflow.utils import operator_helpers


class TestOperatorHelpers:
    def setup_method(self):
        self.dag_id = "dag_id"
        self.task_id = "task_id"
        self.try_number = 1
        self.logical_date = "2017-05-21T00:00:00"
        self.dag_run_id = "dag_run_id"
        self.owner = ["owner1", "owner2"]
        self.email = ["email1@test.com"]
        self.context = {
            "dag_run": mock.MagicMock(
                name="dag_run",
                run_id=self.dag_run_id,
                logical_date=datetime.strptime(self.logical_date, "%Y-%m-%dT%H:%M:%S"),
            ),
            "task_instance": mock.MagicMock(
                name="task_instance",
                task_id=self.task_id,
                dag_id=self.dag_id,
                try_number=self.try_number,
                logical_date=datetime.strptime(self.logical_date, "%Y-%m-%dT%H:%M:%S"),
            ),
            "task": mock.MagicMock(name="task", owner=self.owner, email=self.email),
        }


def callable1(ds_nodash):
    return (ds_nodash,)


def callable3(ds_nodash, *args, **kwargs):
    return (ds_nodash, args, kwargs)


def callable4(ds_nodash, **kwargs):
    return (ds_nodash, kwargs)


def callable5(**kwargs):
    return (kwargs,)


def callable6(arg1, ds_nodash):
    return (arg1, ds_nodash)


def callable7(arg1, **kwargs):
    return (arg1, kwargs)


def callable8(arg1, *args, **kwargs):
    return (arg1, args, kwargs)


def callable9(*args, **kwargs):
    return (args, kwargs)


def callable10(arg1, *, ds_nodash="20200201"):
    return (arg1, ds_nodash)


def callable11(*, ds_nodash, **kwargs):
    return (
        ds_nodash,
        kwargs,
    )


KWARGS = {
    "ds_nodash": "20200101",
}


@pytest.mark.parametrize(
    ("func", "args", "kwargs", "expected"),
    [
        (callable1, (), KWARGS, ("20200101",)),
        (
            callable5,
            (),
            KWARGS,
            (KWARGS,),
        ),
        (callable6, (1,), KWARGS, (1, "20200101")),
        (callable7, (1,), KWARGS, (1, KWARGS)),
        (callable8, (1, 2), KWARGS, (1, (2,), KWARGS)),
        (callable9, (1, 2), KWARGS, ((1, 2), KWARGS)),
        (callable10, (1,), KWARGS, (1, "20200101")),
    ],
)
def test_make_kwargs_callable(func, args, kwargs, expected):
    kwargs_callable = operator_helpers.make_kwargs_callable(func)
    ret = kwargs_callable(*args, **kwargs)
    assert ret == expected


def test_make_kwargs_callable_conflict():
    def func(ds_nodash):
        pytest.fail(f"Should not reach here: {ds_nodash}")

    kwargs_callable = operator_helpers.make_kwargs_callable(func)

    args = ["20200101"]
    kwargs = {"ds_nodash": "20200101"}

    with pytest.raises(ValueError, match="ds_nodash"):
        kwargs_callable(*args, **kwargs)


@pytest.mark.parametrize(
    ("func", "args", "kwargs", "expected"),
    [
        (callable10, (1, 2), {"ds_nodash": 1}, {"ds_nodash": 1}),
        (callable11, (1, 2), {"ds_nodash": 1}, {"ds_nodash": 1}),
    ],
)
def test_args_and_kwargs_conflicts(func, args, kwargs, expected):
    kwargs_result = operator_helpers.determine_kwargs(func, args=args, kwargs=kwargs)
    assert expected == kwargs_result
