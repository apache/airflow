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

import pendulum

from airflow.decorators import dag, task_group


def test_task_group_with_overridden_kwargs():
    @task_group(
        default_args={
            'params': {
                'x': 5,
                'y': 5,
            },
        },
        add_suffix_on_collision=True,
    )
    def simple_tg():
        ...

    tg_with_overridden_kwargs = simple_tg.override(
        group_id='custom_group_id',
        default_args={
            'params': {
                'x': 10,
            },
        },
    )

    assert tg_with_overridden_kwargs.kwargs == {
        'group_id': 'custom_group_id',
        'default_args': {
            'params': {
                'x': 10,
            },
        },
        'add_suffix_on_collision': True,
    }


def test_tooltip_derived_from_function_docstring():
    """Test that the tooltip for TaskGroup is the decorated-function's docstring."""

    @dag(start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg():
            """Function docstring."""

        tg()

    _ = pipeline()

    assert _.task_group_dict["tg"].tooltip == "Function docstring."


def test_tooltip_not_overriden_by_function_docstring():
    """
    Test that the tooltip for TaskGroup is the explicitly set value even if the decorated function has a
    docstring.
    """

    @dag(start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group(tooltip="tooltip for the TaskGroup")
        def tg():
            """Function docstring."""

        tg()

    _ = pipeline()

    assert _.task_group_dict["tg"].tooltip == "tooltip for the TaskGroup"
