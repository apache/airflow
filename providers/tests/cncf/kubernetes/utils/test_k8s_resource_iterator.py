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

from collections import namedtuple

import pytest
import yaml
from kubernetes.utils import FailToCreateError

from airflow.providers.cncf.kubernetes.utils.k8s_resource_iterator import (
    k8s_resource_iterator,
)

TEST_VALID_LIST_RESOURCE_YAML = """
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: test_pvc_1
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: test_pvc_2
"""


def test_k8s_resource_iterator():
    exception_k8s = namedtuple("Exception_k8s", ["reason", "body"])

    def test_callback_failing(yml_doc: dict) -> None:
        raise FailToCreateError([exception_k8s(reason="the_reason", body="the_body ")])

    with pytest.raises(FailToCreateError) as exc_info:
        k8s_resource_iterator(
            test_callback_failing,
            resources=yaml.safe_load_all(TEST_VALID_LIST_RESOURCE_YAML),
        )
    assert (
        str(exc_info.value)
        == "Error from server (the_reason): the_body Error from server (the_reason): the_body "
    )

    def callback_success(yml_doc: dict) -> None:
        return

    k8s_resource_iterator(
        callback_success, resources=yaml.safe_load_all(TEST_VALID_LIST_RESOURCE_YAML)
    )
