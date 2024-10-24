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

from unittest.mock import Mock, patch

import pytest
from kubernetes.client import models as k8s

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    _convert_from_dict,
    _convert_kube_model_object,
    convert_affinity,
    convert_configmap,
    convert_env_vars,
    convert_env_vars_or_raise_error,
    convert_image_pull_secrets,
    convert_pod_runtime_info_env,
    convert_port,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)


# testcase of _convert_kube_model_object() function
class MockKubeModelObject:
    def to_k8s_client_obj(self):
        return "converted_object"


def test__convert_kube_model_object_normal_value():
    obj = MockKubeModelObject()
    new_class = type(obj)

    result = _convert_kube_model_object(obj, new_class)
    assert result == "converted_object"


def test__convert_kube_model_object_already_instance():
    result = _convert_kube_model_object("obj", str)
    assert result == "obj"


def test__convert_kube_model_object_invalid_type():
    obj = "obj"
    with pytest.raises(AirflowException) as exc_info:
        _convert_kube_model_object(obj, int)

    assert str(exc_info.value) == f"Expected {int}, got {type(obj)}"


# testcase of _convert_from_dict() function
@pytest.fixture
def mock_api_client():
    with patch(
        "airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters.ApiClient"
    ) as mock_class:
        instance = mock_class.return_value
        instance._ApiClient__deserialize_model = Mock(return_value="mocked_instance")
        yield instance


def test_convert_from_dict_with_new_class_instance(mock_api_client):
    obj = Mock()
    result = _convert_from_dict(obj, type(obj))

    assert result == obj


def test_convert_from_dict_with_dict(mock_api_client):
    obj = {"key": "value"}
    new_class = Mock()
    result = _convert_from_dict(obj, type(new_class))

    mock_api_client._ApiClient__deserialize_model.assert_called_once_with(obj, type(new_class))
    assert result == "mocked_instance"


def test_convert_from_dict_with_invalid_type():
    obj = "not a dict"
    new_class = Mock()

    with pytest.raises(AirflowException) as exc_info:
        _convert_from_dict(obj, type(new_class))

    assert str(exc_info.value) == "Expected dict or <class 'unittest.mock.Mock'>, got <class 'str'>"


# testcase of convert_volume() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_kube_model_object")
def test_convert_volume_normal_value(mock_convert_kube_model_object):
    mock_convert_kube_model_object.return_value = k8s.V1Volume(name="test_convert_volume")
    volume = Mock()
    result = convert_volume(volume)

    assert result.name == "test_convert_volume"


# testcase of convert_volume_mount() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_kube_model_object")
def test_convert_volume_mount_normal_value(mock_convert_kube_model_object):
    mock_convert_kube_model_object.return_value = k8s.V1VolumeMount(
        name="test_volume_mount", mount_path="/mnt/test"
    )
    volume = Mock()
    result = convert_volume_mount(volume)

    assert result.name == "test_volume_mount"
    assert result.mount_path == "/mnt/test"


# testcase of convert_port() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_kube_model_object")
def test_convert_port_normal_value(mock_convert_kube_model_object):
    mock_convert_kube_model_object.return_value = k8s.V1ContainerPort(container_port=80)
    volume = Mock()
    result = convert_port(volume)

    assert result.container_port == 80


# testcase of convert_env_vars() function
def test_convert_env_vars_with_dict():
    # Normal value input case test
    env_vars = {"FOO": "bar", "BAZ": "qux"}
    result = convert_env_vars(env_vars)

    expected_result = [k8s.V1EnvVar(name="FOO", value="bar"), k8s.V1EnvVar(name="BAZ", value="qux")]
    assert result == expected_result


def test_convert_env_vars_with_list():
    # Normal value input case test
    env_vars = [k8s.V1EnvVar(name="FOO", value="bar"), k8s.V1EnvVar(name="BAZ", value="qux")]

    result = convert_env_vars(env_vars)
    assert result == env_vars


# testcase of convert_env_vars_or_raise_error() function
def test_convert_env_vars_or_raise_error_normal_value():
    env_vars_dict = {"ENV1": "value1", "ENV2": "value2"}
    result = convert_env_vars_or_raise_error(env_vars_dict)

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0].name == "ENV1"
    assert result[0].value == "value1"
    assert result[1].name == "ENV2"
    assert result[1].value == "value2"


def test_convert_env_vars_or_raise_error_list_value():
    env_vars_list = [k8s.V1EnvVar(name="ENV1", value="value1"), k8s.V1EnvVar(name="ENV2", value="value2")]
    result = convert_env_vars_or_raise_error(env_vars_list)

    assert result == env_vars_list


def test_convert_env_vars_or_raise_error_invalid_type():
    invalid_input = 123
    with pytest.raises(AirflowException) as exc_info:
        convert_env_vars_or_raise_error(invalid_input)

    assert str(exc_info.value) == f"Expected dict or list, got {type(invalid_input)}"


# testcase of convert_pod_runtime_info_env() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_kube_model_object")
def test_convert_pod_runtime_info_env_normal_value(mock_convert_kube_model_object):
    mock_convert_kube_model_object.return_value = k8s.V1EnvVar(name="FOO", value="bar")
    volume = Mock()
    result = convert_pod_runtime_info_env(volume)

    assert result.name == "FOO"
    assert result.value == "bar"


# testcase of convert_image_pull_secrets() function
def test_convert_image_pull_secrets_normal_value():
    image_pull_secrets = "secret1,secret2,secret3"
    result = convert_image_pull_secrets(image_pull_secrets)

    expected_result = [
        k8s.V1LocalObjectReference(name="secret1"),
        k8s.V1LocalObjectReference(name="secret2"),
        k8s.V1LocalObjectReference(name="secret3"),
    ]
    assert result == expected_result


def test_convert_image_pull_secrets_not_string():
    image_pull_secrets = ["single_secret"]
    result = convert_image_pull_secrets(image_pull_secrets)

    assert result == image_pull_secrets


# testcase of convert_configmap() function
def test_convert_configmap_normal_value():
    configmaps = "test-configmap"
    result = convert_configmap(configmaps)

    assert isinstance(result, k8s.V1EnvFromSource)
    assert result.config_map_ref.name == "test-configmap"


# testcase of convert_affinity() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_from_dict")
def test_convert_affinity_normal_value(mock_convert_from_dict):
    affinity = {"some_key": "some_value"}
    expected_result = Mock(k8s.V1Affinity)
    mock_convert_from_dict.return_value = expected_result

    result = convert_affinity(affinity)

    mock_convert_from_dict.assert_called_once_with(affinity, k8s.V1Affinity)
    assert result == expected_result


# testcase of convert_toleration() function
@patch("airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters._convert_from_dict")
def test_convert_toleration_normal_value(mock_convert_from_dict):
    toleration = {
        "key": "key",
        "operator": "Equal",
        "value": "value",
        "effect": "NoExecute",
        "toleration_seconds": 600,
    }
    expected_result = Mock(spec=k8s.V1Toleration)
    mock_convert_from_dict.return_value = expected_result

    result = convert_toleration(toleration)

    mock_convert_from_dict.assert_called_once_with(toleration, k8s.V1Toleration)
    assert result == expected_result
