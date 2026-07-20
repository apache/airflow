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

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.toolsets.aws import AWSToolset
from airflow.providers.common.ai.utils.tool_definition import _SUPPORTS_RETURN_SCHEMA


def _make_toolset(**kwargs):
    kwargs.setdefault("allowed_actions", ["s3:List*", "s3:GetBucketLocation"])
    return AWSToolset("aws_test", **kwargs)


def _make_mock_client():
    client = MagicMock()
    client.can_paginate.return_value = False
    return client


def _call(ts, name, args):
    return asyncio.run(ts.call_tool(name, args, ctx=MagicMock(), tool=MagicMock()))


class TestAWSToolsetInit:
    def test_id_includes_conn_id(self):
        ts = _make_toolset()
        assert ts.id == "aws-aws_test"

    def test_rejects_empty_allowed_actions(self):
        with pytest.raises(ValueError, match="non-empty"):
            AWSToolset("aws_test", allowed_actions=[])

    @pytest.mark.parametrize(
        ("action", "match"),
        [
            ("s3", "expected"),
            ("s3:", "expected"),
            (":ListBuckets", "expected"),
            ("*:GetObject", "wildcards are not allowed in the service part"),
            ("nosuchservice:DoThing", "Unknown AWS service"),
            ("s3:NoSuchOperation", "Unknown operation"),
        ],
    )
    def test_rejects_invalid_action(self, action, match):
        with pytest.raises(ValueError, match=match):
            AWSToolset("aws_test", allowed_actions=[action])

    def test_accepts_snake_case_operation(self):
        AWSToolset("aws_test", allowed_actions=["s3:list_buckets"])

    def test_rejects_wildcard_action_that_matches_no_operations(self):
        with pytest.raises(ValueError, match="does not match any"):
            AWSToolset("aws_test", allowed_actions=["s3:ListObjectz*"])

    def test_rejects_wildcard_action_that_only_matches_sensitive_operations(self):
        with pytest.raises(ValueError, match="does not match any"):
            AWSToolset("aws_test", allowed_actions=["kms:Decrypt*"])


class TestAWSToolsetGetTools:
    def test_returns_three_tools(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"list_aws_operations", "describe_aws_operation", "call_aws"}

    def test_tool_definitions_have_descriptions(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.description

    @pytest.mark.skipif(
        not _SUPPORTS_RETURN_SCHEMA, reason="pydantic-ai too old for ToolDefinition.return_schema"
    )
    def test_tools_declare_string_return_schema(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.return_schema == {"type": "string"}


class TestAWSToolsetActionMatching:
    @pytest.mark.parametrize(
        ("actions", "service", "operation", "allowed"),
        [
            (["s3:ListBuckets"], "s3", "ListBuckets", True),
            (["s3:list_buckets"], "s3", "ListBuckets", True),
            (["s3:List*"], "s3", "ListObjectsV2", True),
            (["s3:List*"], "s3", "GetObject", False),
            (["s3:*"], "ec2", "DescribeInstances", False),
            (["secretsmanager:*"], "secretsmanager", "GetSecretValue", False),
            (["secretsmanager:GetSecretValue"], "secretsmanager", "GetSecretValue", True),
            (["kms:*"], "kms", "Decrypt", False),
            (["kms:Decrypt"], "kms", "Decrypt", True),
        ],
    )
    def test_allow_list_matching(self, actions, service, operation, allowed):
        ts = AWSToolset("aws_test", allowed_actions=actions)
        assert ts._is_action_allowed(service=service, operation=operation) is allowed


class TestAWSToolsetListOperations:
    def test_lists_only_allowed_operations(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:List*"])
        listing = json.loads(_call(ts, "list_aws_operations", {}))
        assert "ListBuckets" in listing["s3"]
        assert "GetObject" not in listing["s3"]

    def test_sensitive_operations_hidden_behind_wildcard(self):
        ts = AWSToolset("aws_test", allowed_actions=["secretsmanager:*"])
        listing = json.loads(_call(ts, "list_aws_operations", {}))
        assert "ListSecrets" in listing["secretsmanager"]
        assert "GetSecretValue" not in listing["secretsmanager"]

    def test_verbatim_sensitive_operation_is_listed(self):
        ts = AWSToolset("aws_test", allowed_actions=["secretsmanager:*", "secretsmanager:GetSecretValue"])
        listing = json.loads(_call(ts, "list_aws_operations", {}))
        assert "GetSecretValue" in listing["secretsmanager"]

    def test_service_outside_allow_list_raises_model_retry(self):
        ts = _make_toolset()
        with pytest.raises(ModelRetry, match="not in this toolset's allowed actions"):
            _call(ts, "list_aws_operations", {"service": "ec2"})


class TestAWSToolsetDescribeOperation:
    def test_returns_input_shape_with_required_members(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListObjectsV2"])
        described = json.loads(
            _call(ts, "describe_aws_operation", {"service": "s3", "operation": "ListObjectsV2"})
        )
        assert described["operation"] == "ListObjectsV2"
        assert described["input"]["Bucket"]["required"] is True
        assert "Contents" in described["output"]

    def test_resolves_snake_case_operation_name(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListObjectsV2"])
        described = json.loads(
            _call(ts, "describe_aws_operation", {"service": "s3", "operation": "list_objects_v2"})
        )
        assert described["operation"] == "ListObjectsV2"

    def test_disallowed_operation_raises_model_retry(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"])
        with pytest.raises(ModelRetry, match="not in this toolset's allowed actions"):
            _call(ts, "describe_aws_operation", {"service": "s3", "operation": "GetObject"})


class TestAWSToolsetCallAws:
    def test_executes_operation_and_returns_json(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"])
        client = _make_mock_client()
        client.list_buckets.return_value = {
            "Buckets": [
                {
                    "Name": "data-lake-raw",
                    "CreationDate": datetime(2026, 1, 1, tzinfo=timezone.utc),
                }
            ],
            "ResponseMetadata": {"RequestId": "abc123"},
        }
        ts._clients["s3"] = client

        result = json.loads(_call(ts, "call_aws", {"service": "s3", "operation": "ListBuckets"}))
        client.list_buckets.assert_called_once_with()
        assert result["Buckets"][0]["Name"] == "data-lake-raw"
        # datetime serialized via default=str, ResponseMetadata stripped.
        assert "2026-01-01" in result["Buckets"][0]["CreationDate"]
        assert "ResponseMetadata" not in result

    def test_snake_case_operation_resolves(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"])
        client = _make_mock_client()
        client.list_buckets.return_value = {"Buckets": []}
        ts._clients["s3"] = client

        result = json.loads(_call(ts, "call_aws", {"service": "s3", "operation": "list_buckets"}))
        assert result == {"Buckets": []}

    def test_uses_paginator_when_available(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListObjectsV2"], max_items=250)
        client = _make_mock_client()
        client.can_paginate.return_value = True
        paginator = client.get_paginator.return_value
        paginator.paginate.return_value.build_full_result.return_value = {"Contents": [{"Key": "a.parquet"}]}
        ts._clients["s3"] = client

        result = json.loads(
            _call(
                ts,
                "call_aws",
                {"service": "s3", "operation": "ListObjectsV2", "parameters": {"Bucket": "b"}},
            )
        )
        paginator.paginate.assert_called_once_with(Bucket="b", PaginationConfig={"MaxItems": 250})
        assert result["Contents"] == [{"Key": "a.parquet"}]

    def test_disallowed_operation_raises_model_retry_without_client(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"])
        with pytest.raises(ModelRetry, match="not in this toolset's allowed actions"):
            _call(ts, "call_aws", {"service": "s3", "operation": "DeleteBucket"})
        assert ts._clients == {}

    def test_client_error_raises_model_retry(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"])
        client = _make_mock_client()
        client.list_buckets.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "not authorized"}}, "ListBuckets"
        )
        ts._clients["s3"] = client

        with pytest.raises(ModelRetry, match="AccessDenied"):
            _call(ts, "call_aws", {"service": "s3", "operation": "ListBuckets"})

    def test_large_response_is_truncated(self):
        ts = AWSToolset("aws_test", allowed_actions=["s3:ListBuckets"], max_output_bytes=200)
        client = _make_mock_client()
        client.list_buckets.return_value = {"Buckets": [{"Name": "x" * 50}] * 50}
        ts._clients["s3"] = client

        result = json.loads(_call(ts, "call_aws", {"service": "s3", "operation": "ListBuckets"}))
        assert result["truncated"] is True
        assert len(result["data"]) == 200

    @patch("airflow.providers.common.ai.toolsets.aws.AwsBaseHook", autospec=True)
    def test_client_resolved_via_amazon_hook(self, mock_hook_cls):
        client = _make_mock_client()
        client.list_buckets.return_value = {"Buckets": []}
        mock_hook_cls.return_value.get_conn.return_value = client

        ts = AWSToolset("aws_prod", allowed_actions=["s3:ListBuckets"], region_name="eu-west-1")
        _call(ts, "call_aws", {"service": "s3", "operation": "ListBuckets"})
        mock_hook_cls.assert_called_once_with(
            aws_conn_id="aws_prod", client_type="s3", region_name="eu-west-1"
        )

    def test_unknown_tool_raises_value_error(self):
        ts = _make_toolset()
        with pytest.raises(ValueError, match="Unknown tool"):
            _call(ts, "use_aws", {})
