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

from typing import Any

import pytest

from airflow.providers.elasticsearch.log.es_response import (
    AttributeDict,
    AttributeList,
    ElasticSearchResponse,
    Hit,
    HitMeta,
    _wrap,
)
from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler


class TestWrap:
    def test_wrap_with_dict(self):
        test_dict = {"key1": "value1"}
        result = _wrap(test_dict)
        assert isinstance(result, AttributeDict)
        assert result.key1 == "value1"

    def test_wrap_with_non_dict(self):
        test_values = [1, [2, 3], "string", 4.5]
        for value in test_values:
            assert _wrap(value) == value


class TestAttributeList:
    def test_initialization(self):
        test_list = [1, 2, 3]
        attr_list = AttributeList(test_list)
        assert attr_list._l_ == test_list

        test_tuple = (1, 2, 3)
        attr_list = AttributeList(test_tuple)
        assert attr_list._l_ == list(test_tuple)

    def test_index_access(self):
        test_list = [1, {"key1": "value1"}, 3]
        attr_list = AttributeList(test_list)

        assert attr_list[0] == 1
        assert isinstance(attr_list[1], AttributeDict)
        assert attr_list[1].key1 == "value1"
        assert attr_list[2] == 3

    def test_iteration(self):
        test_list = [1, {"key": "value"}, 3]
        attr_list = AttributeList(test_list)

        for i, item in enumerate(attr_list):
            if isinstance(test_list[i], dict):
                assert isinstance(item, AttributeDict)
            else:
                assert item == test_list[i]

    def test_boolean_representation(self):
        assert AttributeList([1, 2, 3])
        assert not (AttributeList([]))


class TestAttributeDict:
    def test_initialization(self):
        test_dict = {"key1": "value1", "key2": "value2"}
        attr_dict = AttributeDict(test_dict)
        assert attr_dict._d_ == test_dict

    def test_attribute_access(self):
        test_dict = {"key1": "value1", "key2": {"subkey1": "subvalue1"}}
        attr_dict = AttributeDict(test_dict)

        assert attr_dict.key1 == "value1"
        assert isinstance(attr_dict.key2, AttributeDict)
        assert attr_dict.key2.subkey1 == "subvalue1"

    def test_item_access(self):
        test_dict = {"key1": "value1", "key2": "value2"}
        attr_dict = AttributeDict(test_dict)

        assert attr_dict["key1"] == "value1"
        assert attr_dict["key2"] == "value2"

    def test_nonexistent_key(self):
        test_dict = {"key1": "value1"}
        attr_dict = AttributeDict(test_dict)

        with pytest.raises(AttributeError):
            _ = attr_dict.nonexistent_key

    def test_to_dict(self):
        test_dict = {"key1": "value1", "key2": "value2"}
        attr_dict = AttributeDict(test_dict)
        assert attr_dict.to_dict() == test_dict


class TestHitAndHitMetaAndElasticSearchResponse:
    ES_DOCUMENT: dict[str, Any] = {
        "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
        "hits": {
            "hits": [
                {
                    "_id": "jdeZT4kBjAZqZnexVUxk",
                    "_index": ".ds-filebeat-8.8.2-2023.07.09-000001",
                    "_score": 2.482621,
                    "_source": {
                        "@timestamp": "2023-07-13T14:13:15.140Z",
                        "asctime": "2023-07-09T07:47:43.907+0000",
                        "container": {"id": "airflow"},
                        "dag_id": "example_bash_operator",
                        "ecs": {"version": "8.0.0"},
                        "execution_date": "2023_07_09T07_47_32_000000",
                        "filename": "taskinstance.py",
                        "input": {"type": "log"},
                        "levelname": "INFO",
                        "lineno": 1144,
                        "log": {
                            "file": {
                                "path": "/opt/airflow/Documents/GitHub/airflow/logs/"
                                "dag_id=example_bash_operator'"
                                "/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
                            },
                            "offset": 0,
                        },
                        "log.offset": 1688888863907337472,
                        "log_id": "example_bash_operator-run_after_loop-owen_run_run--1-1",
                        "message": "Dependencies all met for "
                        "dep_context=non-requeueable deps "
                        "ti=<TaskInstance: "
                        "example_bash_operator.run_after_loop "
                        "owen_run_run [queued]>",
                        "task_id": "run_after_loop",
                        "try_number": "1",
                    },
                    "_type": "_doc",
                }
            ]
        },
    }
    HIT_DOCUMENT = ES_DOCUMENT["hits"]["hits"][0]

    def test_hit_initialization_and_to_dict(self):
        hit = Hit(self.HIT_DOCUMENT)

        assert hit.asctime == "2023-07-09T07:47:43.907+0000"
        assert hit.dag_id == "example_bash_operator"
        assert hit.lineno == 1144
        assert (
            hit.log.file.path
            == "/opt/airflow/Documents/GitHub/airflow/logs/dag_id=example_bash_operator'/run_id=owen_run_run/task_id=run_after_loop/attempt=1.log"
        )

        # Test meta attribute
        assert isinstance(hit.meta, HitMeta)
        assert hit.to_dict() == self.HIT_DOCUMENT["_source"]

    def test_hitmeta_initialization_and_to_dict(self):
        hitmeta = HitMeta(self.HIT_DOCUMENT)

        assert hitmeta.id == "jdeZT4kBjAZqZnexVUxk"
        assert hitmeta.index == ".ds-filebeat-8.8.2-2023.07.09-000001"
        assert hitmeta.score == 2.482621
        assert hitmeta.doc_type == "_doc"

        expected_dict = {
            k[1:] if k.startswith("_") else k: v
            for (k, v) in self.HIT_DOCUMENT.items()
            if k != "_source"
        }
        expected_dict["doc_type"] = expected_dict.pop("type")
        assert hitmeta.to_dict() == expected_dict

    def test_elasticsearchresponse_initialization_and_hits_and_bool(self):
        task_handler = ElasticsearchTaskHandler(
            base_log_folder="local/log/location",
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
        )
        response = ElasticSearchResponse(task_handler, self.ES_DOCUMENT)

        assert response._d_ == self.ES_DOCUMENT
        assert isinstance(response.hits, AttributeList)

        for hit in response.hits:
            assert isinstance(hit, Hit)
            assert isinstance(hit.meta, HitMeta)

        assert response.hits[0].asctime == "2023-07-09T07:47:43.907+0000"
        assert response.hits[0].levelname == "INFO"

        assert bool(response) is True
