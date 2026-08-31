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

from airflow.providers.common.search.log.shared_response import (
    AttributeDict,
    AttributeList,
    Hit,
    HitMeta,
    SearchResponse,
    _wrap,
    resolve_nested,
)


class _FakeSearch:
    """Minimal stand-in for a task handler exposing the ``_get_result`` hook used by SearchResponse."""

    @staticmethod
    def _get_result(document):
        return Hit(document)


class TestWrap:
    def test_wrap_with_dict(self):
        result = _wrap({"key1": "value1"})
        assert isinstance(result, AttributeDict)
        assert result.key1 == "value1"

    @pytest.mark.parametrize("value", [1, [2, 3], "string", 4.5])
    def test_wrap_with_non_dict(self, value):
        assert _wrap(value) == value


class TestAttributeList:
    def test_initialization(self):
        assert AttributeList([1, 2, 3])._l_ == [1, 2, 3]
        assert AttributeList((1, 2, 3))._l_ == [1, 2, 3]

    def test_index_access(self):
        attr_list = AttributeList([1, {"key1": "value1"}, 3])
        assert attr_list[0] == 1
        assert isinstance(attr_list[1], AttributeDict)
        assert attr_list[1].key1 == "value1"

    def test_slice_access_returns_plain_list(self):
        attr_list = AttributeList([1, 2, 3, 4])
        assert attr_list[1:3] == [2, 3]

    def test_iteration(self):
        attr_list = AttributeList([1, {"key": "value"}, 3])
        items = list(attr_list)
        assert items[0] == 1
        assert isinstance(items[1], AttributeDict)
        assert items[2] == 3

    def test_boolean_representation(self):
        assert AttributeList([1, 2, 3])
        assert not AttributeList([])


class TestAttributeDict:
    def test_attribute_and_item_access(self):
        attr_dict = AttributeDict({"key1": "value1", "key2": {"subkey1": "subvalue1"}})
        assert attr_dict.key1 == "value1"
        assert attr_dict["key1"] == "value1"
        assert isinstance(attr_dict.key2, AttributeDict)
        assert attr_dict.key2.subkey1 == "subvalue1"

    def test_nonexistent_key(self):
        with pytest.raises(AttributeError):
            _ = AttributeDict({"key1": "value1"}).nonexistent_key

    def test_to_dict(self):
        test_dict = {"key1": "value1", "key2": "value2"}
        assert AttributeDict(test_dict).to_dict() == test_dict


class TestResolveNested:
    def test_returns_hit_without_parent_class(self):
        assert resolve_nested({"_nested": {"field": "comments"}}) is Hit

    def test_resolves_doc_class_from_parent_index(self):
        class FakeDocClass:
            pass

        class FakeField:
            _doc_class = FakeDocClass

        class FakeIndex:
            @staticmethod
            def resolve_field(path):
                assert path == "comments.replies"
                return FakeField

        class FakeParent:
            _index = FakeIndex

        hit = {"_nested": {"field": "comments", "_nested": {"field": "replies"}}}
        assert resolve_nested(hit, FakeParent) is FakeDocClass

    def test_falls_back_to_hit_when_field_unresolved(self):
        class FakeIndex:
            @staticmethod
            def resolve_field(path):
                return None

        class FakeParent:
            _index = FakeIndex

        assert resolve_nested({"_nested": {"field": "comments"}}, FakeParent) is Hit


class TestHitAndHitMeta:
    HIT_DOCUMENT: dict[str, Any] = {
        "_id": "jdeZT4kBjAZqZnexVUxk",
        "_index": ".ds-filebeat-8.8.2",
        "_score": 2.482621,
        "_source": {
            "asctime": "2023-07-09T07:47:43.907+0000",
            "dag_id": "example_bash_operator",
            "lineno": 1144,
            "levelname": "INFO",
        },
        "_type": "_doc",
    }

    def test_hit_initialization_and_to_dict(self):
        hit = Hit(self.HIT_DOCUMENT)
        assert hit.asctime == "2023-07-09T07:47:43.907+0000"
        assert hit.dag_id == "example_bash_operator"
        assert hit.lineno == 1144
        assert isinstance(hit.meta, HitMeta)
        assert hit.to_dict() == self.HIT_DOCUMENT["_source"]

    def test_hit_merges_fields(self):
        document = {"_source": {"a": 1}, "fields": {"b": 2}}
        hit = Hit(document)
        assert hit.a == 1
        assert hit.b == 2

    def test_hitmeta_initialization_and_to_dict(self):
        hitmeta = HitMeta(self.HIT_DOCUMENT)
        assert hitmeta.id == "jdeZT4kBjAZqZnexVUxk"
        assert hitmeta.index == ".ds-filebeat-8.8.2"
        assert hitmeta.score == 2.482621
        assert hitmeta.doc_type == "_doc"


class TestSearchResponse:
    RESPONSE: dict[str, Any] = {
        "_shards": {"failed": 0, "successful": 7, "total": 7},
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_id": "jdeZT4kBjAZqZnexVUxk",
                    "_source": {
                        "asctime": "2023-07-09T07:47:43.907+0000",
                        "levelname": "INFO",
                    },
                    "_type": "_doc",
                }
            ],
        },
    }

    def test_initialization_hits_iter_getitem_and_bool(self):
        response = SearchResponse(_FakeSearch(), self.RESPONSE)

        assert response._d_ == self.RESPONSE
        assert isinstance(response.hits, AttributeList)

        for hit in response:
            assert isinstance(hit, Hit)
            assert isinstance(hit.meta, HitMeta)

        assert response[0].asctime == "2023-07-09T07:47:43.907+0000"
        assert response.hits[0].levelname == "INFO"
        # Extra "hits" keys are attached to the AttributeList.
        assert response.hits.total.value == 1
        # Non-integer keys fall through to the underlying response dict.
        assert response["_shards"].total == 7
        assert bool(response) is True

    def test_empty_response_is_falsy(self):
        response = SearchResponse(_FakeSearch(), {"hits": {"hits": []}})
        assert bool(response) is False

    def test_unparseable_hits_raise_type_error(self):
        # A search object missing ``_get_result`` triggers the AttributeError -> TypeError translation.
        response = SearchResponse(object(), self.RESPONSE)
        with pytest.raises(TypeError, match="Could not parse hits."):
            _ = response.hits
