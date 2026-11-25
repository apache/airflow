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

from unittest import mock

import pytest
import requests

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_2_PLUS

if not AIRFLOW_V_3_3_2_PLUS:
    pytest.skip(
        "The example needs the async asset state store API from Airflow 3.3.2", allow_module_level=True
    )

from airflow.sdk.execution_time.context import AssetStateStoreAccessors

from system.http.example_sec_rss import check_sec_feed

FILING_ONE = {"guid": "one", "title": "Filing one", "link": "url-one"}
FILING_TWO = {"guid": "two", "title": "Filing two", "link": "url-two"}


def _build_response(*filings: dict[str, str]) -> mock.Mock:
    items = "".join(
        f"<item><guid>{f['guid']}</guid><title>{f['title']}</title><link>{f['link']}</link></item>"
        for f in filings
    )
    response = mock.Mock(spec=requests.Response)
    response.content = f"<rss><channel>{items}</channel></rss>".encode()
    return response


def _build_store(seen_guids: list[str]) -> mock.Mock:
    store = mock.Mock(spec=AssetStateStoreAccessors)
    store.aget.return_value = seen_guids
    return store


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seen_guids", "filings", "expected_new"),
    [
        pytest.param([], [FILING_ONE], [FILING_ONE], id="first-poll"),
        pytest.param(["one"], [FILING_TWO, FILING_ONE], [FILING_TWO], id="new-filing"),
    ],
)
async def test_check_sec_feed_stores_new_filings(seen_guids, filings, expected_new):
    store = _build_store(seen_guids)

    assert await check_sec_feed(_build_response(*filings), store)

    store.aget.assert_awaited_once_with("seen_guids", [])
    assert store.aset.await_args_list == [
        mock.call("new_filings", expected_new),
        mock.call("seen_guids", [f["guid"] for f in filings]),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seen_guids", "filings"),
    [
        pytest.param(["one"], [FILING_ONE], id="nothing-new"),
        pytest.param([], [], id="empty-feed"),
    ],
)
async def test_check_sec_feed_ignores_known_filings(seen_guids, filings):
    store = _build_store(seen_guids)

    assert not await check_sec_feed(_build_response(*filings), store)

    store.aset.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_sec_feed_rejects_item_without_guid():
    response = mock.Mock(spec=requests.Response)
    response.content = b"<rss><channel><item><title>No GUID</title></item></channel></rss>"

    with pytest.raises(ValueError, match="missing a GUID"):
        await check_sec_feed(response, _build_store([]))


@pytest.mark.asyncio
async def test_check_sec_feed_requires_state_store():
    with pytest.raises(ValueError, match="requires an asset state store"):
        await check_sec_feed(_build_response(FILING_ONE), None)
