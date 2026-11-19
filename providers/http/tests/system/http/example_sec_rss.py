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
"""Watch the SEC's RSS feed for new filings."""

from __future__ import annotations

# [START howto_trigger_http_event_rss]
from datetime import UTC, datetime
from xml.etree import ElementTree

from airflow.providers.http.triggers.http import HttpEventTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task


async def check_sec_feed(response, asset_state_store=None):
    if asset_state_store is None:
        raise ValueError("This example requires an asset state store")

    # The SEC is a trusted source. Parse feeds you do not control with defusedxml instead.
    root = ElementTree.fromstring(response.content)
    filings = [
        {"guid": item.findtext("guid"), "title": item.findtext("title"), "link": item.findtext("link")}
        for item in root.findall("./channel/item")
    ]
    if any(not filing["guid"] for filing in filings):
        raise ValueError("RSS item is missing a GUID")

    seen_guids = set(await asset_state_store.aget("seen_guids", []))
    new_filings = [filing for filing in filings if filing["guid"] not in seen_guids]
    if not new_filings:
        return False
    # Saving the filings before the GUIDs means an interruption between the two repeats a
    # notification on the next poll instead of losing it.
    await asset_state_store.aset("new_filings", new_filings)
    await asset_state_store.aset("seen_guids", [filing["guid"] for filing in filings])
    return True


trigger = HttpEventTrigger(
    http_conn_id="sec_edgar",
    method="GET",
    endpoint="Archives/edgar/usgaap.rss.xml",
    response_check_path="example_sec_rss.check_sec_feed",
    poll_interval=600,
)
asset = Asset(name="sec_filings", watchers=[AssetWatcher(name="sec_rss", trigger=trigger)])


@dag(start_date=datetime(2024, 1, 1, tzinfo=UTC), schedule=asset, catchup=False, tags=["example"])
def example_sec_rss():
    @task(inlets=[asset])
    def print_new_filings(**context):
        for filing in context["asset_state_store"].get("new_filings", []):
            print(f"{filing['title']}: {filing['link']}")

    print_new_filings()


sec_rss_dag = example_sec_rss()
# [END howto_trigger_http_event_rss]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(sec_rss_dag)
