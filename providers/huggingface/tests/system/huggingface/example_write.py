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
"""Publish to a temporary private Hub repo and validate XCom, asset, and Hub head."""

from __future__ import annotations

import os
import uuid

from packaging.version import Version
from sqlalchemy import delete, or_, select

import airflow
from airflow.providers.common.compat.sdk import TriggerRule
from airflow.providers.huggingface.hooks.huggingface import HuggingFaceHook
from airflow.providers.huggingface.operators.datasets import (
    LoadDatasetOperator,
    MapDatasetOperator,
    PushToHubOperator,
)
from airflow.sdk import DAG, Asset, get_current_context, task

HUB_ORG = os.environ.get("HF_TEST_HUB_ORG", "airflow-hf")
SYSTEM_TESTS_ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID") or uuid.uuid4().hex
REPO_BASE = os.environ.get("HF_TEST_WRITE_REPO_ID", f"{HUB_ORG}/airflow-provider-system-write")
REPO_ID = f"{REPO_BASE}-{SYSTEM_TESTS_ENV_ID}"
REPO_DURABLE_BASE = os.environ.get("HF_TEST_DURABLE_REPO_ID", f"{HUB_ORG}/airflow-provider-system-durable")
REPO_DURABLE_ID = f"{REPO_DURABLE_BASE}-{SYSTEM_TESTS_ENV_ID}"
CONN_ID = os.environ.get("HF_TEST_HUB_CONN_ID", "huggingface_default")
REVISION = "bcdcba79d07bc864c1c254ccfcedcce55bcc9a8c"

PUBLISHED_ASSET = Asset(uri=f"hf://datasets/{REPO_ID}")
DURABLE_ASSET = Asset(uri=f"hf://datasets/{REPO_DURABLE_ID}")


def _activate_asset(asset: Asset):
    """Register and activate an asset directly in the Airflow database for test runs."""
    try:
        from airflow.models.asset import AssetActive, AssetModel
        from airflow.settings import Session

        session_factory = Session
        if session_factory is None:
            return

        with session_factory() as session:
            # 1. Upsert AssetModel
            asset_model = session.scalar(
                select(AssetModel).where(or_(AssetModel.name == asset.name, AssetModel.uri == asset.uri))
            )
            if not asset_model:
                asset_model = AssetModel(name=asset.name, uri=asset.uri)
                session.add(asset_model)
                session.flush()

            # 2. Upsert AssetActive
            active_entry = session.scalar(
                select(AssetActive).where(or_(AssetActive.name == asset.name, AssetActive.uri == asset.uri))
            )
            if not active_entry:
                active_entry = AssetActive(name=asset.name, uri=asset.uri)
                session.add(active_entry)

            session.commit()
    except ImportError:
        pass


def _deactivate_asset(asset: Asset):
    """Clean up dynamic test asset records from the DB."""
    try:
        from airflow.models.asset import AssetActive, AssetModel
        from airflow.settings import Session

        session_factory = Session
        if session_factory is None:
            return

        with session_factory() as session:
            session.execute(
                delete(AssetActive).where(or_(AssetActive.name == asset.name, AssetActive.uri == asset.uri))
            )
            session.execute(
                delete(AssetModel).where(or_(AssetModel.name == asset.name, AssetModel.uri == asset.uri))
            )
            session.commit()
    except ImportError:
        pass


def keep_columns(example):
    return {"sentence": example["sentence1"], "label": example["label"]}


def add_row_marker(example):
    return {"row_marker": "durable-retry-test"}


class FailAfterSuccessfulFirstPushOperator(PushToHubOperator):
    """Raise after the first successful commit; retries must reuse that commit."""

    def execute(self, context):
        result = super().execute(context)
        if context["ti"].try_number == 1:
            raise RuntimeError("intentional post-publication failure for retry test")
        return result


with DAG(dag_id="huggingface_system_write", schedule="@once", tags=["huggingface", "system-test"]) as dag:

    @task
    def create_private_repo():
        if not HUB_ORG:
            raise RuntimeError("Set HF_TEST_HUB_ORG to a dedicated Hub test organization")
        HuggingFaceHook(CONN_ID).get_conn().create_repo(
            REPO_ID, repo_type="dataset", private=True, exist_ok=False
        )
        _activate_asset(PUBLISHED_ASSET)
        return REPO_ID

    load = LoadDatasetOperator(
        task_id="load_public_dataset",
        path="nyu-mll/glue",
        name="mrpc",
        split="train[:8]",
        revision=REVISION,
        output_uri="file:///tmp/hf-provider-system/write-source.json",
        huggingface_conn_id=None,
    )
    # [START huggingface_map_and_publish_example]
    mapped = MapDatasetOperator(
        task_id="map_public_dataset",
        input_uri=load.output,
        output_uri="file:///tmp/hf-provider-system/write-mapped.json",
        transform=keep_columns,
    )
    publish = PushToHubOperator(
        task_id="publish_private_dataset",
        input_uri=mapped.output,
        repo_id=REPO_ID,
        private=True,
        huggingface_conn_id=CONN_ID,
    )
    # [END huggingface_map_and_publish_example]

    @task(inlets=[PUBLISHED_ASSET])
    def verify_xcom_matches_hub_head(publication):
        context = get_current_context()
        event = context["inlet_events"][PUBLISHED_ASSET][-1]
        assert publication["commit_sha"]
        assert event.extra["commit_sha"] == publication["commit_sha"]
        head = HuggingFaceHook(CONN_ID).get_dataset_info(REPO_ID).sha
        assert publication["commit_sha"] == head
        assert event.extra["commit_url"].endswith(head)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_private_repo(created_repo):
        try:
            if created_repo:
                HuggingFaceHook(CONN_ID).get_conn().delete_repo(created_repo, repo_type="dataset")
        finally:
            _deactivate_asset(PUBLISHED_ASSET)

    created = create_private_repo()
    verified = verify_xcom_matches_hub_head(publish.output)
    cleanup = delete_private_repo(created)
    created >> load >> mapped >> publish >> verified
    [created, publish, verified] >> cleanup

    if Version(airflow.__version__) >= Version("3.3"):

        @task
        def create_durable_repo():
            if not HUB_ORG:
                raise RuntimeError("Set HF_TEST_HUB_ORG to a dedicated Hub test organization")
            HuggingFaceHook(CONN_ID).get_conn().create_repo(
                REPO_DURABLE_ID, repo_type="dataset", private=True, exist_ok=False
            )
            # Activate durable asset for publish_then_fail_once outlet validation
            _activate_asset(DURABLE_ASSET)
            return REPO_DURABLE_ID

        durable_mapped = MapDatasetOperator(
            task_id="map_for_durable_retry",
            input_uri=load.output,
            output_uri="file:///tmp/hf-provider-system/durable-mapped.json",
            transform=add_row_marker,
        )
        durable_publish = FailAfterSuccessfulFirstPushOperator(
            task_id="publish_then_fail_once",
            input_uri=durable_mapped.output,
            repo_id=REPO_DURABLE_ID,
            private=True,
            retries=1,
            retry_delay=0,
            huggingface_conn_id=CONN_ID,
        )

        @task
        def verify_single_data_commit(mapped_ref, publication):
            fingerprint = mapped_ref["fingerprint"]
            marker_prefix = f"airflow-artifact-sha256:{fingerprint}:target:"
            commits = (
                HuggingFaceHook(CONN_ID).get_conn().list_repo_commits(REPO_DURABLE_ID, repo_type="dataset")
            )
            matches = [
                commit
                for commit in commits
                if marker_prefix in (getattr(commit, "title", "") or "")
                and HuggingFaceHook._commit_is_complete(commit)
            ]
            assert len(matches) == 1
            assert publication["commit_sha"] == matches[0].commit_id

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def delete_durable_repo(created_repo):
            try:
                if created_repo:
                    HuggingFaceHook(CONN_ID).get_conn().delete_repo(created_repo, repo_type="dataset")
            finally:
                _deactivate_asset(DURABLE_ASSET)

        durable_created = create_durable_repo()
        durable_verified = verify_single_data_commit(durable_mapped.output, durable_publish.output)
        durable_cleanup = delete_durable_repo(durable_created)
        durable_created >> load >> durable_mapped >> durable_publish >> durable_verified
        [durable_created, durable_publish, durable_verified] >> durable_cleanup

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

# Needed to run the example DAG with pytest (see: tests/system/README.md)
from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
