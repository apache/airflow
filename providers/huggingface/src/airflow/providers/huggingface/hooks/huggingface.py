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
"""Connection and client helpers for Hugging Face services."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from huggingface_hub import DatasetInfo, GitCommitInfo, HfApi, constants
from huggingface_hub.utils import (
    RepositoryNotFoundError,
    RevisionNotFoundError,
    build_hf_headers,
    get_async_session,
    hf_raise_for_status,
    parse_datetime,
)

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import BaseHook


class HuggingFaceHook(BaseHook):
    """
    Access Hugging Face Datasets and Hub APIs using an Airflow connection.

    If ``huggingface_default`` is absent, or no connection ID is configured,
    huggingface_hub may use ``HF_TOKEN`` or a cached worker login. A missing
    explicitly named connection raises Airflow's connection-not-found
    exception. An existing connection with no password disables SDK
    authentication fallback.
    """

    conn_name_attr = "huggingface_conn_id"
    default_conn_name = "huggingface_default"
    conn_type = "huggingface"
    hook_name = "Hugging Face"

    def __init__(self, huggingface_conn_id: str | None = default_conn_name) -> None:
        super().__init__()
        self.huggingface_conn_id = huggingface_conn_id
        self._allow_ambient_fallback = huggingface_conn_id in (None, self.default_conn_name)
        self._client: HfApi | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """
        Return custom field behaviour for Airflow 3.0 and 3.1.

        Airflow 3.2+ reads ``ui-field-behaviour`` from provider.yaml instead and
        does not call this method.
        """
        return {
            "hidden_fields": ["host", "schema", "port", "login"],
            "relabeling": {"password": "Access token"},
        }

    def get_token(self) -> str | bool | None:
        if not self.huggingface_conn_id:
            return None
        try:
            connection = self.get_connection(self.huggingface_conn_id)
        except AirflowNotFoundException:
            if self._allow_ambient_fallback:
                return None
            raise
        return connection.password or False

    def get_conn(self) -> HfApi:
        if self._client is None:
            self._client = HfApi(token=self.get_token())
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.get_conn().whoami()
            return True, "Connection successfully tested"
        except Exception as exc:
            return False, str(exc)

    def load_dataset(self, path: str, **kwargs: Any) -> Any:
        from datasets import load_dataset

        token = self.get_token()
        if token is not None and "token" not in kwargs:
            kwargs["token"] = token
        return load_dataset(path, **kwargs)

    def get_dataset_info(self, repo_id: str, revision: str | None = None) -> Any:
        return self.get_conn().dataset_info(repo_id, revision=revision)

    async def _aget_token(self) -> str | bool | None:
        """Async token resolution mirroring get_token without blocking the event loop."""
        if not self.huggingface_conn_id:
            return None
        try:
            connection = await get_async_connection(self.huggingface_conn_id, hook=self)
        except AirflowNotFoundException:
            if self._allow_ambient_fallback:
                return None
            raise
        return connection.password or False

    @staticmethod
    async def _aget(client: Any, url: str, *, headers: dict[str, str], **kwargs: Any) -> Any:
        """GET a Hub endpoint on an open client and raise on HTTP errors."""
        response = await client.get(url, headers=headers, **kwargs)
        hf_raise_for_status(response)
        return response

    async def aget_dataset_info(self, repo_id: str, revision: str | None = None) -> Any:
        """Async variant of :meth:`get_dataset_info`; reads the Hub over HTTP."""
        path = f"{constants.ENDPOINT}/api/datasets/{repo_id}"
        if revision is not None:
            path = f"{path}/revision/{quote(revision, safe='')}"
        headers = build_hf_headers(token=await self._aget_token())
        async with get_async_session() as client:
            response = await self._aget(client, path, headers=headers)
        return DatasetInfo(**response.json())

    async def afind_commit_by_marker(
        self,
        repo_id: str,
        marker: str,
        *,
        revision: str | None = None,
        include_pull_requests: bool = False,
    ) -> Any | None:
        """Async variant of :meth:`find_commit_by_marker`; scans the latest commit per requested ref."""
        headers = build_hf_headers(token=await self._aget_token())
        revisions = [revision] if revision else ["main"]
        async with get_async_session() as client:
            if include_pull_requests:
                try:
                    refs_response = await self._aget(
                        client,
                        f"{constants.ENDPOINT}/api/datasets/{repo_id}/refs",
                        headers=headers,
                        params={"include_prs": "1"},
                    )
                except RepositoryNotFoundError:
                    return None
                refs = refs_response.json()
                revisions.extend(item["ref"] for item in refs.get("pullRequests", []) or [])

            seen: set[str] = set()
            for ref in revisions:
                if ref in seen:
                    continue
                seen.add(ref)
                try:
                    commits_response = await self._aget(
                        client,
                        f"{constants.ENDPOINT}/api/datasets/{repo_id}/commits/{quote(ref, safe='')}",
                        headers=headers,
                    )
                except RepositoryNotFoundError:
                    return None
                except RevisionNotFoundError:
                    continue
                items = commits_response.json()
                if not items:
                    continue
                item = items[0]  # Hub returns commits newest first.
                commit = GitCommitInfo(
                    commit_id=item["id"],
                    authors=[author["user"] for author in item["authors"]],
                    created_at=parse_datetime(item["date"]),
                    title=item["title"],
                    message=item["message"],
                    formatted_title=None,
                    formatted_message=None,
                )
                if self._marker_matches(commit, marker):
                    return commit
        return None

    @staticmethod
    def _commit_is_complete(commit: Any) -> bool:
        """Reject intermediate commits from a Hub upload split into parts."""
        title = getattr(commit, "title", "") or ""
        return re.search(r"\(part\b.*\)\s*$", title, re.IGNORECASE) is None

    @staticmethod
    def _marker_matches(commit: Any, marker: str) -> bool:
        """Whether the commit carries the marker and has completed in full."""
        # GitCommitInfo carries title/message/commit_id; CommitInfo carries
        # commit_message/commit_description/oid.
        fields = (
            getattr(commit, "title", ""),
            getattr(commit, "message", ""),
            getattr(commit, "commit_message", ""),
            getattr(commit, "commit_description", ""),
        )
        if not any(marker in (field or "") for field in fields):
            return False
        return HuggingFaceHook._commit_is_complete(commit)

    def find_commit_by_marker(
        self,
        repo_id: str,
        marker: str,
        *,
        revision: str | None = None,
        include_pull_requests: bool = False,
    ) -> Any | None:
        """Find the latest complete commit matching a marker on the requested ref."""
        api = self.get_conn()
        revisions = [revision] if revision else ["main"]
        if include_pull_requests:
            try:
                refs = api.list_repo_refs(repo_id, repo_type="dataset", include_pull_requests=True)
            except RepositoryNotFoundError:
                return None
            revisions.extend(ref.ref for ref in getattr(refs, "pull_requests", []) or [])

        seen: set[str] = set()
        for ref in revisions:
            if ref in seen:
                continue
            seen.add(ref)
            try:
                commits = api.list_repo_commits(repo_id, repo_type="dataset", revision=ref)
            except RepositoryNotFoundError:
                return None
            except RevisionNotFoundError:
                continue
            # Hub returns commits newest first. A historical match is stale if
            # a later publication has advanced this ref since the push attempt.
            commit = commits[0] if commits else None
            if commit is not None and self._marker_matches(commit, marker):
                return commit
        return None

    def push_to_hub(self, dataset: Any, repo_id: str, **kwargs: Any) -> Any:
        token = self.get_token()
        if token is not None and "token" not in kwargs:
            kwargs["token"] = token
        return dataset.push_to_hub(repo_id, **kwargs)
