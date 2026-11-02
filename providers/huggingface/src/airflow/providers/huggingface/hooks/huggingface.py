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

from typing import Any

from datasets import load_dataset
from huggingface_hub import HfApi
from huggingface_hub.utils import RepositoryNotFoundError, RevisionNotFoundError

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.common.compat.sdk import BaseHook


class HuggingFaceHook(BaseHook):
    """Access Hugging Face Datasets and Hub APIs using an Airflow connection.

    If the connection is absent, huggingface_hub may use ``HF_TOKEN`` or a
    cached worker login.
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

    def get_token(self) -> str | None:
        if not self.huggingface_conn_id:
            return None
        try:
            connection = self.get_connection(self.huggingface_conn_id)
        except AirflowNotFoundException:
            if self._allow_ambient_fallback:
                return None
            raise
        return connection.password or None

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
        token = self.get_token()
        if token and "token" not in kwargs:
            kwargs["token"] = token
        return load_dataset(path, **kwargs)

    def get_dataset_info(self, repo_id: str, revision: str | None = None) -> Any:
        return self.get_conn().dataset_info(repo_id, revision=revision)

    def find_publication_commit(
        self,
        repo_id: str,
        fingerprint: str,
        *,
        marker: str,
        revision: str | None = None,
        include_pull_requests: bool = False,
    ) -> Any | None:
        """Find a completed artifact publication after a worker crash."""
        return self.find_commit_by_marker(
            repo_id,
            marker,
            revision=revision,
            include_pull_requests=include_pull_requests,
        )

    @staticmethod
    def _commit_is_complete(commit: Any) -> bool:
        """Reject intermediate commits from a Hub upload split into parts."""
        import re

        title = getattr(commit, "title", "") or ""
        part = re.search(r"\(part\s+(\d+)(?:\s*(?:-of-|of|-)\s*(\d+))?\)\s*$", title, re.IGNORECASE)
        if part is None:
            return not re.search(r"\(part\b.*\)\s*$", title, re.IGNORECASE)
        # Some upload clients only report "part N" without the total. That
        # cannot prove that this is the final part, so do not recover from it.
        return part.group(2) is not None and part.group(1) == part.group(2)

    def find_commit_by_marker(
        self,
        repo_id: str,
        marker: str,
        *,
        revision: str | None = None,
        include_pull_requests: bool = False,
    ) -> Any | None:
        """Find the final upload commit matching a marker on the requested ref."""
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
            for commit in commits:
                # GitCommitInfo carries title/message/commit_id; CommitInfo
                # carries commit_message/commit_description/oid.
                fields = (
                    getattr(commit, "title", ""),
                    getattr(commit, "message", ""),
                    getattr(commit, "commit_message", ""),
                    getattr(commit, "commit_description", ""),
                )
                if any(marker in (field or "") for field in fields) and self._commit_is_complete(commit):
                    return commit
        return None

    def push_to_hub(self, dataset: Any, repo_id: str, **kwargs: Any) -> Any:
        token = self.get_token()
        if token and "token" not in kwargs:
            kwargs["token"] = token
        return dataset.push_to_hub(repo_id, **kwargs)
