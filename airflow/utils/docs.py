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

import sys

if sys.version_info >= (3, 10):
    from importlib import metadata
else:
    import importlib_metadata as metadata  # type: ignore[no-redef]


def get_docs_url(page: str | None = None) -> str:
    """Prepare link to Airflow documentation."""
    from airflow.version import version

    if any(suffix in version for suffix in ["dev", "a", "b"]):
        result = (
            "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/stable/"
        )
    else:
        result = f"https://airflow.apache.org/docs/apache-airflow/{version}/"
    if page:
        result = result + page
    return result


def get_project_url_from_metadata(provider_name: str):
    """Return the Project-URL from metadata."""
    return metadata.metadata(provider_name).get_all("Project-URL")


def get_doc_url_for_provider(provider_name: str, provider_version: str) -> str:
    """Prepare link to Airflow Provider documentation."""
    try:
        from urllib.parse import urlparse

        metadata_items = get_project_url_from_metadata(provider_name)
        if isinstance(metadata_items, str):
            metadata_items = [metadata_items]
        if metadata_items:
            for item in metadata_items:
                if item.lower().startswith("documentation"):
                    _, _, url = item.partition(",")
                    parsed_url = urlparse(url)
                    if url and (parsed_url.scheme in ("http", "https") and bool(parsed_url.netloc)):
                        return url.strip()
    except metadata.PackageNotFoundError:
        pass
    # Fallback if provider is apache one
    if provider_name.startswith("apache-airflow"):
        return f"https://airflow.apache.org/docs/{provider_name}/{provider_version}/"
    return "https://airflow.apache.org/docs/apache-airflow-providers/index.html#creating-your-own-providers"
