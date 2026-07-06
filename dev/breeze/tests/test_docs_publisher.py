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

from pathlib import Path

import pytest

from airflow_breeze.utils import docs_publisher
from airflow_breeze.utils.docs_publisher import DocsPublisher


@pytest.fixture
def generated_path(tmp_path, monkeypatch):
    generated = tmp_path / "generated"
    monkeypatch.setattr(docs_publisher, "GENERATED_PATH", generated)
    return generated


@pytest.fixture
def airflow_site_dir(tmp_path):
    site_dir = tmp_path / "airflow-site"
    site_dir.mkdir()
    return str(site_dir)


def _stage_java_sdk_docs(generated_path: Path, version: str | None) -> None:
    build_dir = generated_path / "_build" / "docs" / "java-sdk"
    (build_dir / "stable").mkdir(parents=True)
    (build_dir / "stable" / "index.html").write_text("docs")
    if version:
        (build_dir / "stable.txt").write_text(version + "\n")


def test_publish_skips_package_without_staged_docs_before_resolving_version(
    generated_path, airflow_site_dir, monkeypatch
):
    def fail_version_resolution():
        raise SystemExit("Version must not be resolved when the docs were not staged")

    monkeypatch.setattr(docs_publisher, "get_java_sdk_version", fail_version_resolution)
    publisher = DocsPublisher(package_name="java-sdk", output=None, verbose=False)

    return_code, message = publisher.publish(override_versioned=False, airflow_site_dir=airflow_site_dir)

    assert return_code == 0
    assert message == "Skipping java-sdk: Build directory does not exist"


def test_publish_preserves_existing_docs_when_build_dir_missing(
    generated_path, airflow_site_dir, monkeypatch
):
    # A previous version is already published to airflow-site, but nothing is staged
    # for this run (build dir missing). Publishing with --override-versioned must not
    # delete the existing docs when there is nothing to copy in their place.
    monkeypatch.setattr(docs_publisher, "get_java_sdk_version", lambda: "1.2.3")
    existing = Path(airflow_site_dir) / "docs-archive" / "java-sdk" / "1.2.3"
    existing.mkdir(parents=True)
    (existing / "index.html").write_text("previously published")

    publisher = DocsPublisher(package_name="java-sdk", output=None, verbose=False)
    return_code, message = publisher.publish(override_versioned=True, airflow_site_dir=airflow_site_dir)

    assert (return_code, message) == (0, "Skipping java-sdk: Build directory does not exist")
    assert (existing / "index.html").read_text() == "previously published"


def test_publish_java_sdk_docs_with_staged_stable_txt(generated_path, airflow_site_dir):
    _stage_java_sdk_docs(generated_path, version="1.2.3")
    publisher = DocsPublisher(package_name="java-sdk", output=None, verbose=False)

    return_code, message = publisher.publish(override_versioned=False, airflow_site_dir=airflow_site_dir)

    assert (return_code, message) == (0, "Docs published: java-sdk")
    java_sdk_archive = Path(airflow_site_dir) / "docs-archive" / "java-sdk"
    assert (java_sdk_archive / "1.2.3" / "index.html").read_text() == "docs"
    assert (java_sdk_archive / "stable.txt").read_text() == "1.2.3\n"


def test_publish_java_sdk_version_falls_back_to_gradle_properties(
    generated_path, airflow_site_dir, monkeypatch
):
    monkeypatch.setattr(docs_publisher, "get_java_sdk_version", lambda: "9.9.9")
    _stage_java_sdk_docs(generated_path, version=None)
    publisher = DocsPublisher(package_name="java-sdk", output=None, verbose=False)

    return_code, message = publisher.publish(override_versioned=False, airflow_site_dir=airflow_site_dir)

    assert (return_code, message) == (0, "Docs published: java-sdk")
    java_sdk_archive = Path(airflow_site_dir) / "docs-archive" / "java-sdk"
    assert (java_sdk_archive / "9.9.9" / "index.html").read_text() == "docs"
    assert (java_sdk_archive / "stable.txt").read_text() == "9.9.9\n"
