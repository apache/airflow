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

import pytest
from sync_java_sdk_docs import (
    DOKKA_MODULE,
    JAVA_RST,
    SDK_README,
    VERSION_PLACEHOLDER,
    check_versions,
    extract_snippet,
    render_snippet,
)

RST_WITH_SNIPPET = """
Declare the Dag in ``HelloDag.java``:

.. BEGIN SHARED SNIPPET hello-dag

.. code-block:: java

    @Airflow.Dag(id = "java_hello", queue = "java")
    public class HelloDag {

      @Airflow.Task
      public long extract() {
        return 42L;
      }
    }

.. END SHARED SNIPPET hello-dag

Build the bundle:
"""

HELLO_DAG = """@Airflow.Dag(id = "java_hello", queue = "java")
public class HelloDag {

  @Airflow.Task
  public long extract() {
    return 42L;
  }
}"""


def test_extracts_the_guides_snippet_without_its_directive_or_indent():
    assert extract_snippet(RST_WITH_SNIPPET, "hello-dag") == HELLO_DAG


def test_rejects_a_snippet_the_guide_does_not_mark():
    with pytest.raises(ValueError, match="hello-main"):
        extract_snippet(RST_WITH_SNIPPET, "hello-main")


def test_renders_the_snippet_as_a_fenced_block():
    rendered = "".join(render_snippet(HELLO_DAG))

    assert rendered == f"\n```java\n{HELLO_DAG}\n```\n\n"


def _contents(rst_version: str, readme_version: str, module_version: str) -> dict[str, str]:
    return {
        JAVA_RST: f"""
    airflowJavaSdkVersion={rst_version}

      <airflow.sdk.version>{rst_version}</airflow.sdk.version>
""",
        SDK_README: f"""
    id("org.apache.airflow.sdk") version "{readme_version}"
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:{readme_version}")
    implementation("org.apache.airflow:airflow-sdk:{readme_version}")
""",
        DOKKA_MODULE: f"""
    id("org.apache.airflow.sdk") version "{module_version}"
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:{module_version}")
    implementation("org.apache.airflow:airflow-sdk:{module_version}")
""",
    }


def test_accepts_the_placeholder_everywhere():
    assert check_versions(_contents(*[VERSION_PLACEHOLDER] * 3)) == []


@pytest.mark.parametrize(
    "rst, readme, module, pinned",
    [
        pytest.param("1.0.0", VERSION_PLACEHOLDER, VERSION_PLACEHOLDER, JAVA_RST, id="guide-pins"),
        pytest.param(VERSION_PLACEHOLDER, "1.0.0-beta2", VERSION_PLACEHOLDER, SDK_README, id="readme-pins"),
        pytest.param(VERSION_PLACEHOLDER, VERSION_PLACEHOLDER, "1.0.0-beta1", DOKKA_MODULE, id="dokka-pins"),
    ],
)
def test_reports_a_page_that_names_a_release(rst: str, readme: str, module: str, pinned: str):
    errors = check_versions(_contents(rst, readme, module))

    assert len(errors) == 1
    assert errors[0].startswith(f"{pinned}: the build snippets install ")


def test_reports_a_file_with_no_version():
    contents = _contents(*[VERSION_PLACEHOLDER] * 3)
    contents[DOKKA_MODULE] = "Write Apache Airflow Dags and tasks in Java.\n"

    assert check_versions(contents) == [f"{DOKKA_MODULE}: no Java SDK version found"]
