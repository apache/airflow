#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

from pathlib import Path

from rich import print
from rich.prompt import Confirm

iml_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<module type="PYTHON_MODULE" version="4">
  <component name="NewModuleRootManager">
     <content url="file://$MODULE_DIR$">
        {SOURCE_ROOT_MODULE_PATH}
        <excludeFolder url="file://$MODULE_DIR$/.build" />
        <excludeFolder url="file://$MODULE_DIR$/.kube" />
        <excludeFolder url="file://$MODULE_DIR$/.venv" />
        <excludeFolder url="file://$MODULE_DIR$/dist" />
        <excludeFolder url="file://$MODULE_DIR$/files" />
        <excludeFolder url="file://$MODULE_DIR$/logs" />
        <excludeFolder url="file://$MODULE_DIR$/out" />
        <excludeFolder url="file://$MODULE_DIR$/tmp" />
        <excludeFolder url="file://$MODULE_DIR$/airflow-core/dist" />
        <excludeFolder url="file://$MODULE_DIR$/generated/" />
        <excludeFolder url="file://$MODULE_DIR$/dev/breeze/.venv" />
    </content>
    <orderEntry type="jdk" jdkName="Python 3.10 (airflow)" jdkType="Python SDK" />
    <orderEntry type="sourceFolder" forTests="false" />
  </component>
  <component name="PyDocumentationSettings">
    <option name="format" value="PLAIN" />
    <option name="myDocStringFormat" value="Plain" />
  </component>
  <component name="TemplatesService">
    <option name="TEMPLATE_FOLDERS">
      <list>
        <option value="$MODULE_DIR$/chart/templates" />
      </list>
    </option>
  </component>
  <component name="TestRunnerService">
    <option name="PROJECT_TEST_RUNNER" value="py.test" />
  </component>
</module>"""

module_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<project version="4">
  <component name="ProjectModuleManager">
    <modules>
      <module fileurl="file://$PROJECT_DIR$/.idea/airflow.iml" filepath="$PROJECT_DIR$/.idea/airflow.iml" />
    </modules>
  </component>
</project>"""

source_root_module_patter: str = '<sourceFolder url="file://$MODULE_DIR$/{path}" isTestSource="{status}" />'

source_root_modules: list[str] = [
    "airflow-core",
    "airflow-ctl",
    "task-sdk",
    "devel-common",
    "dev",
    "dev/breeze",
    "docker-tests",
    "kubernetes-tests",
    "helm-tests",
    "task-sdk-integration-tests",
]

all_module_paths: list[str] = []

ROOT_AIRFLOW_FOLDER_PATH = Path(__file__).parent
IDEA_FOLDER_PATH = ROOT_AIRFLOW_FOLDER_PATH / ".idea"
AIRFLOW_IML_FILE = IDEA_FOLDER_PATH / "airflow.iml"
MODULES_XML_FILE = IDEA_FOLDER_PATH / "modules.xml"


def setup_idea():
    # Providers discovery
    for pyproject_toml_file in ROOT_AIRFLOW_FOLDER_PATH.rglob("providers/**/pyproject.toml"):
        relative_path = pyproject_toml_file.relative_to(ROOT_AIRFLOW_FOLDER_PATH).parent.as_posix()
        source_root_modules.append(f"{relative_path}")
    # Shared discovery
    for pyproject_toml_file in ROOT_AIRFLOW_FOLDER_PATH.rglob("shared/*/pyproject.toml"):
        relative_path = pyproject_toml_file.relative_to(ROOT_AIRFLOW_FOLDER_PATH).parent.as_posix()
        source_root_modules.append(f"{relative_path}")

    source_root_modules.sort()
    for module in source_root_modules:
        print(f"[green]Adding[/] module: [blue]{module}[/]")
        if (ROOT_AIRFLOW_FOLDER_PATH / module / "src").exists():
            all_module_paths.append(source_root_module_patter.format(path=f"{module}/src", status="false"))
        if (ROOT_AIRFLOW_FOLDER_PATH / module / "tests").exists():
            all_module_paths.append(source_root_module_patter.format(path=f"{module}/tests", status="true"))
        if module == "dev":
            all_module_paths.append(source_root_module_patter.format(path=f"{module}", status="false"))
    source_root_module_path = "\n\t\t".join(all_module_paths)

    base_source_root_xml = iml_xml_template.format(SOURCE_ROOT_MODULE_PATH=source_root_module_path)

    IDEA_FOLDER_PATH.mkdir(exist_ok=True)
    AIRFLOW_IML_FILE.write_text(base_source_root_xml)
    MODULES_XML_FILE.write_text(module_xml_template)


if __name__ == "__main__":
    print("\n[yellow]Warning!!![/] This script will update the PyCharm/IntelliJ IDEA configuration files:\n")
    print(f"* {AIRFLOW_IML_FILE}")
    print(f"* {MODULES_XML_FILE}\n")
    should_continue = Confirm.ask("Overwrite the files?")
    if should_continue:
        print()
        setup_idea()
        print("\n[green]Success\n")
        print(
            f"Updated {AIRFLOW_IML_FILE} and {MODULES_XML_FILE} files. "
            f"Now restart the PyCharm/IntelliJ IDEA\n"
        )
    else:
        print("[yellow]Skipped\n")
        print(f"Not updated {AIRFLOW_IML_FILE} and {MODULES_XML_FILE} files\n")
