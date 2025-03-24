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

import os

iml_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<module type="PYTHON_MODULE" version="4">
  <component name="NewModuleRootManager">
    <content url="file://$MODULE_DIR$">
        {SOURCE_ROOT_MODULE_PATH}
        <excludeFolder url="file://$MODULE_DIR$/.build" />
        <excludeFolder url="file://$MODULE_DIR$/.kube" />
        <excludeFolder url="file://$MODULE_DIR$/.venv" />
    </content>
    <orderEntry type="jdk" jdkName="Python 3.9 (airflow)" jdkType="Python SDK" />
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

source_root_modules: list[str] = ["airflow-core", "airflow-ctl", "dev/breeze", "task-sdk"]

all_module_paths: list[str] = []

ROOT_DIR: str = "./providers"


def setup_idea():
    # Providers discovery
    for dirpath, _, filenames in os.walk(ROOT_DIR):
        if "pyproject.toml" in filenames:
            relative_path = os.path.relpath(dirpath, ROOT_DIR)
            source_root_modules.append(f"providers/{relative_path}")

    source_root_modules.sort()
    for module in source_root_modules:
        all_module_paths.append(source_root_module_patter.format(path=f"{module}/src", status="false"))
        all_module_paths.append(source_root_module_patter.format(path=f"{module}/test", status="true"))

    source_root_module_path: str = "\n\t\t".join(all_module_paths)

    base_source_root_xml: str = iml_xml_template.format(SOURCE_ROOT_MODULE_PATH=source_root_module_path)

    with open(".idea/airflow.iml", "w") as file:
        file.write(base_source_root_xml)

    with open(".idea/modules.xml", "w") as file:
        file.write(module_xml_template)


if __name__ == "__main__":
    user_input = input(
        "This script will overwrites the .idea/airflow.iml and .idea/modules.xml files. Press Enter Y/N to continue: "
    )
    if user_input.lower() == "y":
        setup_idea()
        print("Updated airflow.iml and modules.xml files, Now restart the PyCharm/IntelliJ IDEA")
    else:
        print("Not updating airflow.iml and modules.xml files")
        exit(0)
