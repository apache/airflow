#!/usr/bin/env python
#
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
"""
Module to covert Airflow configs in config.yml to default_airflow.cfg file
"""

import os

import yaml

FILE_HEADER = """#
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


# This is the template for Airflow's default configuration. When Airflow is
# imported, it looks for a configuration file at $AIRFLOW_HOME/airflow.cfg. If
# it doesn't exist, Airflow uses this template to generate it by replacing
# variables in curly braces with their global values from configuration.py.

# Users should not modify this file; they should customize the generated
# airflow.cfg instead.


# ----------------------- TEMPLATE BEGINS HERE -----------------------
"""


def read_default_config_yaml(file_path: str) -> dict:
    """
    Read Airflow configs from YAML file

    :param file_path: Full path to config.yaml

    :return: Python dictionary containing configs & their info
    """

    with open(file_path) as config_file:
        return yaml.safe_load(config_file)


def write_config(yaml_config_file_path: str, default_cfg_file_path: str):
    """
    Write config to default_airflow.cfg file

    :param yaml_config_file_path: Full path to config.yaml
    :param default_cfg_file_path: Full path to default_airflow.cfg
    """
    with open(default_cfg_file_path, 'w') as configfile:
        configfile.writelines(FILE_HEADER)
        config_yaml = read_default_config_yaml(yaml_config_file_path)

        for section in config_yaml:  # pylint: disable=too-many-nested-blocks
            section_name = section["name"]
            configfile.write(f"\n[{section_name}]\n")

            section_description = None
            if section["description"] is not None:
                section_description = list(
                    filter(lambda x: (x is not None) or x != "", section["description"].splitlines()))
            if section_description:
                configfile.write("\n")
                for single_line_desc in section_description:
                    if single_line_desc == "":
                        configfile.write(f"#\n")
                    else:
                        configfile.write(f"# {single_line_desc}\n")

            for idx, option in enumerate(section["options"]):
                option_description = None
                if option["description"] is not None:
                    option_description = list(
                        filter(lambda x: x is not None, option["description"].splitlines()))

                if option_description:
                    if idx != 0:
                        configfile.write("\n")
                    for single_line_desc in option_description:
                        if single_line_desc == "":
                            configfile.write(f"#\n")
                        else:
                            configfile.write(f"# {single_line_desc}\n")

                if option["example"]:
                    if not str(option["name"]).endswith("_template"):
                        option["example"] = option["example"].replace("{", "{{").replace("}", "}}")
                    configfile.write("# Example: {} = {}\n".format(option["name"], option["example"]))

                configfile.write("{}{} ={}\n".format(
                    "# " if option["default"] is None else "",
                    option["name"],
                    " " + option["default"] if option["default"] else "")
                )


if __name__ == '__main__':
    airflow_config_dir = os.path.join(
        os.path.dirname(__file__), "../../airflow/config_templates")
    airflow_default_config_path = os.path.join(airflow_config_dir, "default_airflow.cfg")
    airflow_config_yaml_file_path = os.path.join(airflow_config_dir, "config.yml")

    write_config(
        yaml_config_file_path=airflow_config_yaml_file_path,
        default_cfg_file_path=airflow_default_config_path
    )
