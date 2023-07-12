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
from __future__ import annotations

from airflow_breeze.commands.main_command import main
from airflow_breeze.utils.path_utils import (
    create_directories_and_files,
    find_airflow_sources_root_to_operate_on,
)

from airflow_breeze.configure_rich_click import click  # isort: skip # noqa

find_airflow_sources_root_to_operate_on()
create_directories_and_files()

from airflow_breeze.commands import developer_commands  # noqa
from airflow_breeze.commands.ci_commands import ci_group  # noqa
from airflow_breeze.commands.ci_image_commands import ci_image  # noqa
from airflow_breeze.commands.kubernetes_commands import kubernetes_group  # noqa
from airflow_breeze.commands.production_image_commands import prod_image  # noqa
from airflow_breeze.commands.release_management_commands import release_management  # noqa
from airflow_breeze.commands.minor_release_command import create_minor_version_branch  # noqa
from airflow_breeze.commands.release_command import airflow_release  # noqa
from airflow_breeze.commands.release_candidate_command import release_management  # noqa
from airflow_breeze.commands.sbom_commands import sbom  # noqa
from airflow_breeze.commands.setup_commands import setup  # noqa
from airflow_breeze.commands.testing_commands import group_for_testing  # noqa

main.add_command(group_for_testing)
main.add_command(kubernetes_group)
main.add_command(ci_group)
main.add_command(ci_image)
main.add_command(prod_image)
main.add_command(setup)
main.add_command(release_management)
main.add_command(sbom)

if __name__ == "__main__":
    main()
