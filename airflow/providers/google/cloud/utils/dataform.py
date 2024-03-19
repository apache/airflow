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

import json
from enum import Enum
from typing import Mapping

from airflow.providers.google.cloud.operators.dataform import (
    DataformInstallNpmPackagesOperator,
    DataformMakeDirectoryOperator,
    DataformWriteFileOperator,
)


class DataformLocations(str, Enum):
    """Enum for storing available locations for resources in Dataform."""

    US = "US"
    EUROPE = "EU"


def make_initialization_workspace_flow(
    project_id: str,
    region: str,
    repository_id: str,
    workspace_id: str,
    dataform_schema_name: str = "dataform",
    package_name: str | None = None,
    without_installation: bool = False,
) -> tuple:
    """
    Create flow which simulates the initialization of the default project.

    :param project_id: Required. The ID of the Google Cloud project where workspace located.
    :param region: Required. The ID of the Google Cloud region where workspace located.
    :param repository_id: Required. The ID of the Dataform repository where workspace located.
    :param workspace_id: Required. The ID of the Dataform workspace which requires initialization.
    :param dataform_schema_name: Name of the schema.
    :param package_name: Name of the package. If value is not provided then workspace_id will be used.
    :param without_installation: Defines should installation of npm packages be added to flow.
    """
    make_definitions_directory = DataformMakeDirectoryOperator(
        task_id="make-definitions-directory",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        directory_path="definitions",
    )

    first_view_content = b"""
        -- This is an example SQLX file to help you learn the basics of Dataform.
        -- Visit https://cloud.google.com/dataform/docs/how-to for more information on how to configure
        --   your SQL workflow.

        -- You can delete this file, then commit and push your changes to your repository when you are ready.

        -- Config blocks allow you to configure, document, and test your data assets.
        config {
          type: "view", // Creates a view in BigQuery. Try changing to "table" instead.
          columns: {
            test: "A description for the test column", // Column descriptions are pushed to BigQuery.
          }
        }

        -- The rest of a SQLX file contains your SELECT statement used to create the table.

        SELECT 1 as test
    """
    make_first_view_file = DataformWriteFileOperator(
        task_id="write-first-view",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        filepath="definitions/first_view.sqlx",
        contents=first_view_content,
    )

    second_view_content = b"""
        config { type: "view" }

        -- Use the ref() function to manage dependencies.
        -- Learn more about ref() and other built in functions
        --  here: https://cloud.google.com/dataform/docs/dataform-core

        SELECT test from ${ref("first_view")}
    """
    make_second_view_file = DataformWriteFileOperator(
        task_id="write-second-view",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        filepath="definitions/second_view.sqlx",
        contents=second_view_content,
    )

    make_includes_directory = DataformMakeDirectoryOperator(
        task_id="make-includes-directory",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        directory_path="includes",
    )

    gitignore_contents = b"""
    node_modules/
    """
    make_gitignore_file = DataformWriteFileOperator(
        task_id="write-gitignore-file",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        filepath=".gitignore",
        contents=gitignore_contents,
    )

    default_location: str = define_default_location(region).value
    dataform_config_content = json.dumps(
        {
            "defaultSchema": dataform_schema_name,
            "assertionSchema": "dataform_assertions",
            "warehouse": "bigquery",
            "defaultDatabase": project_id,
            "defaultLocation": default_location,
        },
        indent=4,
    ).encode()
    make_dataform_config_file = DataformWriteFileOperator(
        task_id="write-dataform-config-file",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        filepath="dataform.json",
        contents=dataform_config_content,
    )

    package_name = package_name or workspace_id
    package_json_content = json.dumps(
        {
            "name": package_name,
            "dependencies": {
                "@dataform/core": "2.0.1",
            },
        },
        indent=4,
    ).encode()
    make_package_json_file = DataformWriteFileOperator(
        task_id="write-package-json",
        project_id=project_id,
        region=region,
        repository_id=repository_id,
        workspace_id=workspace_id,
        filepath="package.json",
        contents=package_json_content,
    )

    (
        make_definitions_directory
        >> make_first_view_file
        >> make_second_view_file
        >> make_gitignore_file
        >> make_dataform_config_file
        >> make_package_json_file
    )

    if without_installation:
        make_package_json_file >> make_includes_directory
    else:
        install_npm_packages = DataformInstallNpmPackagesOperator(
            task_id="install-npm-packages",
            project_id=project_id,
            region=region,
            repository_id=repository_id,
            workspace_id=workspace_id,
        )
        make_package_json_file >> install_npm_packages >> make_includes_directory

    return make_definitions_directory, make_includes_directory


def define_default_location(region: str) -> DataformLocations:
    if "us" in region:
        return DataformLocations.US
    elif "europe" in region:
        return DataformLocations.EUROPE

    regions_mapping: Mapping[str, DataformLocations] = {}

    return regions_mapping[region]
