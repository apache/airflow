<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Google system test resource cleanup

This helper project manages resources in a Google Cloud project used for Google
provider system tests. It lives under `providers/google/tests` as test tooling
and is not included in the Google provider package.

The tool is intended for Airflow contributors who run Google provider system
tests against their own Google Cloud projects. It does not require access to any
Google-internal project. You need Google Cloud credentials with permission to
read Cloud Asset Inventory and delete the resource types you choose to clean up.

Create and activate a virtual environment, then install this helper from this
directory:

```shell
pip install -e .
```

Now you can use `airflow-google-system-test-cleanup --help` to see the available
commands.

Here is a sample output:

```shell
usage: airflow-google-system-test-cleanup [-h] [--config-path CONFIG_PATH] [--resources-file-path RESOURCES_FILE_PATH] {list,list-asset-types,tree,delete} ...

CLI to manage resource for a GCP project

positional arguments:
  {list,list-asset-types,tree,delete}
    list                Retrieve the GCP resources for the given GCP project
    list-asset-types    List all the unique asset types hierarchically in the GCP project
    tree                Show the resources hierarchically as an HTML file
    delete              Delete the resources for the given GCP project

options:
  -h, --help            show this help message and exit
  --config-path CONFIG_PATH
                        Direct path to a project config JSON file
  --resources-file-path RESOURCES_FILE_PATH
                        Direct path to the resources.json file
```

## Global Options

-   `--config-path`: Override the automatic lookup of the project configuration.
    Defaults to `config/<PROJECT_ID>.json`.
-   `--resources-file-path`: Override where the tool saves or loads resource
    data. Defaults to `resources/<PROJECT_ID>/resources.json`.

The project configuration file is optional. It is created on first use if it does
not exist. Use `protected_resources` or protected labels such as
`do-not-delete` to keep resources out of deletion.

Example configuration:

```json
{
  "project_id": "example-project",
  "default_location": "us-central1",
  "protected_resources": {
    "storage.googleapis.com/Bucket": [
      "//storage.googleapis.com/example-system-test-bucket"
    ]
  }
}
```

## Example usages

-   To retrieve all resources for the project and sync with Cloud Asset
    Inventory: `airflow-google-system-test-cleanup list --project-id <PROJECT_ID> --sync`


-   To list all resources from an existing previously synced file:
    `airflow-google-system-test-cleanup list --project-id <PROJECT_ID>`


-   To retrieve the specific asset type (e.g: `ai`) resources for the project:
    `airflow-google-system-test-cleanup list --project-id <PROJECT_ID> --asset-type <ASSET_TYPE>`

-   To produce an HTML tree visualization using a specific config file:
    `airflow-google-system-test-cleanup tree --project-id <PROJECT_ID> --config-path /path/to/my_config.json`

-   To list the all unique asset types in a hierarchical tree:
    `airflow-google-system-test-cleanup list-asset-types --project-id <PROJECT_ID>`

    > # you can pass `--asset-type` parameter to list only one type of assets.

-   To clean up resources: `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID>`

-   To clean up only resources that are old enough:
    `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID> --asset-type dataproc --min-age-days 3`

-   To skip a service group during deletion, for example Composer:
    `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID> --skip-asset-type composer`

-   To clean up auxiliary resources that are not returned by Cloud Asset
    Inventory, for example Vertex AI Ray clusters:
    `airflow-google-system-test-cleanup delete --project-id <PROJECT_ID> --asset-type vertex_ai_raycluster`

By default `delete` cleans up resources listed in the synced resources file.
Auxiliary resource cleanup is explicit and config-driven because those resources
are discovered by service-specific APIs rather than Cloud Asset Inventory.

## Development & Testing

Unit tests for this helper live in the normal Google provider unit-test tree so
they run in CI without real Google Cloud access. They mock `gcloud`, API calls,
and delete handlers.

To run them from the repository root:

```shell
breeze testing providers-tests --test-type "Providers[google]" -- providers/google/tests/unit/google/resources_cleanup
```

## Building and Distribution

This helper is not published with the Google provider package. The local
`pyproject.toml` exists only to make ad-hoc local execution possible while this
tool remains under discussion.

To build the project as a Python wheel for local testing:

1. Install the build tool: `pip install build`
2. Generate the wheel package: `python -m build`

This will create the `dist/` directory containing the wheel file.
