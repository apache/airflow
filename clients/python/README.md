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

# Apache Airflow Python Client

# Overview

To facilitate management, Apache Airflow supports a range of REST API endpoints across its
objects.
This section provides an overview of the API design, methods, and supported use cases.

Most of the endpoints accept `JSON` as input and return `JSON` responses.
This means that you must usually add the following headers to your request:

```
Content-type: application/json
Accept: application/json
```

## Resources

The term `resource` refers to a single type of object in the Airflow metadata. An API is broken up by its
endpoint's corresponding resource.
The name of a resource is typically plural and expressed in camelCase. Example: `dagRuns`.

Resource names are used as part of endpoint URLs, as well as in API parameters and responses.

## CRUD Operations

The platform supports **Create**, **Read**, **Update**, and **Delete** operations on most resources.
You can review the standards for these operations and their standard parameters below.

Some endpoints have special behavior as exceptions.

### Create

To create a resource, you typically submit an HTTP `POST` request with the resource's required metadata
in the request body.
The response returns a `201 Created` response code upon success with the resource's metadata, including
its internal `id`, in the response body.

### Read

The HTTP `GET` request can be used to read a resource or to list a number of resources.

A resource's `id` can be submitted in the request parameters to read a specific resource.
The response usually returns a `200 OK` response code upon success, with the resource's metadata in
the response body.

If a `GET` request does not include a specific resource `id`, it is treated as a list request.
The response usually returns a `200 OK` response code upon success, with an object containing a list
of resources' metadata in the response body.

When reading resources, some common query parameters are usually available. e.g.:

```
/api/v2/connections?limit=25&offset=25
```

|Query Parameter|Type|Description|
|---------------|----|-----------|
|limit|integer|Maximum number of objects to fetch. Usually 25 by default|
|offset|integer|Offset after which to start returning objects. For use with limit query parameter.|

### Update

Updating a resource requires the resource `id`, and is typically done using an HTTP `PATCH` request,
with the fields to modify in the request body.
The response usually returns a `200 OK` response code upon success, with information about the modified
resource in the response body.

### Delete

Deleting a resource requires the resource `id` and is typically executing via an HTTP `DELETE` request.
The response usually returns a `204 No Content` response code upon success.

## Conventions

- Resource names are plural and expressed in camelCase.
- Names are consistent between URL parameter name and field name.

- Field names are in snake_case.

```json
{
    \"name\": \"string\",
    \"slots\": 0,
    \"occupied_slots\": 0,
    \"used_slots\": 0,
    \"queued_slots\": 0,
    \"open_slots\": 0
}
```

### Update Mask

Update mask is available as a query parameter in patch endpoints. It is used to notify the
API which fields you want to update. Using `update_mask` makes it easier to update objects
by helping the server know which fields to update in an object instead of updating all fields.
The update request ignores any fields that aren't specified in the field mask, leaving them with
their current values.

Example:

```python
import requests

resource = requests.get("/resource/my-id").json()
resource["my_field"] = "new-value"
requests.patch("/resource/my-id?update_mask=my_field", data=json.dumps(resource))
```

## Versioning and Endpoint Lifecycle

- API versioning is not synchronized to specific releases of the Apache Airflow.
- APIs are designed to be backward compatible.
- Any changes to the API will first go through a deprecation phase.

# Trying the API

You can use a third party client, such as [curl](https://curl.haxx.se/), [HTTPie](https://httpie.org/),
[Postman](https://www.postman.com/) or [the Insomnia rest client](https://insomnia.rest/) to test
the Apache Airflow API.

Note that you will need to pass authentication credentials. If your Airflow deployment supports
**Bearer token authentication**, you can use the following example:

For example, here is how to pause a DAG with `curl`, using a Bearer token:

```bash
curl -X PATCH 'https://example.com/api/v2/dags/{dag_id}?update_mask=is_paused' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN' \
  -d '{
      \"is_paused\": true
  }'
```

Using a graphical tool such as [Postman](https://www.postman.com/) or [Insomnia](https://insomnia.rest/),
it is possible to import the API specifications directly:

1. Download the API specification by clicking the **Download** button at top of this document.
2. Import the JSON specification in the graphical tool of your choice.

  - In *Postman*, you can click the **import** button at the top
  - With *Insomnia*, you can just drag-and-drop the file on the UI

Note that with *Postman*, you can also generate code snippets by selecting a request and clicking on
the **Code** button.

## Enabling CORS

[Cross-origin resource sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
is a browser security feature that restricts HTTP requests that are
initiated from scripts running in the browser.

For details on enabling/configuring CORS, see
[Enabling CORS](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html).

# Authentication

To be able to meet the requirements of many organizations, Airflow supports many authentication methods,
and it is even possible to add your own method.

The default is to deny all requests.

For details on configuring the authentication, see
[API Authorization](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html).

# Errors

We follow the error response format proposed in [RFC 7807](https://tools.ietf.org/html/rfc7807)
also known as Problem Details for HTTP APIs. As with our normal API responses,
your client must be prepared to gracefully handle additional members of the response.

## Unauthenticated

This indicates that the request has not been applied because it lacks valid authentication
credentials for the target resource. Please check that you have valid credentials.

## PermissionDenied

This response means that the server understood the request but refuses to authorize
it because it lacks sufficient rights to the resource. It happens when you do not have the
necessary permission to execute the action you performed. You need to get the appropriate
permissions in other to resolve this error.

## BadRequest

This response means that the server cannot or will not process the request due to something
that is perceived to be a client error (e.g., malformed request syntax, invalid request message
framing, or deceptive request routing). To resolve this, please ensure that your syntax is correct.

## NotFound

This client error response indicates that the server cannot find the requested resource.

## MethodNotAllowed

Indicates that the request method is known by the server but is not supported by the target resource.

## NotAcceptable

The target resource does not have a current representation that would be acceptable to the user
agent, according to the proactive negotiation header fields received in the request, and the
server is unwilling to supply a default representation.

## AlreadyExists

The request could not be completed due to a conflict with the current state of the target
resource, e.g. the resource it tries to create already exists.

## Unknown

This means that the server encountered an unexpected condition that prevented it from
fulfilling the request.

This Python package is automatically generated by the [OpenAPI Generator](https://openapi-generator.tech) project:

- API version: 2.9.0
- Package version: 2.9.0
- Build package: org.openapitools.codegen.languages.PythonClientCodegen

For more information, please visit [https://airflow.apache.org](https://airflow.apache.org)

## Requirements.

Python >=3.9

## Installation & Usage

### pip install

You can install the client using standard Python installation tools. It is hosted
in PyPI with `apache-airflow-client` package id so the easiest way to get the latest
version is to run:

```bash
pip install apache-airflow-client
```

If the python package is hosted on a repository, you can install directly using:

```bash
pip install git+https://github.com/apache/airflow-client-python.git
```

### Import check

Then import the package:

```python
import airflow_client.client
```

## Getting Started

Before attempting the following examples ensure you have an account with API access.
As an example you can create an account for usage with the API as follows using the Airflow CLI.

```bash
airflow users create -u admin-api -e admin-api@example.com -f admin-api -l admin-api -p $PASSWORD -r Admin
```

Please follow the [installation procedure](#installation--usage) and then run the following:

```python
import airflow_client.client
import requests
from airflow_client.client.rest import ApiException
from pprint import pprint
from pydantic import BaseModel


# What we expect back from auth/token
class AirflowAccessTokenResponse(BaseModel):
    access_token: str


# An optional helper function to retrieve an access token
def get_airflow_client_access_token(
    host: str,
    username: str,
    password: str,
) -> str:
    url = f"{host}/auth/token"
    payload = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 201:
        raise RuntimeError(f"Failed to get access token: {response.status_code} {response.text}")
    response_success = AirflowAccessTokenResponse(**response.json())
    return response_success.access_token


# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
host = "http://localhost"
configuration = airflow_client.client.Configuration(host=host)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

configuration.access_token = get_airflow_client_access_token(
    host=host,
    username="admin-api",
    password=os.environ["PASSWORD"],
)

# Enter a context with an instance of the API client
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.client.AssetApi(api_client)
    create_asset_events_body = airflow_client.client.CreateAssetEventsBody()  # CreateAssetEventsBody |

    try:
        # Create Asset Event
        api_response = api_instance.create_asset_event(create_asset_events_body)
        print("The response of AssetApi->create_asset_event:\n")
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling AssetApi->create_asset_event: %s\n" % e)
```

## Documentation for API Endpoints

All URIs are relative to *http://localhost*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*AssetApi* | [**create_asset_event**](docs/AssetApi.md#create_asset_event) | **POST** /api/v2/assets/events | Create Asset Event
*AssetApi* | [**delete_asset_queued_events**](docs/AssetApi.md#delete_asset_queued_events) | **DELETE** /api/v2/assets/{asset_id}/queuedEvents | Delete Asset Queued Events
*AssetApi* | [**delete_dag_asset_queued_event**](docs/AssetApi.md#delete_dag_asset_queued_event) | **DELETE** /api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents | Delete Dag Asset Queued Event
*AssetApi* | [**delete_dag_asset_queued_events**](docs/AssetApi.md#delete_dag_asset_queued_events) | **DELETE** /api/v2/dags/{dag_id}/assets/queuedEvents | Delete Dag Asset Queued Events
*AssetApi* | [**get_asset**](docs/AssetApi.md#get_asset) | **GET** /api/v2/assets/{asset_id} | Get Asset
*AssetApi* | [**get_asset_alias**](docs/AssetApi.md#get_asset_alias) | **GET** /api/v2/assets/aliases/{asset_alias_id} | Get Asset Alias
*AssetApi* | [**get_asset_aliases**](docs/AssetApi.md#get_asset_aliases) | **GET** /api/v2/assets/aliases | Get Asset Aliases
*AssetApi* | [**get_asset_events**](docs/AssetApi.md#get_asset_events) | **GET** /api/v2/assets/events | Get Asset Events
*AssetApi* | [**get_asset_queued_events**](docs/AssetApi.md#get_asset_queued_events) | **GET** /api/v2/assets/{asset_id}/queuedEvents | Get Asset Queued Events
*AssetApi* | [**get_assets**](docs/AssetApi.md#get_assets) | **GET** /api/v2/assets | Get Assets
*AssetApi* | [**get_dag_asset_queued_event**](docs/AssetApi.md#get_dag_asset_queued_event) | **GET** /api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents | Get Dag Asset Queued Event
*AssetApi* | [**get_dag_asset_queued_events**](docs/AssetApi.md#get_dag_asset_queued_events) | **GET** /api/v2/dags/{dag_id}/assets/queuedEvents | Get Dag Asset Queued Events
*AssetApi* | [**materialize_asset**](docs/AssetApi.md#materialize_asset) | **POST** /api/v2/assets/{asset_id}/materialize | Materialize Asset
*BackfillApi* | [**cancel_backfill**](docs/BackfillApi.md#cancel_backfill) | **PUT** /api/v2/backfills/{backfill_id}/cancel | Cancel Backfill
*BackfillApi* | [**create_backfill**](docs/BackfillApi.md#create_backfill) | **POST** /api/v2/backfills | Create Backfill
*BackfillApi* | [**create_backfill_dry_run**](docs/BackfillApi.md#create_backfill_dry_run) | **POST** /api/v2/backfills/dry_run | Create Backfill Dry Run
*BackfillApi* | [**get_backfill**](docs/BackfillApi.md#get_backfill) | **GET** /api/v2/backfills/{backfill_id} | Get Backfill
*BackfillApi* | [**list_backfills**](docs/BackfillApi.md#list_backfills) | **GET** /api/v2/backfills | List Backfills
*BackfillApi* | [**pause_backfill**](docs/BackfillApi.md#pause_backfill) | **PUT** /api/v2/backfills/{backfill_id}/pause | Pause Backfill
*BackfillApi* | [**unpause_backfill**](docs/BackfillApi.md#unpause_backfill) | **PUT** /api/v2/backfills/{backfill_id}/unpause | Unpause Backfill
*ConfigApi* | [**get_config**](docs/ConfigApi.md#get_config) | **GET** /api/v2/config | Get Config
*ConfigApi* | [**get_config_value**](docs/ConfigApi.md#get_config_value) | **GET** /api/v2/config/section/{section}/option/{option} | Get Config Value
*ConnectionApi* | [**bulk_connections**](docs/ConnectionApi.md#bulk_connections) | **PATCH** /api/v2/connections | Bulk Connections
*ConnectionApi* | [**create_default_connections**](docs/ConnectionApi.md#create_default_connections) | **POST** /api/v2/connections/defaults | Create Default Connections
*ConnectionApi* | [**delete_connection**](docs/ConnectionApi.md#delete_connection) | **DELETE** /api/v2/connections/{connection_id} | Delete Connection
*ConnectionApi* | [**get_connection**](docs/ConnectionApi.md#get_connection) | **GET** /api/v2/connections/{connection_id} | Get Connection
*ConnectionApi* | [**get_connections**](docs/ConnectionApi.md#get_connections) | **GET** /api/v2/connections | Get Connections
*ConnectionApi* | [**patch_connection**](docs/ConnectionApi.md#patch_connection) | **PATCH** /api/v2/connections/{connection_id} | Patch Connection
*ConnectionApi* | [**post_connection**](docs/ConnectionApi.md#post_connection) | **POST** /api/v2/connections | Post Connection
*ConnectionApi* | [**test_connection**](docs/ConnectionApi.md#test_connection) | **POST** /api/v2/connections/test | Test Connection
*DAGApi* | [**delete_dag**](docs/DAGApi.md#delete_dag) | **DELETE** /api/v2/dags/{dag_id} | Delete Dag
*DAGApi* | [**get_dag**](docs/DAGApi.md#get_dag) | **GET** /api/v2/dags/{dag_id} | Get Dag
*DAGApi* | [**get_dag_details**](docs/DAGApi.md#get_dag_details) | **GET** /api/v2/dags/{dag_id}/details | Get Dag Details
*DAGApi* | [**get_dag_tags**](docs/DAGApi.md#get_dag_tags) | **GET** /api/v2/dagTags | Get Dag Tags
*DAGApi* | [**get_dags**](docs/DAGApi.md#get_dags) | **GET** /api/v2/dags | Get Dags
*DAGApi* | [**patch_dag**](docs/DAGApi.md#patch_dag) | **PATCH** /api/v2/dags/{dag_id} | Patch Dag
*DAGApi* | [**patch_dags**](docs/DAGApi.md#patch_dags) | **PATCH** /api/v2/dags | Patch Dags
*DAGParsingApi* | [**reparse_dag_file**](docs/DAGParsingApi.md#reparse_dag_file) | **PUT** /api/v2/parseDagFile/{file_token} | Reparse Dag File
*DagReportApi* | [**get_dag_reports**](docs/DagReportApi.md#get_dag_reports) | **GET** /api/v2/dagReports | Get Dag Reports
*DagRunApi* | [**clear_dag_run**](docs/DagRunApi.md#clear_dag_run) | **POST** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear | Clear Dag Run
*DagRunApi* | [**delete_dag_run**](docs/DagRunApi.md#delete_dag_run) | **DELETE** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id} | Delete Dag Run
*DagRunApi* | [**get_dag_run**](docs/DagRunApi.md#get_dag_run) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id} | Get Dag Run
*DagRunApi* | [**get_dag_runs**](docs/DagRunApi.md#get_dag_runs) | **GET** /api/v2/dags/{dag_id}/dagRuns | Get Dag Runs
*DagRunApi* | [**get_list_dag_runs_batch**](docs/DagRunApi.md#get_list_dag_runs_batch) | **POST** /api/v2/dags/{dag_id}/dagRuns/list | Get List Dag Runs Batch
*DagRunApi* | [**get_upstream_asset_events**](docs/DagRunApi.md#get_upstream_asset_events) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents | Get Upstream Asset Events
*DagRunApi* | [**wait_dag_run_until_finished**](docs/DagRunApi.md#wait_dag_run_until_finished) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/wait | Experimental: Wait for a dag run to complete, and return task results if requested.
*DagRunApi* | [**patch_dag_run**](docs/DagRunApi.md#patch_dag_run) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id} | Patch Dag Run
*DagRunApi* | [**trigger_dag_run**](docs/DagRunApi.md#trigger_dag_run) | **POST** /api/v2/dags/{dag_id}/dagRuns | Trigger Dag Run
*DagSourceApi* | [**get_dag_source**](docs/DagSourceApi.md#get_dag_source) | **GET** /api/v2/dagSources/{dag_id} | Get Dag Source
*DagStatsApi* | [**get_dag_stats**](docs/DagStatsApi.md#get_dag_stats) | **GET** /api/v2/dagStats | Get Dag Stats
*DagVersionApi* | [**get_dag_version**](docs/DagVersionApi.md#get_dag_version) | **GET** /api/v2/dags/{dag_id}/dagVersions/{version_number} | Get Dag Version
*DagVersionApi* | [**get_dag_versions**](docs/DagVersionApi.md#get_dag_versions) | **GET** /api/v2/dags/{dag_id}/dagVersions | Get Dag Versions
*DagWarningApi* | [**list_dag_warnings**](docs/DagWarningApi.md#list_dag_warnings) | **GET** /api/v2/dagWarnings | List Dag Warnings
*EventLogApi* | [**get_event_log**](docs/EventLogApi.md#get_event_log) | **GET** /api/v2/eventLogs/{event_log_id} | Get Event Log
*EventLogApi* | [**get_event_logs**](docs/EventLogApi.md#get_event_logs) | **GET** /api/v2/eventLogs | Get Event Logs
*ExtraLinksApi* | [**get_extra_links**](docs/ExtraLinksApi.md#get_extra_links) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links | Get Extra Links
*ImportErrorApi* | [**get_import_error**](docs/ImportErrorApi.md#get_import_error) | **GET** /api/v2/importErrors/{import_error_id} | Get Import Error
*ImportErrorApi* | [**get_import_errors**](docs/ImportErrorApi.md#get_import_errors) | **GET** /api/v2/importErrors | Get Import Errors
*JobApi* | [**get_jobs**](docs/JobApi.md#get_jobs) | **GET** /api/v2/jobs | Get Jobs
*LoginApi* | [**login**](docs/LoginApi.md#login) | **GET** /api/v2/auth/login | Login
*LoginApi* | [**logout**](docs/LoginApi.md#logout) | **GET** /api/v2/auth/logout | Logout
*MonitorApi* | [**get_health**](docs/MonitorApi.md#get_health) | **GET** /api/v2/monitor/health | Get Health
*PluginApi* | [**get_plugins**](docs/PluginApi.md#get_plugins) | **GET** /api/v2/plugins | Get Plugins
*PoolApi* | [**bulk_pools**](docs/PoolApi.md#bulk_pools) | **PATCH** /api/v2/pools | Bulk Pools
*PoolApi* | [**delete_pool**](docs/PoolApi.md#delete_pool) | **DELETE** /api/v2/pools/{pool_name} | Delete Pool
*PoolApi* | [**get_pool**](docs/PoolApi.md#get_pool) | **GET** /api/v2/pools/{pool_name} | Get Pool
*PoolApi* | [**get_pools**](docs/PoolApi.md#get_pools) | **GET** /api/v2/pools | Get Pools
*PoolApi* | [**patch_pool**](docs/PoolApi.md#patch_pool) | **PATCH** /api/v2/pools/{pool_name} | Patch Pool
*PoolApi* | [**post_pool**](docs/PoolApi.md#post_pool) | **POST** /api/v2/pools | Post Pool
*ProviderApi* | [**get_providers**](docs/ProviderApi.md#get_providers) | **GET** /api/v2/providers | Get Providers
*TaskApi* | [**get_task**](docs/TaskApi.md#get_task) | **GET** /api/v2/dags/{dag_id}/tasks/{task_id} | Get Task
*TaskApi* | [**get_tasks**](docs/TaskApi.md#get_tasks) | **GET** /api/v2/dags/{dag_id}/tasks | Get Tasks
*TaskInstanceApi* | [**get_extra_links**](docs/TaskInstanceApi.md#get_extra_links) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links | Get Extra Links
*TaskInstanceApi* | [**get_log**](docs/TaskInstanceApi.md#get_log) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number} | Get Log
*TaskInstanceApi* | [**get_mapped_task_instance**](docs/TaskInstanceApi.md#get_mapped_task_instance) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index} | Get Mapped Task Instance
*TaskInstanceApi* | [**get_mapped_task_instance_tries**](docs/TaskInstanceApi.md#get_mapped_task_instance_tries) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries | Get Mapped Task Instance Tries
*TaskInstanceApi* | [**get_mapped_task_instance_try_details**](docs/TaskInstanceApi.md#get_mapped_task_instance_try_details) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries/{task_try_number} | Get Mapped Task Instance Try Details
*TaskInstanceApi* | [**get_mapped_task_instances**](docs/TaskInstanceApi.md#get_mapped_task_instances) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped | Get Mapped Task Instances
*TaskInstanceApi* | [**get_task_instance**](docs/TaskInstanceApi.md#get_task_instance) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id} | Get Task Instance
*TaskInstanceApi* | [**get_task_instance_dependencies**](docs/TaskInstanceApi.md#get_task_instance_dependencies) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dependencies | Get Task Instance Dependencies
*TaskInstanceApi* | [**get_task_instance_dependencies_by_map_index**](docs/TaskInstanceApi.md#get_task_instance_dependencies_by_map_index) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dependencies | Get Task Instance Dependencies
*TaskInstanceApi* | [**get_task_instance_tries**](docs/TaskInstanceApi.md#get_task_instance_tries) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries | Get Task Instance Tries
*TaskInstanceApi* | [**get_task_instance_try_details**](docs/TaskInstanceApi.md#get_task_instance_try_details) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries/{task_try_number} | Get Task Instance Try Details
*TaskInstanceApi* | [**get_task_instances**](docs/TaskInstanceApi.md#get_task_instances) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances | Get Task Instances
*TaskInstanceApi* | [**get_task_instances_batch**](docs/TaskInstanceApi.md#get_task_instances_batch) | **POST** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/list | Get Task Instances Batch
*TaskInstanceApi* | [**patch_task_instance**](docs/TaskInstanceApi.md#patch_task_instance) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id} | Patch Task Instance
*TaskInstanceApi* | [**patch_task_instance_by_map_index**](docs/TaskInstanceApi.md#patch_task_instance_by_map_index) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index} | Patch Task Instance
*TaskInstanceApi* | [**patch_task_instance_dry_run**](docs/TaskInstanceApi.md#patch_task_instance_dry_run) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dry_run | Patch Task Instance Dry Run
*TaskInstanceApi* | [**patch_task_instance_dry_run_by_map_index**](docs/TaskInstanceApi.md#patch_task_instance_dry_run_by_map_index) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dry_run | Patch Task Instance Dry Run
*TaskInstanceApi* | [**post_clear_task_instances**](docs/TaskInstanceApi.md#post_clear_task_instances) | **POST** /api/v2/dags/{dag_id}/clearTaskInstances | Post Clear Task Instances
*VariableApi* | [**bulk_variables**](docs/VariableApi.md#bulk_variables) | **PATCH** /api/v2/variables | Bulk Variables
*VariableApi* | [**delete_variable**](docs/VariableApi.md#delete_variable) | **DELETE** /api/v2/variables/{variable_key} | Delete Variable
*VariableApi* | [**get_variable**](docs/VariableApi.md#get_variable) | **GET** /api/v2/variables/{variable_key} | Get Variable
*VariableApi* | [**get_variables**](docs/VariableApi.md#get_variables) | **GET** /api/v2/variables | Get Variables
*VariableApi* | [**patch_variable**](docs/VariableApi.md#patch_variable) | **PATCH** /api/v2/variables/{variable_key} | Patch Variable
*VariableApi* | [**post_variable**](docs/VariableApi.md#post_variable) | **POST** /api/v2/variables | Post Variable
*VersionApi* | [**get_version**](docs/VersionApi.md#get_version) | **GET** /api/v2/version | Get Version
*XComApi* | [**create_xcom_entry**](docs/XComApi.md#create_xcom_entry) | **POST** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Create Xcom Entry
*XComApi* | [**get_xcom_entries**](docs/XComApi.md#get_xcom_entries) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Get Xcom Entries
*XComApi* | [**get_xcom_entry**](docs/XComApi.md#get_xcom_entry) | **GET** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Get Xcom Entry
*XComApi* | [**update_xcom_entry**](docs/XComApi.md#update_xcom_entry) | **PATCH** /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Update Xcom Entry


## Documentation For Models

 - [AppBuilderMenuItemResponse](docs/AppBuilderMenuItemResponse.md)
 - [AppBuilderViewResponse](docs/AppBuilderViewResponse.md)
 - [AssetAliasCollectionResponse](docs/AssetAliasCollectionResponse.md)
 - [AssetAliasResponse](docs/AssetAliasResponse.md)
 - [AssetCollectionResponse](docs/AssetCollectionResponse.md)
 - [AssetEventCollectionResponse](docs/AssetEventCollectionResponse.md)
 - [AssetEventResponse](docs/AssetEventResponse.md)
 - [AssetResponse](docs/AssetResponse.md)
 - [BackfillCollectionResponse](docs/BackfillCollectionResponse.md)
 - [BackfillPostBody](docs/BackfillPostBody.md)
 - [BackfillResponse](docs/BackfillResponse.md)
 - [BaseInfoResponse](docs/BaseInfoResponse.md)
 - [BulkAction](docs/BulkAction.md)
 - [BulkActionNotOnExistence](docs/BulkActionNotOnExistence.md)
 - [BulkActionOnExistence](docs/BulkActionOnExistence.md)
 - [BulkActionResponse](docs/BulkActionResponse.md)
 - [BulkBodyConnectionBody](docs/BulkBodyConnectionBody.md)
 - [BulkBodyConnectionBodyActionsInner](docs/BulkBodyConnectionBodyActionsInner.md)
 - [BulkBodyPoolBody](docs/BulkBodyPoolBody.md)
 - [BulkBodyPoolBodyActionsInner](docs/BulkBodyPoolBodyActionsInner.md)
 - [BulkBodyVariableBody](docs/BulkBodyVariableBody.md)
 - [BulkBodyVariableBodyActionsInner](docs/BulkBodyVariableBodyActionsInner.md)
 - [BulkCreateActionConnectionBody](docs/BulkCreateActionConnectionBody.md)
 - [BulkCreateActionPoolBody](docs/BulkCreateActionPoolBody.md)
 - [BulkCreateActionVariableBody](docs/BulkCreateActionVariableBody.md)
 - [BulkDeleteActionConnectionBody](docs/BulkDeleteActionConnectionBody.md)
 - [BulkDeleteActionPoolBody](docs/BulkDeleteActionPoolBody.md)
 - [BulkDeleteActionVariableBody](docs/BulkDeleteActionVariableBody.md)
 - [BulkResponse](docs/BulkResponse.md)
 - [BulkUpdateActionConnectionBody](docs/BulkUpdateActionConnectionBody.md)
 - [BulkUpdateActionPoolBody](docs/BulkUpdateActionPoolBody.md)
 - [BulkUpdateActionVariableBody](docs/BulkUpdateActionVariableBody.md)
 - [ClearTaskInstancesBody](docs/ClearTaskInstancesBody.md)
 - [ClearTaskInstancesBodyTaskIdsInner](docs/ClearTaskInstancesBodyTaskIdsInner.md)
 - [Config](docs/Config.md)
 - [ConfigOption](docs/ConfigOption.md)
 - [ConfigSection](docs/ConfigSection.md)
 - [ConnectionBody](docs/ConnectionBody.md)
 - [ConnectionCollectionResponse](docs/ConnectionCollectionResponse.md)
 - [ConnectionResponse](docs/ConnectionResponse.md)
 - [ConnectionTestResponse](docs/ConnectionTestResponse.md)
 - [Content](docs/Content.md)
 - [CreateAssetEventsBody](docs/CreateAssetEventsBody.md)
 - [DAGCollectionResponse](docs/DAGCollectionResponse.md)
 - [DAGDetailsResponse](docs/DAGDetailsResponse.md)
 - [DAGPatchBody](docs/DAGPatchBody.md)
 - [DAGResponse](docs/DAGResponse.md)
 - [DAGRunClearBody](docs/DAGRunClearBody.md)
 - [DAGRunCollectionResponse](docs/DAGRunCollectionResponse.md)
 - [DAGRunPatchBody](docs/DAGRunPatchBody.md)
 - [DAGRunPatchStates](docs/DAGRunPatchStates.md)
 - [DAGRunResponse](docs/DAGRunResponse.md)
 - [DAGRunsBatchBody](docs/DAGRunsBatchBody.md)
 - [DAGSourceResponse](docs/DAGSourceResponse.md)
 - [DAGTagCollectionResponse](docs/DAGTagCollectionResponse.md)
 - [DAGVersionCollectionResponse](docs/DAGVersionCollectionResponse.md)
 - [DAGWarningCollectionResponse](docs/DAGWarningCollectionResponse.md)
 - [DAGWarningResponse](docs/DAGWarningResponse.md)
 - [DagProcessorInfoResponse](docs/DagProcessorInfoResponse.md)
 - [DagRunAssetReference](docs/DagRunAssetReference.md)
 - [DagRunState](docs/DagRunState.md)
 - [DagRunTriggeredByType](docs/DagRunTriggeredByType.md)
 - [DagRunType](docs/DagRunType.md)
 - [DagScheduleAssetReference](docs/DagScheduleAssetReference.md)
 - [DagStatsCollectionResponse](docs/DagStatsCollectionResponse.md)
 - [DagStatsResponse](docs/DagStatsResponse.md)
 - [DagStatsStateResponse](docs/DagStatsStateResponse.md)
 - [DagTagResponse](docs/DagTagResponse.md)
 - [DagVersionResponse](docs/DagVersionResponse.md)
 - [DagWarningType](docs/DagWarningType.md)
 - [Detail](docs/Detail.md)
 - [DryRunBackfillCollectionResponse](docs/DryRunBackfillCollectionResponse.md)
 - [DryRunBackfillResponse](docs/DryRunBackfillResponse.md)
 - [EventLogCollectionResponse](docs/EventLogCollectionResponse.md)
 - [EventLogResponse](docs/EventLogResponse.md)
 - [ExtraLinkCollectionResponse](docs/ExtraLinkCollectionResponse.md)
 - [FastAPIAppResponse](docs/FastAPIAppResponse.md)
 - [FastAPIRootMiddlewareResponse](docs/FastAPIRootMiddlewareResponse.md)
 - [HTTPExceptionResponse](docs/HTTPExceptionResponse.md)
 - [HTTPValidationError](docs/HTTPValidationError.md)
 - [HealthInfoResponse](docs/HealthInfoResponse.md)
 - [ImportErrorCollectionResponse](docs/ImportErrorCollectionResponse.md)
 - [ImportErrorResponse](docs/ImportErrorResponse.md)
 - [JobCollectionResponse](docs/JobCollectionResponse.md)
 - [JobResponse](docs/JobResponse.md)
 - [PatchTaskInstanceBody](docs/PatchTaskInstanceBody.md)
 - [PluginCollectionResponse](docs/PluginCollectionResponse.md)
 - [PluginResponse](docs/PluginResponse.md)
 - [PoolBody](docs/PoolBody.md)
 - [PoolCollectionResponse](docs/PoolCollectionResponse.md)
 - [PoolPatchBody](docs/PoolPatchBody.md)
 - [PoolResponse](docs/PoolResponse.md)
 - [ProviderCollectionResponse](docs/ProviderCollectionResponse.md)
 - [ProviderResponse](docs/ProviderResponse.md)
 - [QueuedEventCollectionResponse](docs/QueuedEventCollectionResponse.md)
 - [QueuedEventResponse](docs/QueuedEventResponse.md)
 - [ReprocessBehavior](docs/ReprocessBehavior.md)
 - [ResponseClearDagRun](docs/ResponseClearDagRun.md)
 - [ResponseGetXcomEntry](docs/ResponseGetXcomEntry.md)
 - [SchedulerInfoResponse](docs/SchedulerInfoResponse.md)
 - [StructuredLogMessage](docs/StructuredLogMessage.md)
 - [TaskCollectionResponse](docs/TaskCollectionResponse.md)
 - [TaskDependencyCollectionResponse](docs/TaskDependencyCollectionResponse.md)
 - [TaskDependencyResponse](docs/TaskDependencyResponse.md)
 - [TaskInstanceCollectionResponse](docs/TaskInstanceCollectionResponse.md)
 - [TaskInstanceHistoryCollectionResponse](docs/TaskInstanceHistoryCollectionResponse.md)
 - [TaskInstanceHistoryResponse](docs/TaskInstanceHistoryResponse.md)
 - [TaskInstanceResponse](docs/TaskInstanceResponse.md)
 - [TaskInstanceState](docs/TaskInstanceState.md)
 - [TaskInstancesBatchBody](docs/TaskInstancesBatchBody.md)
 - [TaskInstancesLogResponse](docs/TaskInstancesLogResponse.md)
 - [TaskOutletAssetReference](docs/TaskOutletAssetReference.md)
 - [TaskResponse](docs/TaskResponse.md)
 - [TimeDelta](docs/TimeDelta.md)
 - [TriggerDAGRunPostBody](docs/TriggerDAGRunPostBody.md)
 - [TriggerResponse](docs/TriggerResponse.md)
 - [TriggererInfoResponse](docs/TriggererInfoResponse.md)
 - [ValidationError](docs/ValidationError.md)
 - [ValidationErrorLocInner](docs/ValidationErrorLocInner.md)
 - [Value](docs/Value.md)
 - [VariableBody](docs/VariableBody.md)
 - [VariableCollectionResponse](docs/VariableCollectionResponse.md)
 - [VariableResponse](docs/VariableResponse.md)
 - [VersionInfo](docs/VersionInfo.md)
 - [XComCollectionResponse](docs/XComCollectionResponse.md)
 - [XComCreateBody](docs/XComCreateBody.md)
 - [XComResponse](docs/XComResponse.md)
 - [XComResponseNative](docs/XComResponseNative.md)
 - [XComResponseString](docs/XComResponseString.md)
 - [XComUpdateBody](docs/XComUpdateBody.md)

## Documentation For Authorization

By default the generated client supports the three authentication schemes:

* Basic
* GoogleOpenID
* Kerberos
* OAuth2PasswordBearer

However, you can generate client and documentation with your own schemes by adding your own schemes in
the security section of the OpenAPI specification. You can do it with Breeze CLI by adding the
``--security-schemes`` option to the ``breeze release-management prepare-python-client`` command.

## Basic "smoke" tests

You can run basic smoke tests to check if the client is working properly - we have a simple test script
that uses the API to run the tests. To do that, you need to:

* install the `apache-airflow-client` package as described above
* install ``rich`` Python package
* download the [test_python_client.py](test_python_client.py) file
* make sure you have test airflow installation running. Do not experiment with your production deployment
* configure your airflow webserver to enable basic authentication
  In the `[api]` section of your `airflow.cfg` set:

```ini
[api]
auth_backend = airflow.providers.fab.auth_manager.api.auth.backend.session,airflow.providers.fab.auth_manager.api.auth.backend.basic_auth
```

You can also set it by env variable:
`export AIRFLOW__API__AUTH_BACKENDS=airflow.providers.fab.auth_manager.api.auth.backend.session,airflow.providers.fab.auth_manager.api.auth.backend.basic_auth`

* configure your airflow webserver to load example dags
  In the `[core]` section of your `airflow.cfg` set:

```ini
[core]
load_examples = True
```

You can also set it by env variable: `export AIRFLOW__CORE__LOAD_EXAMPLES=True`

* optionally expose configuration (NOTE! that this is dangerous setting). The script will happily run with
  the default setting, but if you want to see the configuration, you need to expose it.
  Note that sensitive configuration values are values are always masked
  In the `[api]` section of your `airflow.cfg` set:

```ini
[api]
expose_config = True
```

You can also set it by env variable: `export AIRFLOW__API__EXPOSE_CONFIG=True`

* Configure your host/ip/user/password in the `test_python_client.py` file

```python
import airflow_client

# get the access token from Airflow API Server via /auth/token
configuration = airflow_client.client.Configuration(host="http://localhost:8080", access_token=access_token)
```

* Run scheduler (or dag file processor you have setup with standalone dag file processor) for few parsing
  loops (you can pass --num-runs parameter to it or keep it running in the background). The script relies
  on example DAGs being serialized to the DB and this only
  happens when scheduler runs with ``core/load_examples`` set to True.

* Run webserver - reachable at the host/port for the test script you want to run. Make sure it had enough
  time to initialize.

Run `python test_python_client.py` and you should see colored output showing attempts to connect and status.


## Notes for Large OpenAPI documents

If the OpenAPI document is large, imports in client.apis and client.models may fail with a
RecursionError indicating the maximum recursion limit has been exceeded. In that case, there are a couple of solutions:

Solution 1:
Use specific imports for apis and models like:

- `from airflow_client.client.api.default_api import DefaultApi`
- `from airflow_client.client.model.pet import Pet`

Solution 2:
Before importing the package, adjust the maximum recursion limit as shown below:

```python
import sys

sys.setrecursionlimit(1500)
import airflow_client.client
from airflow_client.client.api import *
from airflow_client.client.models import *
```

## Authors

dev@airflow.apache.org
