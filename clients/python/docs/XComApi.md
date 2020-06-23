# airflow_client.XComApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_xcom_entry**](XComApi.md#delete_xcom_entry) | **DELETE** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Delete an XCom entry
[**get_xcom_entries**](XComApi.md#get_xcom_entries) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Get all XCom entries
[**get_xcom_entry**](XComApi.md#get_xcom_entry) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Get an XCom entry
[**patch_xcom_entry**](XComApi.md#patch_xcom_entry) | **PATCH** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Update an XCom entry
[**post_xcom_entries**](XComApi.md#post_xcom_entries) | **POST** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Create an XCom entry


# **delete_xcom_entry**
> delete_xcom_entry(dag_id, dag_run_id, task_id, xcom_key)

Delete an XCom entry

### Example

```python
from __future__ import print_function
import time
import airflow_client
from airflow_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with airflow_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.XComApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
xcom_key = 'xcom_key_example' # str | The XCom Key.

    try:
        # Delete an XCom entry
        api_instance.delete_xcom_entry(dag_id, dag_run_id, task_id, xcom_key)
    except ApiException as e:
        print("Exception when calling XComApi->delete_xcom_entry: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **xcom_key** | **str**| The XCom Key. | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No content. |  -  |
**400** | Client specified an invalid argument. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_xcom_entries**
> XComCollection get_xcom_entries(dag_id, dag_run_id, task_id, limit=limit, offset=offset)

Get all XCom entries

This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCOM entries for for all DAGs, DAG Runs and task instances.

### Example

```python
from __future__ import print_function
import time
import airflow_client
from airflow_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with airflow_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.XComApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
limit = 100 # int | The numbers of items to return. (optional) (default to 100)
offset = 56 # int | The number of items to skip before starting to collect the result set. (optional)

    try:
        # Get all XCom entries
        api_response = api_instance.get_xcom_entries(dag_id, dag_run_id, task_id, limit=limit, offset=offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling XComApi->get_xcom_entries: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **limit** | **int**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **int**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**XComCollection**](XComCollection.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of XCom entries. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_xcom_entry**
> XCom get_xcom_entry(dag_id, dag_run_id, task_id, xcom_key)

Get an XCom entry

### Example

```python
from __future__ import print_function
import time
import airflow_client
from airflow_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with airflow_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.XComApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
xcom_key = 'xcom_key_example' # str | The XCom Key.

    try:
        # Get an XCom entry
        api_response = api_instance.get_xcom_entry(dag_id, dag_run_id, task_id, xcom_key)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling XComApi->get_xcom_entry: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **xcom_key** | **str**| The XCom Key. | 

### Return type

[**XCom**](XCom.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |
**404** | A specified resource is not found. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **patch_xcom_entry**
> XCom patch_xcom_entry(dag_id, dag_run_id, task_id, xcom_key, x_com, update_mask=update_mask)

Update an XCom entry

### Example

```python
from __future__ import print_function
import time
import airflow_client
from airflow_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with airflow_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.XComApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
xcom_key = 'xcom_key_example' # str | The XCom Key.
x_com = airflow_client.XCom() # XCom | 
update_mask = ['update_mask_example'] # list[str] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  (optional)

    try:
        # Update an XCom entry
        api_response = api_instance.patch_xcom_entry(dag_id, dag_run_id, task_id, xcom_key, x_com, update_mask=update_mask)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling XComApi->patch_xcom_entry: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **xcom_key** | **str**| The XCom Key. | 
 **x_com** | [**XCom**](XCom.md)|  | 
 **update_mask** | [**list[str]**](str.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**XCom**](XCom.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response. |  -  |
**400** | Client specified an invalid argument. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |
**404** | A specified resource is not found. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **post_xcom_entries**
> XCom post_xcom_entries(dag_id, dag_run_id, task_id, x_com)

Create an XCom entry

### Example

```python
from __future__ import print_function
import time
import airflow_client
from airflow_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.Configuration(
    host = "http://localhost/api/v1"
)


# Enter a context with an instance of the API client
with airflow_client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = airflow_client.XComApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
x_com = airflow_client.XCom() # XCom | 

    try:
        # Create an XCom entry
        api_response = api_instance.post_xcom_entries(dag_id, dag_run_id, task_id, x_com)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling XComApi->post_xcom_entries: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **x_com** | [**XCom**](XCom.md)|  | 

### Return type

[**XCom**](XCom.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response. |  -  |
**400** | Client specified an invalid argument. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

