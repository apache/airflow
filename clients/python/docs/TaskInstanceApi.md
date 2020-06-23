# airflow_client.TaskInstanceApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_extra_links**](TaskInstanceApi.md#get_extra_links) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links | Get extra links for task instance
[**get_log**](TaskInstanceApi.md#get_log) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number} | Get logs for a task instance
[**get_task_instance**](TaskInstanceApi.md#get_task_instance) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id} | Get a task instance
[**get_task_instances**](TaskInstanceApi.md#get_task_instances) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances | Get a list of task instance of DAG.
[**get_task_instances_batch**](TaskInstanceApi.md#get_task_instances_batch) | **POST** /dags/~/dagRuns/~/taskInstances/list | Get list of task instances from all DAGs and DAG Runs.


# **get_extra_links**
> ExtraLinkCollection get_extra_links(dag_id, dag_run_id, task_id)

Get extra links for task instance

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
    api_instance = airflow_client.TaskInstanceApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.

    try:
        # Get extra links for task instance
        api_response = api_instance.get_extra_links(dag_id, dag_run_id, task_id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling TaskInstanceApi->get_extra_links: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 

### Return type

[**ExtraLinkCollection**](ExtraLinkCollection.md)

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

# **get_log**
> InlineResponse200 get_log(dag_id, dag_run_id, task_id, task_try_number, full_content=full_content, token=token)

Get logs for a task instance

Get logs for a specific task instance and its try number

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
    api_instance = airflow_client.TaskInstanceApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.
task_try_number = 56 # int | The Task Try Number.
full_content = True # bool | A full content will be returned. By default, only the first fragment will be returned.  (optional)
token = 'token_example' # str | A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued.  (optional)

    try:
        # Get logs for a task instance
        api_response = api_instance.get_log(dag_id, dag_run_id, task_id, task_try_number, full_content=full_content, token=token)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling TaskInstanceApi->get_log: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 
 **task_try_number** | **int**| The Task Try Number. | 
 **full_content** | **bool**| A full content will be returned. By default, only the first fragment will be returned.  | [optional] 
 **token** | **str**| A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued.  | [optional] 

### Return type

[**InlineResponse200**](InlineResponse200.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Content of logs. |  -  |
**400** | Client specified an invalid argument. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |
**404** | A specified resource is not found. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_task_instance**
> TaskInstance get_task_instance(dag_id, dag_run_id, task_id)

Get a task instance

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
    api_instance = airflow_client.TaskInstanceApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
task_id = 'task_id_example' # str | The Task ID.

    try:
        # Get a task instance
        api_response = api_instance.get_task_instance(dag_id, dag_run_id, task_id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling TaskInstanceApi->get_task_instance: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **task_id** | **str**| The Task ID. | 

### Return type

[**TaskInstance**](TaskInstance.md)

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

# **get_task_instances**
> TaskInstanceCollection get_task_instances(dag_id, dag_run_id, execution_date_gte=execution_date_gte, execution_date_lte=execution_date_lte, start_date_gte=start_date_gte, start_date_lte=start_date_lte, end_date_gte=end_date_gte, end_date_lte=end_date_lte, duration_gte=duration_gte, duration_lte=duration_lte, state=state, pool=pool, queue=queue, limit=limit, offset=offset)

Get a list of task instance of DAG.

This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve DAG Runs for all DAGs and DAG Runs. 

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
    api_instance = airflow_client.TaskInstanceApi(api_client)
    dag_id = 'dag_id_example' # str | The DAG ID.
dag_run_id = 'dag_run_id_example' # str | The DAG Run ID.
execution_date_gte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  (optional)
execution_date_lte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  (optional)
start_date_gte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  (optional)
start_date_lte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  (optional)
end_date_gte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  (optional)
end_date_lte = '2013-10-20T19:20:30+01:00' # datetime | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  (optional)
duration_gte = 3.4 # float | Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  (optional)
duration_lte = 3.4 # float | Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  (optional)
state = ['state_example'] # list[str] | The value can be repeated to retrieve multiple matching values (OR condition). (optional)
pool = ['pool_example'] # list[str] | The value can be repeated to retrieve multiple matching values (OR condition). (optional)
queue = ['queue_example'] # list[str] | The value can be repeated to retrieve multiple matching values (OR condition). (optional)
limit = 100 # int | The numbers of items to return. (optional) (default to 100)
offset = 56 # int | The number of items to skip before starting to collect the result set. (optional)

    try:
        # Get a list of task instance of DAG.
        api_response = api_instance.get_task_instances(dag_id, dag_run_id, execution_date_gte=execution_date_gte, execution_date_lte=execution_date_lte, start_date_gte=start_date_gte, start_date_lte=start_date_lte, end_date_gte=end_date_gte, end_date_lte=end_date_lte, duration_gte=duration_gte, duration_lte=duration_lte, state=state, pool=pool, queue=queue, limit=limit, offset=offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling TaskInstanceApi->get_task_instances: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dag_id** | **str**| The DAG ID. | 
 **dag_run_id** | **str**| The DAG Run ID. | 
 **execution_date_gte** | **datetime**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional] 
 **execution_date_lte** | **datetime**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional] 
 **start_date_gte** | **datetime**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional] 
 **start_date_lte** | **datetime**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
 **end_date_gte** | **datetime**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional] 
 **end_date_lte** | **datetime**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
 **duration_gte** | **float**| Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  | [optional] 
 **duration_lte** | **float**| Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  | [optional] 
 **state** | [**list[str]**](str.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **pool** | [**list[str]**](str.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **queue** | [**list[str]**](str.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **limit** | **int**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **int**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of task instances. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_task_instances_batch**
> TaskInstanceCollection get_task_instances_batch(list_task_instance_form)

Get list of task instances from all DAGs and DAG Runs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

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
    api_instance = airflow_client.TaskInstanceApi(api_client)
    list_task_instance_form = airflow_client.ListTaskInstanceForm() # ListTaskInstanceForm | 

    try:
        # Get list of task instances from all DAGs and DAG Runs.
        api_response = api_instance.get_task_instances_batch(list_task_instance_form)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling TaskInstanceApi->get_task_instances_batch: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **list_task_instance_form** | [**ListTaskInstanceForm**](ListTaskInstanceForm.md)|  | 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of task instances. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |
**404** | A specified resource is not found. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

