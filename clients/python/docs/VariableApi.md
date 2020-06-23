# airflow_client.VariableApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_variable**](VariableApi.md#delete_variable) | **DELETE** /variables/{variable_key} | Delete variable
[**get_variable**](VariableApi.md#get_variable) | **GET** /variables/{variable_key} | Get a variable by key
[**get_variables**](VariableApi.md#get_variables) | **GET** /variables | Get all variables
[**patch_variable**](VariableApi.md#patch_variable) | **PATCH** /variables/{variable_key} | Update a variable by key
[**post_variables**](VariableApi.md#post_variables) | **POST** /variables | Create a variable


# **delete_variable**
> delete_variable(variable_key)

Delete variable

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
    api_instance = airflow_client.VariableApi(api_client)
    variable_key = 'variable_key_example' # str | The Variable Key.

    try:
        # Delete variable
        api_instance.delete_variable(variable_key)
    except ApiException as e:
        print("Exception when calling VariableApi->delete_variable: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **variable_key** | **str**| The Variable Key. | 

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

# **get_variable**
> Variable get_variable(variable_key)

Get a variable by key

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
    api_instance = airflow_client.VariableApi(api_client)
    variable_key = 'variable_key_example' # str | The Variable Key.

    try:
        # Get a variable by key
        api_response = api_instance.get_variable(variable_key)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VariableApi->get_variable: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **variable_key** | **str**| The Variable Key. | 

### Return type

[**Variable**](Variable.md)

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

# **get_variables**
> VariableCollection get_variables(limit=limit, offset=offset)

Get all variables

The collection does not contain data. To get data, you must get a single entity.

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
    api_instance = airflow_client.VariableApi(api_client)
    limit = 100 # int | The numbers of items to return. (optional) (default to 100)
offset = 56 # int | The number of items to skip before starting to collect the result set. (optional)

    try:
        # Get all variables
        api_response = api_instance.get_variables(limit=limit, offset=offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VariableApi->get_variables: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **int**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**VariableCollection**](VariableCollection.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of variables. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **patch_variable**
> Variable patch_variable(variable_key, variable, update_mask=update_mask)

Update a variable by key

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
    api_instance = airflow_client.VariableApi(api_client)
    variable_key = 'variable_key_example' # str | The Variable Key.
variable = airflow_client.Variable() # Variable | 
update_mask = ['update_mask_example'] # list[str] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  (optional)

    try:
        # Update a variable by key
        api_response = api_instance.patch_variable(variable_key, variable, update_mask=update_mask)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VariableApi->patch_variable: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **variable_key** | **str**| The Variable Key. | 
 **variable** | [**Variable**](Variable.md)|  | 
 **update_mask** | [**list[str]**](str.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**Variable**](Variable.md)

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

# **post_variables**
> Variable post_variables(variable)

Create a variable

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
    api_instance = airflow_client.VariableApi(api_client)
    variable = airflow_client.Variable() # Variable | 

    try:
        # Create a variable
        api_response = api_instance.post_variables(variable)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VariableApi->post_variables: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **variable** | [**Variable**](Variable.md)|  | 

### Return type

[**Variable**](Variable.md)

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

