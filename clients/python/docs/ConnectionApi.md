# airflow_client.ConnectionApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_connection**](ConnectionApi.md#delete_connection) | **DELETE** /connections/{connection_id} | Delete a connection entry
[**get_connection**](ConnectionApi.md#get_connection) | **GET** /connections/{connection_id} | Get a connection entry
[**get_connections**](ConnectionApi.md#get_connections) | **GET** /connections | Get all connection entries
[**patch_connection**](ConnectionApi.md#patch_connection) | **PATCH** /connections/{connection_id} | Update a connection entry
[**post_connection**](ConnectionApi.md#post_connection) | **POST** /connections | Create connection entry


# **delete_connection**
> delete_connection(connection_id)

Delete a connection entry

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
    api_instance = airflow_client.ConnectionApi(api_client)
    connection_id = 'connection_id_example' # str | The Connection ID.

    try:
        # Delete a connection entry
        api_instance.delete_connection(connection_id)
    except ApiException as e:
        print("Exception when calling ConnectionApi->delete_connection: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connection_id** | **str**| The Connection ID. | 

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

# **get_connection**
> Connection get_connection(connection_id)

Get a connection entry

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
    api_instance = airflow_client.ConnectionApi(api_client)
    connection_id = 'connection_id_example' # str | The Connection ID.

    try:
        # Get a connection entry
        api_response = api_instance.get_connection(connection_id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ConnectionApi->get_connection: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connection_id** | **str**| The Connection ID. | 

### Return type

[**Connection**](Connection.md)

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

# **get_connections**
> ConnectionCollection get_connections(limit=limit, offset=offset)

Get all connection entries

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
    api_instance = airflow_client.ConnectionApi(api_client)
    limit = 100 # int | The numbers of items to return. (optional) (default to 100)
offset = 56 # int | The number of items to skip before starting to collect the result set. (optional)

    try:
        # Get all connection entries
        api_response = api_instance.get_connections(limit=limit, offset=offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ConnectionApi->get_connections: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **int**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**ConnectionCollection**](ConnectionCollection.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of connection entry. |  -  |
**401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
**403** | Client does not have sufficient permission. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **patch_connection**
> Connection patch_connection(connection_id, connection, update_mask=update_mask)

Update a connection entry

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
    api_instance = airflow_client.ConnectionApi(api_client)
    connection_id = 'connection_id_example' # str | The Connection ID.
connection = airflow_client.Connection() # Connection | 
update_mask = ['update_mask_example'] # list[str] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  (optional)

    try:
        # Update a connection entry
        api_response = api_instance.patch_connection(connection_id, connection, update_mask=update_mask)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ConnectionApi->patch_connection: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connection_id** | **str**| The Connection ID. | 
 **connection** | [**Connection**](Connection.md)|  | 
 **update_mask** | [**list[str]**](str.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**Connection**](Connection.md)

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

# **post_connection**
> Connection post_connection(connection)

Create connection entry

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
    api_instance = airflow_client.ConnectionApi(api_client)
    connection = airflow_client.Connection() # Connection | 

    try:
        # Create connection entry
        api_response = api_instance.post_connection(connection)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ConnectionApi->post_connection: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connection** | [**Connection**](Connection.md)|  | 

### Return type

[**Connection**](Connection.md)

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

