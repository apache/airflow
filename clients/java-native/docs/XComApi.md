# XComApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deleteXcomEntry**](XComApi.md#deleteXcomEntry) | **DELETE** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Delete an XCom entry
[**getXcomEntries**](XComApi.md#getXcomEntries) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Get all XCom entries
[**getXcomEntry**](XComApi.md#getXcomEntry) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Get an XCom entry
[**patchXcomEntry**](XComApi.md#patchXcomEntry) | **PATCH** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key} | Update an XCom entry
[**postXcomEntries**](XComApi.md#postXcomEntries) | **POST** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries | Create an XCom entry



## deleteXcomEntry

> deleteXcomEntry(dagId, dagRunId, taskId, xcomKey)

Delete an XCom entry

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.XComApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        XComApi apiInstance = new XComApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        String xcomKey = "xcomKey_example"; // String | The XCom Key.
        try {
            apiInstance.deleteXcomEntry(dagId, dagRunId, taskId, xcomKey);
        } catch (ApiException e) {
            System.err.println("Exception when calling XComApi#deleteXcomEntry");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. |
 **dagRunId** | **String**| The DAG Run ID. |
 **taskId** | **String**| The Task ID. |
 **xcomKey** | **String**| The XCom Key. |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | No content. |  -  |
| **400** | Client specified an invalid argument. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |


## getXcomEntries

> XComCollection getXcomEntries(dagId, dagRunId, taskId, limit, offset)

Get all XCom entries

This endpoint allows specifying &#x60;~&#x60; as the dag_id, dag_run_id, task_id to retrieve XCOM entries for for all DAGs, DAG Runs and task instances.

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.XComApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        XComApi apiInstance = new XComApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        Integer limit = 100; // Integer | The numbers of items to return.
        Integer offset = 56; // Integer | The number of items to skip before starting to collect the result set.
        try {
            XComCollection result = apiInstance.getXcomEntries(dagId, dagRunId, taskId, limit, offset);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling XComApi#getXcomEntries");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. |
 **dagRunId** | **String**| The DAG Run ID. |
 **taskId** | **String**| The Task ID. |
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional]

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
| **200** | List of XCom entries. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |


## getXcomEntry

> XCom getXcomEntry(dagId, dagRunId, taskId, xcomKey)

Get an XCom entry

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.XComApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        XComApi apiInstance = new XComApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        String xcomKey = "xcomKey_example"; // String | The XCom Key.
        try {
            XCom result = apiInstance.getXcomEntry(dagId, dagRunId, taskId, xcomKey);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling XComApi#getXcomEntry");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. |
 **dagRunId** | **String**| The DAG Run ID. |
 **taskId** | **String**| The Task ID. |
 **xcomKey** | **String**| The XCom Key. |

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
| **200** | Successful response. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |


## patchXcomEntry

> XCom patchXcomEntry(dagId, dagRunId, taskId, xcomKey, xcom, updateMask)

Update an XCom entry

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.XComApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        XComApi apiInstance = new XComApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        String xcomKey = "xcomKey_example"; // String | The XCom Key.
        XCom xcom = new XCom(); // XCom | 
        List<String> updateMask = Arrays.asList(); // List<String> | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields. 
        try {
            XCom result = apiInstance.patchXcomEntry(dagId, dagRunId, taskId, xcomKey, xcom, updateMask);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling XComApi#patchXcomEntry");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. |
 **dagRunId** | **String**| The DAG Run ID. |
 **taskId** | **String**| The Task ID. |
 **xcomKey** | **String**| The XCom Key. |
 **xcom** | [**XCom**](XCom.md)|  |
 **updateMask** | [**List&lt;String&gt;**](String.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional]

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
| **200** | Successful response. |  -  |
| **400** | Client specified an invalid argument. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |


## postXcomEntries

> XCom postXcomEntries(dagId, dagRunId, taskId, xcom)

Create an XCom entry

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.XComApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        XComApi apiInstance = new XComApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        XCom xcom = new XCom(); // XCom | 
        try {
            XCom result = apiInstance.postXcomEntries(dagId, dagRunId, taskId, xcom);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling XComApi#postXcomEntries");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. |
 **dagRunId** | **String**| The DAG Run ID. |
 **taskId** | **String**| The Task ID. |
 **xcom** | [**XCom**](XCom.md)|  |

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
| **200** | Successful response. |  -  |
| **400** | Client specified an invalid argument. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |

