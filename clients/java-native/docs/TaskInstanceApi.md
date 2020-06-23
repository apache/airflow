# TaskInstanceApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getExtraLinks**](TaskInstanceApi.md#getExtraLinks) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links | Get extra links for task instance
[**getLog**](TaskInstanceApi.md#getLog) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number} | Get logs for a task instance
[**getTaskInstance**](TaskInstanceApi.md#getTaskInstance) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id} | Get a task instance
[**getTaskInstances**](TaskInstanceApi.md#getTaskInstances) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances | Get a list of task instance of DAG.
[**getTaskInstancesBatch**](TaskInstanceApi.md#getTaskInstancesBatch) | **POST** /dags/~/dagRuns/~/taskInstances/list | Get list of task instances from all DAGs and DAG Runs.



## getExtraLinks

> ExtraLinkCollection getExtraLinks(dagId, dagRunId, taskId)

Get extra links for task instance

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.TaskInstanceApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        TaskInstanceApi apiInstance = new TaskInstanceApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        try {
            ExtraLinkCollection result = apiInstance.getExtraLinks(dagId, dagRunId, taskId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TaskInstanceApi#getExtraLinks");
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
| **200** | Successful response. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |


## getLog

> InlineResponse200 getLog(dagId, dagRunId, taskId, taskTryNumber, fullContent, token)

Get logs for a task instance

Get logs for a specific task instance and its try number

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.TaskInstanceApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        TaskInstanceApi apiInstance = new TaskInstanceApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        Integer taskTryNumber = 56; // Integer | The Task Try Number.
        Boolean fullContent = true; // Boolean | A full content will be returned. By default, only the first fragment will be returned. 
        String token = "token_example"; // String | A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued. 
        try {
            InlineResponse200 result = apiInstance.getLog(dagId, dagRunId, taskId, taskTryNumber, fullContent, token);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TaskInstanceApi#getLog");
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
 **taskTryNumber** | **Integer**| The Task Try Number. |
 **fullContent** | **Boolean**| A full content will be returned. By default, only the first fragment will be returned.  | [optional]
 **token** | **String**| A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued.  | [optional]

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
| **200** | Content of logs. |  -  |
| **400** | Client specified an invalid argument. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |


## getTaskInstance

> TaskInstance getTaskInstance(dagId, dagRunId, taskId)

Get a task instance

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.TaskInstanceApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        TaskInstanceApi apiInstance = new TaskInstanceApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        String taskId = "taskId_example"; // String | The Task ID.
        try {
            TaskInstance result = apiInstance.getTaskInstance(dagId, dagRunId, taskId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TaskInstanceApi#getTaskInstance");
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
| **200** | Successful response. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |


## getTaskInstances

> TaskInstanceCollection getTaskInstances(dagId, dagRunId, executionDateGte, executionDateLte, startDateGte, startDateLte, endDateGte, endDateLte, durationGte, durationLte, state, pool, queue, limit, offset)

Get a list of task instance of DAG.

This endpoint allows specifying &#x60;~&#x60; as the dag_id, dag_run_id to retrieve DAG Runs for all DAGs and DAG Runs. 

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.TaskInstanceApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        TaskInstanceApi apiInstance = new TaskInstanceApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        OffsetDateTime executionDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period. 
        OffsetDateTime executionDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period. 
        OffsetDateTime startDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period. 
        OffsetDateTime startDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
        OffsetDateTime endDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period. 
        OffsetDateTime endDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
        BigDecimal durationGte = new BigDecimal(); // BigDecimal | Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period. 
        BigDecimal durationLte = new BigDecimal(); // BigDecimal | Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range. 
        List<String> state = Arrays.asList(); // List<String> | The value can be repeated to retrieve multiple matching values (OR condition).
        List<String> pool = Arrays.asList(); // List<String> | The value can be repeated to retrieve multiple matching values (OR condition).
        List<String> queue = Arrays.asList(); // List<String> | The value can be repeated to retrieve multiple matching values (OR condition).
        Integer limit = 100; // Integer | The numbers of items to return.
        Integer offset = 56; // Integer | The number of items to skip before starting to collect the result set.
        try {
            TaskInstanceCollection result = apiInstance.getTaskInstances(dagId, dagRunId, executionDateGte, executionDateLte, startDateGte, startDateLte, endDateGte, endDateLte, durationGte, durationLte, state, pool, queue, limit, offset);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TaskInstanceApi#getTaskInstances");
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
 **executionDateGte** | **OffsetDateTime**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional]
 **executionDateLte** | **OffsetDateTime**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional]
 **startDateGte** | **OffsetDateTime**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional]
 **startDateLte** | **OffsetDateTime**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional]
 **endDateGte** | **OffsetDateTime**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional]
 **endDateLte** | **OffsetDateTime**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional]
 **durationGte** | **BigDecimal**| Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  | [optional]
 **durationLte** | **BigDecimal**| Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  | [optional]
 **state** | [**List&lt;String&gt;**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional]
 **pool** | [**List&lt;String&gt;**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional]
 **queue** | [**List&lt;String&gt;**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional]
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional]

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
| **200** | List of task instances. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |


## getTaskInstancesBatch

> TaskInstanceCollection getTaskInstancesBatch(listTaskInstanceForm)

Get list of task instances from all DAGs and DAG Runs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.TaskInstanceApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        TaskInstanceApi apiInstance = new TaskInstanceApi(defaultClient);
        ListTaskInstanceForm listTaskInstanceForm = new ListTaskInstanceForm(); // ListTaskInstanceForm | 
        try {
            TaskInstanceCollection result = apiInstance.getTaskInstancesBatch(listTaskInstanceForm);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling TaskInstanceApi#getTaskInstancesBatch");
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
 **listTaskInstanceForm** | [**ListTaskInstanceForm**](ListTaskInstanceForm.md)|  |

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
| **200** | List of task instances. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |
| **404** | A specified resource is not found. |  -  |

