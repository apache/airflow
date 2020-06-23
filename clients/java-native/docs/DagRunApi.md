# DagRunApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deleteDagRun**](DagRunApi.md#deleteDagRun) | **DELETE** /dags/{dag_id}/dagRuns/{dag_run_id} | Delete a DAG Run
[**getDagRun**](DagRunApi.md#getDagRun) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id} | Get a DAG Run
[**getDagRuns**](DagRunApi.md#getDagRuns) | **GET** /dags/{dag_id}/dagRuns | Get all DAG Runs
[**getDagRunsBatch**](DagRunApi.md#getDagRunsBatch) | **POST** /dags/~/dagRuns/list | Get all DAG Runs from aall DAGs.
[**patchDagRun**](DagRunApi.md#patchDagRun) | **PATCH** /dags/{dag_id}/dagRuns/{dag_run_id} | Update a DAG Run
[**postDagRun**](DagRunApi.md#postDagRun) | **POST** /dags/{dag_id}/dagRuns/{dag_run_id} | Trigger a DAG Run



## deleteDagRun

> deleteDagRun(dagId, dagRunId)

Delete a DAG Run

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        try {
            apiInstance.deleteDagRun(dagId, dagRunId);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#deleteDagRun");
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


## getDagRun

> DAGRun getDagRun(dagId, dagRunId)

Get a DAG Run

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        try {
            DAGRun result = apiInstance.getDagRun(dagId, dagRunId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#getDagRun");
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

### Return type

[**DAGRun**](DAGRun.md)

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


## getDagRuns

> DAGRunCollection getDagRuns(dagId, limit, offset, executionDateGte, executionDateLte, startDateGte, startDateLte, endDateGte, endDateLte)

Get all DAG Runs

This endpoint allows specifying &#x60;~&#x60; as the dag_id to retrieve DAG Runs for all DAGs. 

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        Integer limit = 100; // Integer | The numbers of items to return.
        Integer offset = 56; // Integer | The number of items to skip before starting to collect the result set.
        OffsetDateTime executionDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period. 
        OffsetDateTime executionDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period. 
        OffsetDateTime startDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period. 
        OffsetDateTime startDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
        OffsetDateTime endDateGte = new OffsetDateTime(); // OffsetDateTime | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period. 
        OffsetDateTime endDateLte = new OffsetDateTime(); // OffsetDateTime | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
        try {
            DAGRunCollection result = apiInstance.getDagRuns(dagId, limit, offset, executionDateGte, executionDateLte, startDateGte, startDateLte, endDateGte, endDateLte);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#getDagRuns");
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
 **limit** | **Integer**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Integer**| The number of items to skip before starting to collect the result set. | [optional]
 **executionDateGte** | **OffsetDateTime**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional]
 **executionDateLte** | **OffsetDateTime**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional]
 **startDateGte** | **OffsetDateTime**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional]
 **startDateLte** | **OffsetDateTime**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional]
 **endDateGte** | **OffsetDateTime**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional]
 **endDateLte** | **OffsetDateTime**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional]

### Return type

[**DAGRunCollection**](DAGRunCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of DAG Runs. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |


## getDagRunsBatch

> DAGRunCollection getDagRunsBatch(listDagRunsForm)

Get all DAG Runs from aall DAGs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        ListDagRunsForm listDagRunsForm = new ListDagRunsForm(); // ListDagRunsForm | 
        try {
            DAGRunCollection result = apiInstance.getDagRunsBatch(listDagRunsForm);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#getDagRunsBatch");
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
 **listDagRunsForm** | [**ListDagRunsForm**](ListDagRunsForm.md)|  |

### Return type

[**DAGRunCollection**](DAGRunCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of DAG Runs. |  -  |
| **400** | Client specified an invalid argument. |  -  |
| **401** | Request not authenticated due to missing, invalid, authentication info. |  -  |
| **403** | Client does not have sufficient permission. |  -  |


## patchDagRun

> DAGRun patchDagRun(dagId, dagRunId, daGRun, updateMask)

Update a DAG Run

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        DAGRun daGRun = new DAGRun(); // DAGRun | 
        List<String> updateMask = Arrays.asList(); // List<String> | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields. 
        try {
            DAGRun result = apiInstance.patchDagRun(dagId, dagRunId, daGRun, updateMask);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#patchDagRun");
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
 **daGRun** | [**DAGRun**](DAGRun.md)|  |
 **updateMask** | [**List&lt;String&gt;**](String.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional]

### Return type

[**DAGRun**](DAGRun.md)

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


## postDagRun

> DAGRun postDagRun(dagId, dagRunId, daGRun)

Trigger a DAG Run

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.DagRunApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        DagRunApi apiInstance = new DagRunApi(defaultClient);
        String dagId = "dagId_example"; // String | The DAG ID.
        String dagRunId = "dagRunId_example"; // String | The DAG Run ID.
        DAGRun daGRun = new DAGRun(); // DAGRun | 
        try {
            DAGRun result = apiInstance.postDagRun(dagId, dagRunId, daGRun);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling DagRunApi#postDagRun");
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
 **daGRun** | [**DAGRun**](DAGRun.md)|  |

### Return type

[**DAGRun**](DAGRun.md)

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
| **409** | The resource that a client tried to create already exists. |  -  |
| **403** | Client does not have sufficient permission. |  -  |

