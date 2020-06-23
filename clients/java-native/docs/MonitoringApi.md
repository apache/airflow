# MonitoringApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getHealth**](MonitoringApi.md#getHealth) | **GET** /health | Checks if the API works
[**getVersion**](MonitoringApi.md#getVersion) | **GET** /version | Get version information



## getHealth

> String getHealth()

Checks if the API works

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.MonitoringApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        MonitoringApi apiInstance = new MonitoringApi(defaultClient);
        try {
            String result = apiInstance.getHealth();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling MonitoringApi#getHealth");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

**String**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/plain

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | It should always return \&quot;OK\&quot; |  -  |


## getVersion

> VersionInfo getVersion()

Get version information

### Example

```java
// Import classes:
import org.apache.airflow.client.ApiClient;
import org.apache.airflow.client.ApiException;
import org.apache.airflow.client.Configuration;
import org.apache.airflow.client.models.*;
import org.apache.airflow.client.api.MonitoringApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost/api/v1");

        MonitoringApi apiInstance = new MonitoringApi(defaultClient);
        try {
            VersionInfo result = apiInstance.getVersion();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling MonitoringApi#getVersion");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**VersionInfo**](VersionInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Return current configuration. |  -  |

