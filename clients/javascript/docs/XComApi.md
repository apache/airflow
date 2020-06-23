# AirflowApiStable.XComApi

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

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.XComApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let xcomKey = "xcomKey_example"; // String | The XCom Key.
apiInstance.deleteXcomEntry(dagId, dagRunId, taskId, xcomKey, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully.');
  }
});
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


## getXcomEntries

> XComCollection getXcomEntries(dagId, dagRunId, taskId, opts)

Get all XCom entries

This endpoint allows specifying &#x60;~&#x60; as the dag_id, dag_run_id, task_id to retrieve XCOM entries for for all DAGs, DAG Runs and task instances.

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.XComApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let opts = {
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56 // Number | The number of items to skip before starting to collect the result set.
};
apiInstance.getXcomEntries(dagId, dagRunId, taskId, opts, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. | 
 **dagRunId** | **String**| The DAG Run ID. | 
 **taskId** | **String**| The Task ID. | 
 **limit** | **Number**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Number**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**XComCollection**](XComCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getXcomEntry

> XCom getXcomEntry(dagId, dagRunId, taskId, xcomKey)

Get an XCom entry

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.XComApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let xcomKey = "xcomKey_example"; // String | The XCom Key.
apiInstance.getXcomEntry(dagId, dagRunId, taskId, xcomKey, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
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


## patchXcomEntry

> XCom patchXcomEntry(dagId, dagRunId, taskId, xcomKey, xCom, opts)

Update an XCom entry

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.XComApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let xcomKey = "xcomKey_example"; // String | The XCom Key.
let xCom = new AirflowApiStable.XCom(); // XCom | 
let opts = {
  'updateMask': ["null"] // [String] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields. 
};
apiInstance.patchXcomEntry(dagId, dagRunId, taskId, xcomKey, xCom, opts, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. | 
 **dagRunId** | **String**| The DAG Run ID. | 
 **taskId** | **String**| The Task ID. | 
 **xcomKey** | **String**| The XCom Key. | 
 **xCom** | [**XCom**](XCom.md)|  | 
 **updateMask** | [**[String]**](String.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**XCom**](XCom.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


## postXcomEntries

> XCom postXcomEntries(dagId, dagRunId, taskId, xCom)

Create an XCom entry

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.XComApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let xCom = new AirflowApiStable.XCom(); // XCom | 
apiInstance.postXcomEntries(dagId, dagRunId, taskId, xCom, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dagId** | **String**| The DAG ID. | 
 **dagRunId** | **String**| The DAG Run ID. | 
 **taskId** | **String**| The Task ID. | 
 **xCom** | [**XCom**](XCom.md)|  | 

### Return type

[**XCom**](XCom.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

