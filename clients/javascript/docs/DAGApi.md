# AirflowApiStable.DAGApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getDag**](DAGApi.md#getDag) | **GET** /dags/{dag_id} | Get basic information about a DAG
[**getDagDetails**](DAGApi.md#getDagDetails) | **GET** /dags/{dag_id}/details | Get a simplified representation of DAG.
[**getDagSource**](DAGApi.md#getDagSource) | **GET** /dagSources/{file_token} | Get source code using file token
[**getDags**](DAGApi.md#getDags) | **GET** /dags | Get all DAGs
[**getTask**](DAGApi.md#getTask) | **GET** /dags/{dag_id}/tasks/{task_id} | Get simplified representation of a task.
[**getTasks**](DAGApi.md#getTasks) | **GET** /dags/{dag_id}/tasks | Get tasks for DAG
[**patchDag**](DAGApi.md#patchDag) | **PATCH** /dags/{dag_id} | Update a DAG
[**postClearTaskInstances**](DAGApi.md#postClearTaskInstances) | **POST** /dags/{dag_id}/clearTaskInstances | Clears a set of task instances associated with the DAG for a specified date range.



## getDag

> DAG getDag(dagId)

Get basic information about a DAG

Presents only information available in database (DAGModel). If you need detailed information, consider using GET /dags/{dag_id}/detail. 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
apiInstance.getDag(dagId, (error, data, response) => {
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

### Return type

[**DAG**](DAG.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDagDetails

> DAGDetail getDagDetails(dagId)

Get a simplified representation of DAG.

The response contains many DAG attributes, so the response can be large. If possible, consider using GET /dags/{dag_id}. 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
apiInstance.getDagDetails(dagId, (error, data, response) => {
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

### Return type

[**DAGDetail**](DAGDetail.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDagSource

> InlineResponse2001 getDagSource(fileToken)

Get source code using file token

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let fileToken = "fileToken_example"; // String | The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading an non-DAG file. This also ensures API extensibility, because the format of encrypted data may change. 
apiInstance.getDagSource(fileToken, (error, data, response) => {
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
 **fileToken** | **String**| The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading an non-DAG file. This also ensures API extensibility, because the format of encrypted data may change.  | 

### Return type

[**InlineResponse2001**](InlineResponse2001.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDags

> DAGCollection getDags(opts)

Get all DAGs

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let opts = {
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56 // Number | The number of items to skip before starting to collect the result set.
};
apiInstance.getDags(opts, (error, data, response) => {
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
 **limit** | **Number**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Number**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**DAGCollection**](DAGCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getTask

> Task getTask(dagId, taskId)

Get simplified representation of a task.

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
let taskId = "taskId_example"; // String | The Task ID.
apiInstance.getTask(dagId, taskId, (error, data, response) => {
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
 **taskId** | **String**| The Task ID. | 

### Return type

[**Task**](Task.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getTasks

> TaskCollection getTasks(dagId)

Get tasks for DAG

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
apiInstance.getTasks(dagId, (error, data, response) => {
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

### Return type

[**TaskCollection**](TaskCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## patchDag

> DAG patchDag(dagId, DAG)

Update a DAG

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
let DAG = new AirflowApiStable.DAG(); // DAG | 
apiInstance.patchDag(dagId, DAG, (error, data, response) => {
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
 **DAG** | [**DAG**](DAG.md)|  | 

### Return type

[**DAG**](DAG.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


## postClearTaskInstances

> TaskInstanceReferenceCollection postClearTaskInstances(dagId, clearTaskInstance)

Clears a set of task instances associated with the DAG for a specified date range.

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGApi();
let dagId = "dagId_example"; // String | The DAG ID.
let clearTaskInstance = new AirflowApiStable.ClearTaskInstance(); // ClearTaskInstance | Parameters of action
apiInstance.postClearTaskInstances(dagId, clearTaskInstance, (error, data, response) => {
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
 **clearTaskInstance** | [**ClearTaskInstance**](ClearTaskInstance.md)| Parameters of action | 

### Return type

[**TaskInstanceReferenceCollection**](TaskInstanceReferenceCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

