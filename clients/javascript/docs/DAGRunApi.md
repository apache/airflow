# AirflowApiStable.DAGRunApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deleteDagRun**](DAGRunApi.md#deleteDagRun) | **DELETE** /dags/{dag_id}/dagRuns/{dag_run_id} | Delete a DAG Run
[**getDagRun**](DAGRunApi.md#getDagRun) | **GET** /dags/{dag_id}/dagRuns/{dag_run_id} | Get a DAG Run
[**getDagRuns**](DAGRunApi.md#getDagRuns) | **GET** /dags/{dag_id}/dagRuns | Get all DAG Runs
[**getDagRunsBatch**](DAGRunApi.md#getDagRunsBatch) | **POST** /dags/~/dagRuns/list | Get all DAG Runs from aall DAGs.
[**patchDagRun**](DAGRunApi.md#patchDagRun) | **PATCH** /dags/{dag_id}/dagRuns/{dag_run_id} | Update a DAG Run
[**postDagRun**](DAGRunApi.md#postDagRun) | **POST** /dags/{dag_id}/dagRuns/{dag_run_id} | Trigger a DAG Run



## deleteDagRun

> deleteDagRun(dagId, dagRunId)

Delete a DAG Run

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
apiInstance.deleteDagRun(dagId, dagRunId, (error, data, response) => {
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

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDagRun

> DAGRun getDagRun(dagId, dagRunId)

Get a DAG Run

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
apiInstance.getDagRun(dagId, dagRunId, (error, data, response) => {
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

### Return type

[**DAGRun**](DAGRun.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDagRuns

> DAGRunCollection getDagRuns(dagId, opts)

Get all DAG Runs

This endpoint allows specifying &#x60;~&#x60; as the dag_id to retrieve DAG Runs for all DAGs. 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let dagId = "dagId_example"; // String | The DAG ID.
let opts = {
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56, // Number | The number of items to skip before starting to collect the result set.
  'executionDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period. 
  'executionDateLte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period. 
  'startDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period. 
  'startDateLte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
  'endDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period. 
  'endDateLte': new Date("2013-10-20T19:20:30+01:00") // Date | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
};
apiInstance.getDagRuns(dagId, opts, (error, data, response) => {
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
 **limit** | **Number**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Number**| The number of items to skip before starting to collect the result set. | [optional] 
 **executionDateGte** | **Date**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional] 
 **executionDateLte** | **Date**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional] 
 **startDateGte** | **Date**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional] 
 **startDateLte** | **Date**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
 **endDateGte** | **Date**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional] 
 **endDateLte** | **Date**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 

### Return type

[**DAGRunCollection**](DAGRunCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getDagRunsBatch

> DAGRunCollection getDagRunsBatch(listDagRunsForm)

Get all DAG Runs from aall DAGs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let listDagRunsForm = new AirflowApiStable.ListDagRunsForm(); // ListDagRunsForm | 
apiInstance.getDagRunsBatch(listDagRunsForm, (error, data, response) => {
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
 **listDagRunsForm** | [**ListDagRunsForm**](ListDagRunsForm.md)|  | 

### Return type

[**DAGRunCollection**](DAGRunCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


## patchDagRun

> DAGRun patchDagRun(dagId, dagRunId, dAGRun, opts)

Update a DAG Run

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let dAGRun = new AirflowApiStable.DAGRun(); // DAGRun | 
let opts = {
  'updateMask': ["null"] // [String] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields. 
};
apiInstance.patchDagRun(dagId, dagRunId, dAGRun, opts, (error, data, response) => {
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
 **dAGRun** | [**DAGRun**](DAGRun.md)|  | 
 **updateMask** | [**[String]**](String.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**DAGRun**](DAGRun.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


## postDagRun

> DAGRun postDagRun(dagId, dagRunId, dAGRun)

Trigger a DAG Run

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.DAGRunApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let dAGRun = new AirflowApiStable.DAGRun(); // DAGRun | 
apiInstance.postDagRun(dagId, dagRunId, dAGRun, (error, data, response) => {
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
 **dAGRun** | [**DAGRun**](DAGRun.md)|  | 

### Return type

[**DAGRun**](DAGRun.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

