# AirflowApiStable.TaskInstanceApi

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

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.TaskInstanceApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
apiInstance.getExtraLinks(dagId, dagRunId, taskId, (error, data, response) => {
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

### Return type

[**ExtraLinkCollection**](ExtraLinkCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getLog

> InlineResponse200 getLog(dagId, dagRunId, taskId, taskTryNumber, opts)

Get logs for a task instance

Get logs for a specific task instance and its try number

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.TaskInstanceApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
let taskTryNumber = 56; // Number | The Task Try Number.
let opts = {
  'fullContent': true, // Boolean | A full content will be returned. By default, only the first fragment will be returned. 
  'token': "token_example" // String | A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued. 
};
apiInstance.getLog(dagId, dagRunId, taskId, taskTryNumber, opts, (error, data, response) => {
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
 **taskTryNumber** | **Number**| The Task Try Number. | 
 **fullContent** | **Boolean**| A full content will be returned. By default, only the first fragment will be returned.  | [optional] 
 **token** | **String**| A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued.  | [optional] 

### Return type

[**InlineResponse200**](InlineResponse200.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json, text/plain


## getTaskInstance

> TaskInstance getTaskInstance(dagId, dagRunId, taskId)

Get a task instance

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.TaskInstanceApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let taskId = "taskId_example"; // String | The Task ID.
apiInstance.getTaskInstance(dagId, dagRunId, taskId, (error, data, response) => {
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

### Return type

[**TaskInstance**](TaskInstance.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getTaskInstances

> TaskInstanceCollection getTaskInstances(dagId, dagRunId, opts)

Get a list of task instance of DAG.

This endpoint allows specifying &#x60;~&#x60; as the dag_id, dag_run_id to retrieve DAG Runs for all DAGs and DAG Runs. 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.TaskInstanceApi();
let dagId = "dagId_example"; // String | The DAG ID.
let dagRunId = "dagRunId_example"; // String | The DAG Run ID.
let opts = {
  'executionDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period. 
  'executionDateLte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period. 
  'startDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period. 
  'startDateLte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
  'endDateGte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period. 
  'endDateLte': new Date("2013-10-20T19:20:30+01:00"), // Date | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period. 
  'durationGte': 3.4, // Number | Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period. 
  'durationLte': 3.4, // Number | Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range. 
  'state': ["null"], // [String] | The value can be repeated to retrieve multiple matching values (OR condition).
  'pool': ["null"], // [String] | The value can be repeated to retrieve multiple matching values (OR condition).
  'queue': ["null"], // [String] | The value can be repeated to retrieve multiple matching values (OR condition).
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56 // Number | The number of items to skip before starting to collect the result set.
};
apiInstance.getTaskInstances(dagId, dagRunId, opts, (error, data, response) => {
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
 **executionDateGte** | **Date**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional] 
 **executionDateLte** | **Date**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional] 
 **startDateGte** | **Date**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional] 
 **startDateLte** | **Date**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
 **endDateGte** | **Date**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional] 
 **endDateLte** | **Date**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
 **durationGte** | **Number**| Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  | [optional] 
 **durationLte** | **Number**| Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  | [optional] 
 **state** | [**[String]**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **pool** | [**[String]**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **queue** | [**[String]**](String.md)| The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
 **limit** | **Number**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Number**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getTaskInstancesBatch

> TaskInstanceCollection getTaskInstancesBatch(listTaskInstanceForm)

Get list of task instances from all DAGs and DAG Runs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.TaskInstanceApi();
let listTaskInstanceForm = new AirflowApiStable.ListTaskInstanceForm(); // ListTaskInstanceForm | 
apiInstance.getTaskInstancesBatch(listTaskInstanceForm, (error, data, response) => {
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
 **listTaskInstanceForm** | [**ListTaskInstanceForm**](ListTaskInstanceForm.md)|  | 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

