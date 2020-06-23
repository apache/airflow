# \TaskInstanceApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetExtraLinks**](TaskInstanceApi.md#GetExtraLinks) | **Get** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links | Get extra links for task instance
[**GetLog**](TaskInstanceApi.md#GetLog) | **Get** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number} | Get logs for a task instance
[**GetTaskInstance**](TaskInstanceApi.md#GetTaskInstance) | **Get** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id} | Get a task instance
[**GetTaskInstances**](TaskInstanceApi.md#GetTaskInstances) | **Get** /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances | Get a list of task instance of DAG.
[**GetTaskInstancesBatch**](TaskInstanceApi.md#GetTaskInstancesBatch) | **Post** /dags/~/dagRuns/~/taskInstances/list | Get list of task instances from all DAGs and DAG Runs.



## GetExtraLinks

> ExtraLinkCollection GetExtraLinks(ctx, dagId, dagRunId, taskId)

Get extra links for task instance

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dagId** | **string**| The DAG ID. | 
**dagRunId** | **string**| The DAG Run ID. | 
**taskId** | **string**| The Task ID. | 

### Return type

[**ExtraLinkCollection**](ExtraLinkCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetLog

> InlineResponse200 GetLog(ctx, dagId, dagRunId, taskId, taskTryNumber, optional)

Get logs for a task instance

Get logs for a specific task instance and its try number

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dagId** | **string**| The DAG ID. | 
**dagRunId** | **string**| The DAG Run ID. | 
**taskId** | **string**| The Task ID. | 
**taskTryNumber** | **int32**| The Task Try Number. | 
 **optional** | ***GetLogOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a GetLogOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------




 **fullContent** | **optional.Bool**| A full content will be returned. By default, only the first fragment will be returned.  | 
 **token** | **optional.String**| A token that allows you to continue fetching logs. If passed, it will specify the location from which the download should be continued.  | 

### Return type

[**InlineResponse200**](inline_response_200.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTaskInstance

> TaskInstance GetTaskInstance(ctx, dagId, dagRunId, taskId)

Get a task instance

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dagId** | **string**| The DAG ID. | 
**dagRunId** | **string**| The DAG Run ID. | 
**taskId** | **string**| The Task ID. | 

### Return type

[**TaskInstance**](TaskInstance.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTaskInstances

> TaskInstanceCollection GetTaskInstances(ctx, dagId, dagRunId, optional)

Get a list of task instance of DAG.

This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve DAG Runs for all DAGs and DAG Runs. 

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**dagId** | **string**| The DAG ID. | 
**dagRunId** | **string**| The DAG Run ID. | 
 **optional** | ***GetTaskInstancesOpts** | optional parameters | nil if no parameters

### Optional Parameters

Optional parameters are passed through a pointer to a GetTaskInstancesOpts struct


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


 **executionDateGte** | **optional.Time**| Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | 
 **executionDateLte** | **optional.Time**| Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | 
 **startDateGte** | **optional.Time**| Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | 
 **startDateLte** | **optional.Time**| Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | 
 **endDateGte** | **optional.Time**| Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | 
 **endDateLte** | **optional.Time**| Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | 
 **durationGte** | **optional.Float32**| Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  | 
 **durationLte** | **optional.Float32**| Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  | 
 **state** | [**optional.Interface of []string**](string.md)| The value can be repeated to retrieve multiple matching values (OR condition). | 
 **pool** | [**optional.Interface of []string**](string.md)| The value can be repeated to retrieve multiple matching values (OR condition). | 
 **queue** | [**optional.Interface of []string**](string.md)| The value can be repeated to retrieve multiple matching values (OR condition). | 
 **limit** | **optional.Int32**| The numbers of items to return. | [default to 100]
 **offset** | **optional.Int32**| The number of items to skip before starting to collect the result set. | 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTaskInstancesBatch

> TaskInstanceCollection GetTaskInstancesBatch(ctx, listTaskInstanceForm)

Get list of task instances from all DAGs and DAG Runs.

This endpoint is a POST to allow filtering across a large number of DAG IDs, where as a GET it would run in to maximum HTTP request URL lengthlimits 

### Required Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**listTaskInstanceForm** | [**ListTaskInstanceForm**](ListTaskInstanceForm.md)|  | 

### Return type

[**TaskInstanceCollection**](TaskInstanceCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

