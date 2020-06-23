# AirflowApiStable.EventLogApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getEventLog**](EventLogApi.md#getEventLog) | **GET** /eventLogs/{event_log_id} | Get a log entry
[**getEventLogs**](EventLogApi.md#getEventLogs) | **GET** /eventLogs | Get all log entries from event log



## getEventLog

> EventLog getEventLog(eventLogId)

Get a log entry

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.EventLogApi();
let eventLogId = 56; // Number | The Event Log ID.
apiInstance.getEventLog(eventLogId, (error, data, response) => {
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
 **eventLogId** | **Number**| The Event Log ID. | 

### Return type

[**EventLog**](EventLog.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getEventLogs

> EventLogCollection getEventLogs(opts)

Get all log entries from event log

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.EventLogApi();
let opts = {
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56 // Number | The number of items to skip before starting to collect the result set.
};
apiInstance.getEventLogs(opts, (error, data, response) => {
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

[**EventLogCollection**](EventLogCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

