# DagRun

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DagRunId** | **string** | Run ID. The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error. If not provided, a value will be generated based on execution_date. If the specified dag_run_id is in use, the creation request fails with an ALREADY_EXISTS error. This together with DAG_ID are a unique key.  | [optional] 
**DagId** | **string** |  | [readonly] 
**ExecutionDate** | [**time.Time**](time.Time.md) | The execution date. This is the time when the DAG run should be started according to the DAG definition. The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error. This together with DAG_ID are a unique key.  | [optional] 
**StartDate** | [**time.Time**](time.Time.md) | The start time. The time when DAG Run was actually created..  | [optional] [readonly] 
**EndDate** | Pointer to [**time.Time**](time.Time.md) |  | [optional] [readonly] 
**State** | [**DagState**](DagState.md) |  | [optional] 
**ExternalTrigger** | **bool** |  | [optional] [readonly] [default to true]
**Conf** | [**map[string]interface{}**](.md) | JSON object describing additional configuration parameters. The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error.  | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


