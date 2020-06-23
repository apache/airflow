# ClearTaskInstance

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DryRun** | **bool** | If set, don&#39;t actually run this operation. The response will contain a list of task instances planned to be cleaned, but not modified in any way.  | [optional] [default to true]
**StartDate** | **string** | The minimum execution date to clear. | [optional] 
**EndDate** | **string** | The maximum execution date to clear. | [optional] 
**OnlyFailed** | **string** | Only clear failed tasks. | [optional] 
**OnlyRunning** | **string** | Only clear running tasks. | [optional] 
**IncludeSubdags** | **bool** | Clear tasks in subdags and clear external tasks indicated by ExternalTaskMarker. | [optional] 
**IncludeParentdag** | **bool** | Clear tasks in the parent dag of the subdag. | [optional] 
**ResetDagRuns** | **bool** | Set state of DAG Runs to RUNNING. | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


