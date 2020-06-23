# Task

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ClassRef** | [**ClassReference**](ClassReference.md) |  | [optional] 
**TaskId** | **string** |  | [optional] [readonly] 
**Owner** | **string** |  | [optional] [readonly] 
**StartDate** | [**time.Time**](time.Time.md) |  | [optional] [readonly] 
**EndDate** | [**time.Time**](time.Time.md) |  | [optional] [readonly] 
**TriggerRule** | [**TriggerRule**](TriggerRule.md) |  | [optional] 
**ExtraLinks** | [**[]TaskExtraLinks**](Task_extra_links.md) |  | [optional] [readonly] 
**DependsOnPast** | **bool** |  | [optional] [readonly] 
**WaitForDownstream** | **bool** |  | [optional] [readonly] 
**Retries** | **float32** |  | [optional] [readonly] 
**Queue** | **string** |  | [optional] [readonly] 
**Pool** | **string** |  | [optional] [readonly] 
**PoolSlots** | **float32** |  | [optional] [readonly] 
**ExecutionTimeout** | [**TimeDelta**](TimeDelta.md) |  | [optional] 
**RetryDelay** | [**TimeDelta**](TimeDelta.md) |  | [optional] 
**RetryExponentialBackoff** | **bool** |  | [optional] [readonly] 
**PriorityWeight** | **float32** |  | [optional] [readonly] 
**WeightRule** | [**WeightRule**](WeightRule.md) |  | [optional] 
**UiColor** | **string** |  | [optional] 
**UiFgcolor** | **string** |  | [optional] 
**TemplateFields** | **[]string** |  | [optional] [readonly] 
**SubDag** | [**Dag**](DAG.md) |  | [optional] 
**DownstreamTaskIds** | **[]string** |  | [optional] [readonly] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


