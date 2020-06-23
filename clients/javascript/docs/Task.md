# AirflowApiStable.Task

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**classRef** | [**ClassReference**](ClassReference.md) |  | [optional] 
**taskId** | **String** |  | [optional] [readonly] 
**owner** | **String** |  | [optional] [readonly] 
**startDate** | **Date** |  | [optional] [readonly] 
**endDate** | **Date** |  | [optional] [readonly] 
**triggerRule** | [**TriggerRule**](TriggerRule.md) |  | [optional] 
**extraLinks** | [**[TaskExtraLinks]**](TaskExtraLinks.md) |  | [optional] [readonly] 
**dependsOnPast** | **Boolean** |  | [optional] [readonly] 
**waitForDownstream** | **Boolean** |  | [optional] [readonly] 
**retries** | **Number** |  | [optional] [readonly] 
**queue** | **String** |  | [optional] [readonly] 
**pool** | **String** |  | [optional] [readonly] 
**poolSlots** | **Number** |  | [optional] [readonly] 
**executionTimeout** | [**TimeDelta**](TimeDelta.md) |  | [optional] 
**retryDelay** | [**TimeDelta**](TimeDelta.md) |  | [optional] 
**retryExponentialBackoff** | **Boolean** |  | [optional] [readonly] 
**priorityWeight** | **Number** |  | [optional] [readonly] 
**weightRule** | [**WeightRule**](WeightRule.md) |  | [optional] 
**uiColor** | **String** |  | [optional] 
**uiFgcolor** | **String** |  | [optional] 
**templateFields** | **[String]** |  | [optional] [readonly] 
**subDag** | [**DAG**](DAG.md) |  | [optional] 
**downstreamTaskIds** | **[String]** |  | [optional] [readonly] 


