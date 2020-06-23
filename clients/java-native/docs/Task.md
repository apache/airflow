

# Task

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**classRef** | [**ClassReference**](ClassReference.md) |  |  [optional]
**taskId** | **String** |  |  [optional] [readonly]
**owner** | **String** |  |  [optional] [readonly]
**startDate** | [**OffsetDateTime**](OffsetDateTime.md) |  |  [optional] [readonly]
**endDate** | [**OffsetDateTime**](OffsetDateTime.md) |  |  [optional] [readonly]
**triggerRule** | [**TriggerRule**](TriggerRule.md) |  |  [optional]
**extraLinks** | [**List&lt;TaskExtraLinks&gt;**](TaskExtraLinks.md) |  |  [optional] [readonly]
**dependsOnPast** | **Boolean** |  |  [optional] [readonly]
**waitForDownstream** | **Boolean** |  |  [optional] [readonly]
**retries** | [**BigDecimal**](BigDecimal.md) |  |  [optional] [readonly]
**queue** | **String** |  |  [optional] [readonly]
**pool** | **String** |  |  [optional] [readonly]
**poolSlots** | [**BigDecimal**](BigDecimal.md) |  |  [optional] [readonly]
**executionTimeout** | [**TimeDelta**](TimeDelta.md) |  |  [optional]
**retryDelay** | [**TimeDelta**](TimeDelta.md) |  |  [optional]
**retryExponentialBackoff** | **Boolean** |  |  [optional] [readonly]
**priorityWeight** | [**BigDecimal**](BigDecimal.md) |  |  [optional] [readonly]
**weightRule** | [**WeightRule**](WeightRule.md) |  |  [optional]
**uiColor** | **String** |  |  [optional]
**uiFgcolor** | **String** |  |  [optional]
**templateFields** | **List&lt;String&gt;** |  |  [optional] [readonly]
**subDag** | [**DAG**](DAG.md) |  |  [optional]
**downstreamTaskIds** | **List&lt;String&gt;** |  |  [optional] [readonly]



