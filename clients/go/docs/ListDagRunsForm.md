# ListDagRunsForm

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageOffset** | **int32** | The number of items to skip before starting to collect the result set. | [optional] 
**PageLimit** | **int32** | The numbers of items to return. | [optional] [default to 100]
**DagIds** | **[]string** | Return objects with specific DAG IDs. The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
**ExecutionDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte key to receive only the selected period.  | [optional] 
**ExecutionDateLte** | [**time.Time**](time.Time.md) | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte key to receive only the selected period.  | [optional] 
**StartDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal the specified date. This can be combined with start_date_lte key to receive only the selected period.  | [optional] 
**StartDateLte** | [**time.Time**](time.Time.md) | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period  | [optional] 
**EndDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal the specified date. This can be combined with end_date_lte parameter to receive only the selected period.  | [optional] 
**EndDateLte** | [**time.Time**](time.Time.md) | Returns objects less than or equal to the specified date. This can be combined with end_date_gte parameter to receive only the selected period.  | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


