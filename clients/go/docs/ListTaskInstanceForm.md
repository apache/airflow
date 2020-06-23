# ListTaskInstanceForm

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DagIds** | **[]string** | Return objects with specific DAG IDs. The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
**ExecutionDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  | [optional] 
**ExecutionDateLte** | [**time.Time**](time.Time.md) | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  | [optional] 
**StartDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  | [optional] 
**StartDateLte** | [**time.Time**](time.Time.md) | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
**EndDateGte** | [**time.Time**](time.Time.md) | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  | [optional] 
**EndDateLte** | [**time.Time**](time.Time.md) | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  | [optional] 
**DurationGte** | **float32** | Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  | [optional] 
**DurationLte** | **float32** | Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  | [optional] 
**State** | **[]string** | The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
**Pool** | **[]string** | The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 
**Queue** | **[]string** | The value can be repeated to retrieve multiple matching values (OR condition). | [optional] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


