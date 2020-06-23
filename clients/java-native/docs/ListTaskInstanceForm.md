

# ListTaskInstanceForm

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**dagIds** | **List&lt;String&gt;** | Return objects with specific DAG IDs. The value can be repeated to retrieve multiple matching values (OR condition). |  [optional]
**executionDateGte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects greater or equal to the specified date. This can be combined with execution_date_lte parameter to receive only the selected period.  |  [optional]
**executionDateLte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects less than or equal to the specified date. This can be combined with execution_date_gte parameter to receive only the selected period.  |  [optional]
**startDateGte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects greater or equal the specified date. This can be combined with startd_ate_lte parameter to receive only the selected period.  |  [optional]
**startDateLte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects less or equal the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  |  [optional]
**endDateGte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects greater or equal the specified date. This can be combined with start_date_lte parameter to receive only the selected period.  |  [optional]
**endDateLte** | [**OffsetDateTime**](OffsetDateTime.md) | Returns objects less than or equal to the specified date. This can be combined with start_date_gte parameter to receive only the selected period.  |  [optional]
**durationGte** | [**BigDecimal**](BigDecimal.md) | Returns objects greater than or equal to the specified values. This can be combined with duration_lte parameter to receive only the selected period.  |  [optional]
**durationLte** | [**BigDecimal**](BigDecimal.md) | Returns objects less than or equal to the specified values. This can be combined with duration_gte parameter to receive only the selected range.  |  [optional]
**state** | **List&lt;String&gt;** | The value can be repeated to retrieve multiple matching values (OR condition). |  [optional]
**pool** | **List&lt;String&gt;** | The value can be repeated to retrieve multiple matching values (OR condition). |  [optional]
**queue** | **List&lt;String&gt;** | The value can be repeated to retrieve multiple matching values (OR condition). |  [optional]



