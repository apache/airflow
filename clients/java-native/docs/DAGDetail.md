

# DAGDetail

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**dagId** | **String** |  |  [optional] [readonly]
**rootDagId** | **String** |  |  [optional] [readonly]
**isPaused** | **Boolean** |  |  [optional]
**isSubdag** | **Boolean** |  |  [optional] [readonly]
**fileloc** | **String** |  |  [optional] [readonly]
**fileToken** | **String** | The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading an non-DAG file. This also ensures API extensibility, because the format of encrypted data may change.  |  [optional] [readonly]
**owners** | **List&lt;String&gt;** |  |  [optional] [readonly]
**description** | **String** |  |  [optional] [readonly]
**scheduleInterval** | [**ScheduleInterval**](ScheduleInterval.md) |  |  [optional]
**tags** | [**List&lt;Tag&gt;**](Tag.md) |  |  [optional] [readonly]
**timezone** | **String** |  |  [optional]
**catchup** | **Boolean** |  |  [optional] [readonly]
**orientation** | **String** |  |  [optional] [readonly]
**concurrency** | [**BigDecimal**](BigDecimal.md) |  |  [optional] [readonly]
**startDate** | [**OffsetDateTime**](OffsetDateTime.md) |  |  [optional] [readonly]
**dagRunTimeout** | [**TimeDelta**](TimeDelta.md) |  |  [optional]
**docMd** | **String** |  |  [optional] [readonly]
**defaultView** | **String** |  |  [optional] [readonly]



