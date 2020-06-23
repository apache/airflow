# AirflowApiStable.DAG

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**dagId** | **String** |  | [optional] [readonly] 
**rootDagId** | **String** |  | [optional] [readonly] 
**isPaused** | **Boolean** |  | [optional] 
**isSubdag** | **Boolean** |  | [optional] [readonly] 
**fileloc** | **String** |  | [optional] [readonly] 
**fileToken** | **String** | The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading an non-DAG file. This also ensures API extensibility, because the format of encrypted data may change.  | [optional] [readonly] 
**owners** | **[String]** |  | [optional] [readonly] 
**description** | **String** |  | [optional] [readonly] 
**scheduleInterval** | [**ScheduleInterval**](ScheduleInterval.md) |  | [optional] 
**tags** | [**[Tag]**](Tag.md) |  | [optional] [readonly] 


