# Dag

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DagId** | **string** |  | [optional] [readonly] 
**RootDagId** | **string** |  | [optional] [readonly] 
**IsPaused** | **bool** |  | [optional] 
**IsSubdag** | **bool** |  | [optional] [readonly] 
**Fileloc** | **string** |  | [optional] [readonly] 
**FileToken** | **string** | The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading an non-DAG file. This also ensures API extensibility, because the format of encrypted data may change.  | [optional] [readonly] 
**Owners** | **[]string** |  | [optional] [readonly] 
**Description** | **string** |  | [optional] [readonly] 
**ScheduleInterval** | [**ScheduleInterval**](ScheduleInterval.md) |  | [optional] 
**Tags** | [**[]Tag**](Tag.md) |  | [optional] [readonly] 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


