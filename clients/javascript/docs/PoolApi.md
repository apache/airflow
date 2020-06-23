# AirflowApiStable.PoolApi

All URIs are relative to *http://localhost/api/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**deletePool**](PoolApi.md#deletePool) | **DELETE** /pools/{pool_name} | Delete a pool
[**getPool**](PoolApi.md#getPool) | **GET** /pools/{pool_name} | Get a pool
[**getPools**](PoolApi.md#getPools) | **GET** /pools | Get all pools
[**patchPool**](PoolApi.md#patchPool) | **PATCH** /pools/{pool_name} | Update a pool
[**postPool**](PoolApi.md#postPool) | **POST** /pools | Create a pool



## deletePool

> deletePool(poolName)

Delete a pool

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.PoolApi();
let poolName = "poolName_example"; // String | The Pool name.
apiInstance.deletePool(poolName, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully.');
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **poolName** | **String**| The Pool name. | 

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getPool

> Pool getPool(poolName)

Get a pool

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.PoolApi();
let poolName = "poolName_example"; // String | The Pool name.
apiInstance.getPool(poolName, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **poolName** | **String**| The Pool name. | 

### Return type

[**Pool**](Pool.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## getPools

> PoolCollection getPools(opts)

Get all pools

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.PoolApi();
let opts = {
  'limit': 100, // Number | The numbers of items to return.
  'offset': 56 // Number | The number of items to skip before starting to collect the result set.
};
apiInstance.getPools(opts, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **Number**| The numbers of items to return. | [optional] [default to 100]
 **offset** | **Number**| The number of items to skip before starting to collect the result set. | [optional] 

### Return type

[**PoolCollection**](PoolCollection.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## patchPool

> Pool patchPool(poolName, pool, opts)

Update a pool

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.PoolApi();
let poolName = "poolName_example"; // String | The Pool name.
let pool = new AirflowApiStable.Pool(); // Pool | 
let opts = {
  'updateMask': ["null"] // [String] | The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields. 
};
apiInstance.patchPool(poolName, pool, opts, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **poolName** | **String**| The Pool name. | 
 **pool** | [**Pool**](Pool.md)|  | 
 **updateMask** | [**[String]**](String.md)| The fields to update on the connection (connection, pool etc). If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.  | [optional] 

### Return type

[**Pool**](Pool.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


## postPool

> Pool postPool(pool)

Create a pool

### Example

```javascript
import AirflowApiStable from 'airflow_api__stable';

let apiInstance = new AirflowApiStable.PoolApi();
let pool = new AirflowApiStable.Pool(); // Pool | 
apiInstance.postPool(pool, (error, data, response) => {
  if (error) {
    console.error(error);
  } else {
    console.log('API called successfully. Returned data: ' + data);
  }
});
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pool** | [**Pool**](Pool.md)|  | 

### Return type

[**Pool**](Pool.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

