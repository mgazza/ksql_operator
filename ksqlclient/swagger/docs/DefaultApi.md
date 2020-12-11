# {{classname}}

All URIs are relative to *http://ksqldb-server:8088/*

Method | HTTP request | Description
------------- | ------------- | -------------
[**KsqlPost**](DefaultApi.md#KsqlPost) | **Post** /ksql | Query the ksql database
[**StatusCommandIDGet**](DefaultApi.md#StatusCommandIDGet) | **Get** /status/{commandID} | Query the status of a command

# **KsqlPost**
> KsqlResponse KsqlPost(ctx, body)
Query the ksql database

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Statement**](Statement.md)| The KSQL query | 

### Return type

[**KsqlResponse**](KSQL_Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/vnd.ksql.v1+json
 - **Accept**: application/vnd.ksql.v1+json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **StatusCommandIDGet**
> StatusResponse StatusCommandIDGet(ctx, commandID)
Query the status of a command

### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **commandID** | **string**|  | 

### Return type

[**StatusResponse**](Status_Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/vnd.ksql.v1+json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

