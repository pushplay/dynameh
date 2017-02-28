import * as aws from "aws-sdk";
import { TableSchema } from "./TableSchema";
/**
 * Extract a single property from a get response.
 * @param item
 * @returns the extracted value
 */
export declare function unwrapResponseItem(item: aws.DynamoDB.Types.AttributeValue): any;
/**
 * Extract the JSON objects from a response to `getItem`.
 * @param response result of getItem
 * @returns the object returned
 */
export declare function unwrapGetOutput(response: aws.DynamoDB.Types.GetItemOutput): any;
/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the string table name or TableSchema
 * @param response result of batchGetItem
 * @returns the objects returned
 */
export declare function unwrapBatchGetOutput(tableName: string | TableSchema, response: aws.DynamoDB.Types.BatchGetItemOutput): any[];
/**
 * Extract the JSON objects from a response to `scan`.
 * @param response result of scan
 * @returns the objects returned
 */
export declare function unwrapScanOutput(response: aws.DynamoDB.Types.ScanOutput): any[];
/**
 * Extract the JSON objects from a response to `query`.
 * @param response result of query
 * @returns the objects returned
 */
export declare function unwrapQueryOutput(response: aws.DynamoDB.Types.QueryOutput): any[];
