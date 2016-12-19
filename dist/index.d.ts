import * as aws from "aws-sdk";
import * as babysitter from "./babysitter";
export { babysitter };
/**
 * The maximum number of items that can be gotten in a single request.
 */
export declare const getLimit = 100;
/**
 * The maximum number of items that can be put in a single request.
 */
export declare const putLimit = 25;
/**
 * Build a put object for a single item.
 * @param item
 * @returns the put item
 */
export declare function buildRequestPutItem(item: any): aws.DynamoDB.Types.AttributeValue;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableName
 * @param items the items to put
 * @returns the put request object
 */
export declare function buildBatchPutInput(tableName: string, items: Object[]): aws.DynamoDB.Types.BatchWriteItemInput;
/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableName
 * @param keyField the name of the field that is the key
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
export declare function buildBatchGetInput(tableName: string, keyField: string, keys: any[]): aws.DynamoDB.Types.BatchGetItemInput;
/**
 * Extract a single property from a get response.
 * @param item
 * @returns the extracted value
 */
export declare function unwrapResponseItem(item: aws.DynamoDB.Types.AttributeValue): any;
/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the name of the table requested from
 * @param response the batchGetItem response
 * @returns the objects extracted
 */
export declare function unwrapBatchGetOutput(tableName: string, response: aws.DynamoDB.Types.BatchGetItemOutput): any[];
