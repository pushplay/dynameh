import * as aws from "aws-sdk";
import { TableSchema } from "./TableSchema";
import { DynamoKeyPair, DynamoKey } from "./validation";
/**
 * Build a put object for a single item.
 * @param item
 * @returns the put item
 */
export declare function buildRequestPutItem(item: any): aws.DynamoDB.Types.AttributeValue;
export declare function buildGetInput(tableSchema: TableSchema, primaryKey: DynamoKey, sortKey?: DynamoKey): aws.DynamoDB.Types.GetItemInput;
export declare function buildPutInput(tableSchema: TableSchema, item: Object): aws.DynamoDB.Types.PutItemInput;
export declare function buildDeleteInput(tableSchema: TableSchema, primaryKey: DynamoKey, sortKey?: DynamoKey): aws.DynamoDB.Types.DeleteItemInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param items the items to put
 * @returns the BatchWriteItemInput
 */
export declare function buildBatchPutInput(tableSchema: TableSchema, items: Object[]): aws.DynamoDB.Types.BatchWriteItemInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to delete
 * @param clobber whether to clobber the previous value if it has changed
 * @returns the BatchWriteItemInput
 */
export declare function buildBatchDeleteInput(tableSchema: TableSchema, keys: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchWriteItemInput;
/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
export declare function buildBatchGetInput(tableSchema: TableSchema, keys: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchGetItemInput;
/**
 * Build a request object that can be passed into `createTable`.
 * ProvisionedThroughput takes on default values of 1 and 1 and
 * should probably be edited.
 * @param tableSchema
 * @returns the create table request object
 */
export declare function buildCreateTableRequest(tableSchema: TableSchema): aws.DynamoDB.Types.CreateTableInput;
