import * as aws from "aws-sdk";
import { TableSchema } from "./TableSchema";
import { DynamoKey, DynamoKeyPair } from "./validation";
/**
 * Build a serialized item that can be put in DynamoDB.
 * @param tableSchema
 * @param item
 * @returns a put item
 */
export declare function buildRequestPutItem(tableSchema: TableSchema, item: any): aws.DynamoDB.AttributeValue;
/**
 * Build a request object that can be passed into `getItem`.
 * @param tableSchema
 * @param primaryKeyValue the key of the item to get
 * @param sortKeyValue sort key of the item to get, if set in the schema
 * @returns input for the `getItem` method
 */
export declare function buildGetInput(tableSchema: TableSchema, primaryKeyValue: DynamoKey, sortKeyValue?: DynamoKey): aws.DynamoDB.Types.GetItemInput;
/**
 * Build a request object that can be passed into `putItem`.
 * @param tableSchema
 * @param item
 * @returns input for the `putItem` method
 */
export declare function buildPutInput(tableSchema: TableSchema, item: Object): aws.DynamoDB.Types.PutItemInput;
/**
 * Build a request object that can be passed into `deleteItem`
 * @param tableSchema
 * @param primaryKeyValue the key of the item to delete
 * @param sortKeyValue sort key of the item to delete, if set in the schema
 * @returns input for the `deleteItem` method
 */
export declare function buildDeleteInput(tableSchema: TableSchema, primaryKeyValue: DynamoKey, sortKeyValue?: DynamoKey): aws.DynamoDB.Types.DeleteItemInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param items the items to put
 * @returns input for the `batchWriteItem` method
 */
export declare function buildBatchPutInput(tableSchema: TableSchema, items: Object[]): aws.DynamoDB.Types.BatchWriteItemInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param keyValues an array of the key values for each item to delete
 * @returns input for the `batchWriteItem` method
 */
export declare function buildBatchDeleteInput(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchWriteItemInput;
/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableSchema
 * @param keyValues an array of the key values for each item to request
 * @returns input for the `batchGetItem` method
 */
export declare function buildBatchGetInput(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchGetItemInput;
/**
 * Build a request object that can be passed into `createTable`.
 * ProvisionedThroughput takes on default values of 1 and 1 and
 * should probably be edited.
 * @param tableSchema
 * @returns input for the `createTable` method
 */
export declare function buildCreateTableInput(tableSchema: TableSchema): aws.DynamoDB.Types.CreateTableInput;
