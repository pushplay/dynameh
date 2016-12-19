import * as aws from "aws-sdk";
/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param tableName
 * @param keyField
 * @param keys
 * @returns the stored objects
 */
export declare function batchGetAll(dynamodb: aws.DynamoDB, tableName: string, keyField: string, keys: any[]): Promise<any>;
/**
 * Batch put in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param tableName
 * @param items
 */
export declare function batchPutAll(dynamodb: aws.DynamoDB, tableName: string, items: any[]): Promise<void>;
