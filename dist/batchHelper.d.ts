import * as aws from "aws-sdk";
/**
 * The maximum number of items that can be gotten in a single request.
 */
export declare const batchGetLimit = 100;
/**
 * The maximum number of items that can be put in a single request.
 */
export declare const batchWriteLimit = 25;
/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param batchGetInput
 * @returns the stored objects
 */
export declare function batchGetAll(dynamodb: aws.DynamoDB, batchGetInput: aws.DynamoDB.Types.BatchGetItemInput): Promise<any[]>;
/**
 * Batch write in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param batchPutInput
 */
export declare function batchWriteAll(dynamodb: aws.DynamoDB, batchPutInput: aws.DynamoDB.Types.BatchWriteItemInput): Promise<void>;
