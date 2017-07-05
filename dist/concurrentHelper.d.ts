import * as aws from "aws-sdk";
/**
 * The maximum number of concurrent requests to have pending.
 */
export declare let concurrentFactor: number;
/**
 * The initial wait when backing off on request rate.
 */
export declare let backoffInitial: number;
/**
 * The wait growth factor when repeatedly backing off.
 */
export declare let backoffFactor: number;
/**
 * Manages a number of concurrent dynamodb putItem requests.  Put requests
 * are more powerful than batchWrites but cannot be done in batch form.  Making
 * them concurrently is the next best thing.
 * @param dynamodb
 * @param putInputs
 * @returns a Promise of an array of objects of errors with the corresponding input
 */
export declare function concurrentPutAll(dynamodb: aws.DynamoDB, putInputs: aws.DynamoDB.Types.PutItemInput[]): Promise<{
    input: aws.DynamoDB.Types.PutItemInput;
    error: Error;
}[]>;
