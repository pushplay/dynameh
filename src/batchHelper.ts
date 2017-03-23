import * as aws from "aws-sdk";
import {unwrapBatchGetOutput} from "./responseUnwrapper";

/**
 * The maximum number of items that can be gotten in a single request.
 */
export const batchGetLimit = 100;

/**
 * The maximum number of items that can be put in a single request.
 */
export const batchWriteLimit = 25;

/**
 * The initial wait when backing off on request rate.
 */
export let backoffInitial = 2000;

/**
 * The wait growth factor when repeatedly backing off.
 */
export let backoffFactor = 2;

/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param batchGetInput
 * @returns the stored objects
 */
export async function batchGetAll(dynamodb: aws.DynamoDB, batchGetInput: aws.DynamoDB.Types.BatchGetItemInput): Promise<any[]> {
    const requestItemsTables = Object.keys(batchGetInput.RequestItems);
    if (requestItemsTables.length !== 1) {
        throw new Error("Only batchGet from a single table at a time is supported in this method.");
    }

    const requestItemsTable = requestItemsTables[0];
    const unprocessedKeys = [...batchGetInput.RequestItems[requestItemsTable].Keys];
    let results: any[] = [];
    let backoff = backoffInitial;

    while (unprocessedKeys.length) {
        // Take values from the input but override the Keys we fetch.
        const request: aws.DynamoDB.Types.BatchGetItemInput = {
            ...batchGetInput,
            RequestItems: {
                [requestItemsTable]: {
                    ...batchGetInput.RequestItems[requestItemsTable],
                    Keys: unprocessedKeys.splice(0, Math.min(unprocessedKeys.length, batchGetLimit))
                }
            }
        };

        const response = await dynamodb.batchGetItem(request).promise();
        const responseObjects = unwrapBatchGetOutput(requestItemsTable, response);
        results = [...results, ...responseObjects];
        if (response.UnprocessedKeys && response.UnprocessedKeys[requestItemsTable] && response.UnprocessedKeys[requestItemsTable].Keys.length) {
            unprocessedKeys.unshift(...response.UnprocessedKeys[requestItemsTable].Keys);
            await wait(backoff);
            backoff *= backoffFactor;
        } else {
            backoff = backoffInitial;
        }
    }

    return results;
}

/**
 * Batch write in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param batchPutInput
 */
export async function batchWriteAll(dynamodb: aws.DynamoDB, batchPutInput: aws.DynamoDB.Types.BatchWriteItemInput): Promise<void> {
    const requestItemsTables = Object.keys(batchPutInput.RequestItems);
    if (requestItemsTables.length !== 1) {
        throw new Error("Only batchWrite to a single table at a time is supported in this method.");
    }

    const requestItemsTable = requestItemsTables[0];
    const unprocessedItems = [...batchPutInput.RequestItems[requestItemsTable]];
    let backoff = backoffInitial;

    while (unprocessedItems.length) {
        const request: aws.DynamoDB.Types.BatchWriteItemInput = {
            ...batchPutInput,
            RequestItems: {
                [requestItemsTable]: unprocessedItems.splice(0, Math.min(unprocessedItems.length, batchWriteLimit))
            }
        };

        const response = await dynamodb.batchWriteItem(request).promise();
        if (response.UnprocessedItems && response.UnprocessedItems[requestItemsTable] && response.UnprocessedItems[requestItemsTable].length) {
            unprocessedItems.unshift(...response.UnprocessedItems[requestItemsTable]);
            await wait(backoff);
            backoff *= backoffFactor;
        } else {
            backoff = backoffInitial;
        }
    }
}

async function wait(millis: number): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, millis));
}
