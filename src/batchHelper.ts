import * as aws from "aws-sdk";
import {unwrapBatchGetOutput} from "./responseUnwrapper";

/**
 * The maximum number of items that can be got in a single request.
 */
export const batchGetLimit = 100;

/**
 * The maximum number of items that can be put in a single request.
 */
export const batchWriteLimit = 25;

/**
 * The initial wait when backing off on request rate, in milliseconds.
 */
export let backoffInitial = 1000;

/**
 * The maximum wait when backing off on request rate, in milliseconds.
 */
export let backoffMax = 30000;

/**
 * The wait growth factor when repeatedly backing off.  When backing off
 * from an operation the first wait `backoff = backoffInitial`
 * and for each consecutive wait `backoff = Math.min(backoff * backoffFactor, backoffMax)`.
 */
export let backoffFactor = 2;

/**
 * Configure module settings for: backoffInitial, backoffMax, backoffFactor.
 * See the module member for individual documentation.
 * @param options an object with optional values for the settings
 */
export function configure(options: {
    backoffInitial?: number;
    backoffMax?: number;
    backoffFactor?: number;
}) {
    if (options.hasOwnProperty("backoffInitial")) {
        backoffInitial = options.backoffInitial;
    }
    if (options.hasOwnProperty("backoffMax")) {
        backoffMax = options.backoffMax;
    }
    if (options.hasOwnProperty("backoffFactor")) {
        backoffFactor = options.backoffFactor;
    }
}

/**
 * Batch get all items in the request.  Can handle more than 100 keys at once by making
 * multiple requests.  Reattempts UnprocessedKeys.
 * @param dynamodb
 * @param batchGetInput
 * @returns the stored objects
 */
export async function batchGetAll(dynamodb: aws.DynamoDB, batchGetInput: aws.DynamoDB.BatchGetItemInput): Promise<any[]> {
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
        const request: aws.DynamoDB.BatchGetItemInput = {
            ...batchGetInput,
            RequestItems: {
                [requestItemsTable]: {
                    ...batchGetInput.RequestItems[requestItemsTable],
                    Keys: unprocessedKeys.splice(0, Math.min(unprocessedKeys.length, batchGetLimit))
                }
            }
        };

        const response = await dynamodb.batchGetItem(request).promise();
        const responseObjects = unwrapBatchGetOutput(response);
        results = [...results, ...responseObjects];
        if (response.UnprocessedKeys && response.UnprocessedKeys[requestItemsTable] && response.UnprocessedKeys[requestItemsTable].Keys.length) {
            unprocessedKeys.unshift(...response.UnprocessedKeys[requestItemsTable].Keys);
            await wait(backoff);
            backoff = Math.min(backoff * backoffFactor, backoffMax);
        } else {
            backoff = backoffInitial;
        }
    }

    return results;
}

/**
 * Batch write all items in the request.  Can handle more than 25 objects at once
 * by making multiple requests.  Reattempts UnprocessedItems.
 * @param dynamodb
 * @param batchPutInput
 */
export async function batchWriteAll(dynamodb: aws.DynamoDB, batchPutInput: aws.DynamoDB.BatchWriteItemInput): Promise<void> {
    const requestItemsTables = Object.keys(batchPutInput.RequestItems);
    if (requestItemsTables.length !== 1) {
        throw new Error("Only batchWrite to a single table at a time is supported in this method.");
    }

    const requestItemsTable = requestItemsTables[0];
    const unprocessedItems = [...batchPutInput.RequestItems[requestItemsTable]];
    let backoff = backoffInitial;

    while (unprocessedItems.length) {
        const request: aws.DynamoDB.BatchWriteItemInput = {
            ...batchPutInput,
            RequestItems: {
                [requestItemsTable]: unprocessedItems.splice(0, Math.min(unprocessedItems.length, batchWriteLimit))
            }
        };

        const response = await dynamodb.batchWriteItem(request).promise();
        if (response.UnprocessedItems && response.UnprocessedItems[requestItemsTable] && response.UnprocessedItems[requestItemsTable].length) {
            unprocessedItems.unshift(...response.UnprocessedItems[requestItemsTable]);
            await wait(backoff);
            backoff = Math.min(backoff * backoffFactor, backoffMax);
        } else {
            backoff = backoffInitial;
        }
    }
}

async function wait(millis: number): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, millis));
}
