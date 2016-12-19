import * as aws from "aws-sdk";
import * as dynameh from "./";

/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param tableName
 * @param keyField
 * @param keys
 * @returns the stored objects
 */
export async function batchGetAll(dynamodb: aws.DynamoDB, tableName: string, keyField: string, keys: any[]): Promise<any> {
    const keysToFetch = [...keys];
    let results: any[] = [];

    while (keysToFetch.length) {
        const requestKeys = keysToFetch.splice(0, Math.min(dynameh.getLimit, keysToFetch.length));
        const request = dynameh.buildBatchGetInput(tableName, keyField, requestKeys);

        const response = await dynamodb.batchGetItem(request).promise();
        // TODO handle ProvisionedThroughputExceededException
        const responseObjects = dynameh.unwrapBatchGetOutput(tableName, response);
        results = [...results, ...responseObjects];
        if (response.UnprocessedKeys) {
            keysToFetch.unshift(...Object.keys(response.UnprocessedKeys));
        }
    }

    return results;
}

/**
 * Batch put in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param tableName
 * @param items
 */
export async function batchPutAll(dynamodb: aws.DynamoDB, tableName: string, items: any[]): Promise<void> {
    const itemsToPut = [...items];
    let unprocessedPuts: aws.DynamoDB.WriteRequest[] = null;

    while (itemsToPut.length || unprocessedPuts) {
        const numUnprocessed = unprocessedPuts ? unprocessedPuts.length : 0;
        const requestItems = itemsToPut.splice(0, Math.min(dynameh.putLimit - numUnprocessed, itemsToPut.length));
        const request = dynameh.buildBatchPutInput(tableName, requestItems);
        if (numUnprocessed) {
            request.RequestItems[tableName] = [...unprocessedPuts, ...request.RequestItems[tableName]];
        }

        const response = await dynamodb.batchWriteItem(request).promise();
        // TODO handle ProvisionedThroughputExceededException
        if (response.UnprocessedItems && response.UnprocessedItems[tableName]) {
            unprocessedPuts = response.UnprocessedItems[tableName];
        } else {
            unprocessedPuts = null;
        }
    }
}
