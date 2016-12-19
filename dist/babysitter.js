"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const dynameh = require("./");
/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param tableName
 * @param keyField
 * @param keys
 * @returns the stored objects
 */
function batchGetAll(dynamodb, tableName, keyField, keys) {
    return __awaiter(this, void 0, void 0, function* () {
        const keysToFetch = [...keys];
        let results = [];
        while (keysToFetch.length) {
            const requestKeys = keysToFetch.splice(0, Math.min(dynameh.getLimit, keysToFetch.length));
            const request = dynameh.buildBatchGetInput(tableName, keyField, requestKeys);
            const response = yield dynamodb.batchGetItem(request).promise();
            // TODO handle ProvisionedThroughputExceededException
            const responseObjects = dynameh.unwrapBatchGetOutput(tableName, response);
            results = [...results, ...responseObjects];
            if (response.UnprocessedKeys) {
                keysToFetch.unshift(...Object.keys(response.UnprocessedKeys));
            }
        }
        return results;
    });
}
exports.batchGetAll = batchGetAll;
/**
 * Batch put in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param tableName
 * @param items
 */
function batchPutAll(dynamodb, tableName, items) {
    return __awaiter(this, void 0, void 0, function* () {
        const itemsToPut = [...items];
        let unprocessedPuts = null;
        while (itemsToPut.length || unprocessedPuts) {
            const numUnprocessed = unprocessedPuts ? unprocessedPuts.length : 0;
            const requestItems = itemsToPut.splice(0, Math.min(dynameh.putLimit - numUnprocessed, itemsToPut.length));
            const request = dynameh.buildBatchPutInput(tableName, requestItems);
            if (numUnprocessed) {
                request.RequestItems[tableName] = [...unprocessedPuts, ...request.RequestItems[tableName]];
            }
            const response = yield dynamodb.batchWriteItem(request).promise();
            // TODO handle ProvisionedThroughputExceededException
            if (response.UnprocessedItems && response.UnprocessedItems[tableName]) {
                unprocessedPuts = response.UnprocessedItems[tableName];
            }
            else {
                unprocessedPuts = null;
            }
        }
    });
}
exports.batchPutAll = batchPutAll;
