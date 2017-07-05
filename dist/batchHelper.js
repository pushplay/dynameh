"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const responseUnwrapper_1 = require("./responseUnwrapper");
/**
 * The maximum number of items that can be gotten in a single request.
 */
exports.batchGetLimit = 100;
/**
 * The maximum number of items that can be put in a single request.
 */
exports.batchWriteLimit = 25;
/**
 * The initial wait when backing off on request rate.
 */
exports.backoffInitial = 2000;
/**
 * The wait growth factor when repeatedly backing off.
 */
exports.backoffFactor = 2;
/**
 * Batch get in multiple requests.  Can handle more than 100 keys at once and
 * reattempts UnprocessedKeys.
 * @param dynamodb
 * @param batchGetInput
 * @returns the stored objects
 */
function batchGetAll(dynamodb, batchGetInput) {
    return __awaiter(this, void 0, void 0, function* () {
        const requestItemsTables = Object.keys(batchGetInput.RequestItems);
        if (requestItemsTables.length !== 1) {
            throw new Error("Only batchGet from a single table at a time is supported in this method.");
        }
        const requestItemsTable = requestItemsTables[0];
        const unprocessedKeys = [...batchGetInput.RequestItems[requestItemsTable].Keys];
        let results = [];
        let backoff = exports.backoffInitial;
        while (unprocessedKeys.length) {
            // Take values from the input but override the Keys we fetch.
            const request = Object.assign({}, batchGetInput, { RequestItems: {
                    [requestItemsTable]: Object.assign({}, batchGetInput.RequestItems[requestItemsTable], { Keys: unprocessedKeys.splice(0, Math.min(unprocessedKeys.length, exports.batchGetLimit)) })
                } });
            const response = yield dynamodb.batchGetItem(request).promise();
            const responseObjects = responseUnwrapper_1.unwrapBatchGetOutput(requestItemsTable, response);
            results = [...results, ...responseObjects];
            if (response.UnprocessedKeys && response.UnprocessedKeys[requestItemsTable] && response.UnprocessedKeys[requestItemsTable].Keys.length) {
                unprocessedKeys.unshift(...response.UnprocessedKeys[requestItemsTable].Keys);
                yield wait(backoff);
                backoff *= exports.backoffFactor;
            }
            else {
                backoff = exports.backoffInitial;
            }
        }
        return results;
    });
}
exports.batchGetAll = batchGetAll;
/**
 * Batch write in multiple requests.  Can handle more than 25 objects at once
 * and reattempts UnprocessedItems.
 * @param dynamodb
 * @param batchPutInput
 */
function batchWriteAll(dynamodb, batchPutInput) {
    return __awaiter(this, void 0, void 0, function* () {
        const requestItemsTables = Object.keys(batchPutInput.RequestItems);
        if (requestItemsTables.length !== 1) {
            throw new Error("Only batchWrite to a single table at a time is supported in this method.");
        }
        const requestItemsTable = requestItemsTables[0];
        const unprocessedItems = [...batchPutInput.RequestItems[requestItemsTable]];
        let backoff = exports.backoffInitial;
        while (unprocessedItems.length) {
            const request = Object.assign({}, batchPutInput, { RequestItems: {
                    [requestItemsTable]: unprocessedItems.splice(0, Math.min(unprocessedItems.length, exports.batchWriteLimit))
                } });
            const response = yield dynamodb.batchWriteItem(request).promise();
            if (response.UnprocessedItems && response.UnprocessedItems[requestItemsTable] && response.UnprocessedItems[requestItemsTable].length) {
                unprocessedItems.unshift(...response.UnprocessedItems[requestItemsTable]);
                yield wait(backoff);
                backoff *= exports.backoffFactor;
            }
            else {
                backoff = exports.backoffInitial;
            }
        }
    });
}
exports.batchWriteAll = batchWriteAll;
function wait(millis) {
    return __awaiter(this, void 0, void 0, function* () {
        yield new Promise(resolve => setTimeout(resolve, millis));
    });
}
