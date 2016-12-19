"use strict";
const babysitter = require("./babysitter");
exports.babysitter = babysitter;
/**
 * The maximum number of items that can be gotten in a single request.
 */
exports.getLimit = 100;
/**
 * The maximum number of items that can be put in a single request.
 */
exports.putLimit = 25;
/**
 * Build a put object for a single item.
 * @param item
 * @returns the put item
 */
function buildRequestPutItem(item) {
    switch (typeof item) {
        case "boolean":
            return { BOOL: item };
        case "string":
            return { S: item };
        case "number":
            return { N: item.toString() };
        case "undefined":
        case "object": {
            if (!item) {
                return { NULL: true };
            }
            else if (item instanceof Buffer) {
                return { B: item.toString("base64") };
            }
            else if (Array.isArray(item)) {
                if (item.length > 0) {
                    const firstItemType = typeof item[0];
                    if (firstItemType === "string" && item.every(x => typeof x === "string")) {
                        return { SS: item.map(s => s.toString()) };
                    }
                    if (firstItemType === "number" && item.every(x => typeof x === "number")) {
                        return { NS: item.map(n => n.toString()) };
                    }
                }
                return { L: item.map(i => buildRequestPutItem(i)) };
            }
            else {
                const valueMap = {};
                Object.keys(item).forEach(key => valueMap[key] = buildRequestPutItem(item[key]));
                return { M: valueMap };
            }
        }
        default:
            // "symbol", "function"
            throw new Error(`Type ${typeof item} cannot be serialized into a request object.`);
    }
}
exports.buildRequestPutItem = buildRequestPutItem;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableName
 * @param items the items to put
 * @returns the put request object
 */
function buildBatchPutInput(tableName, items) {
    if (items.length > exports.putLimit) {
        throw new Error(`A single put request cannot exceed ${exports.putLimit} items.`);
    }
    return {
        RequestItems: {
            [tableName]: items.map(item => ({ PutRequest: { Item: buildRequestPutItem(item).M } }))
        }
    };
}
exports.buildBatchPutInput = buildBatchPutInput;
/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableName
 * @param keyField the name of the field that is the key
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
function buildBatchGetInput(tableName, keyField, keys) {
    if (keys.length > exports.getLimit) {
        throw new Error(`A single get request cannot exceed ${exports.getLimit} items.`);
    }
    return {
        RequestItems: {
            [tableName]: {
                Keys: keys.map(key => ({ [keyField]: buildRequestPutItem(key) }))
            }
        }
    };
}
exports.buildBatchGetInput = buildBatchGetInput;
/**
 * Extract a single property from a get response.
 * @param item
 * @returns the extracted value
 */
function unwrapResponseItem(item) {
    if (item.B) {
        if (typeof item.B === "string") {
            return Buffer.from(item.B, "base64");
        }
        return item.B;
    }
    else if (item.BOOL) {
        return item.BOOL;
    }
    else if (item.L) {
        return item.L.map(i => unwrapResponseItem(i));
    }
    else if (item.N) {
        return parseFloat(item.N);
    }
    else if (item.NS) {
        return item.NS.map(n => parseFloat(n));
    }
    else if (item.NULL) {
        return null;
    }
    else if (item.S) {
        return item.S;
    }
    else if (item.SS) {
        return item.SS;
    }
    else if (item.M) {
        const resp = {};
        Object.keys(item).forEach(key => resp[key] = unwrapResponseItem(item[key]));
    }
    throw new Error(`Unhandled response item ${JSON.stringify(item, null, 2)}`);
}
exports.unwrapResponseItem = unwrapResponseItem;
/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the name of the table requested from
 * @param response the batchGetItem response
 * @returns the objects extracted
 */
function unwrapBatchGetOutput(tableName, response) {
    const responseTableItems = response.Responses[tableName];
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({ M: responseItem }));
}
exports.unwrapBatchGetOutput = unwrapBatchGetOutput;
