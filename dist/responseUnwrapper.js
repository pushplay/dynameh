"use strict";
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
        Object.keys(item.M).forEach(key => resp[key] = unwrapResponseItem(item.M[key]));
        return resp;
    }
    throw new Error(`Unhandled response item ${JSON.stringify(item, null, 2)}`);
}
exports.unwrapResponseItem = unwrapResponseItem;
/**
 * Extract the JSON objects from a response to `getItem`.
 * @param response result of getItem
 * @returns the object returned
 */
function unwrapGetOutput(response) {
    return unwrapResponseItem({ M: response.Item });
}
exports.unwrapGetOutput = unwrapGetOutput;
/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the string table name or TableSchema
 * @param response result of batchGetItem
 * @returns the objects returned
 */
function unwrapBatchGetOutput(tableName, response) {
    if (tableName.tableName) {
        tableName = tableName.tableName;
    }
    const responseTableItems = response.Responses[tableName];
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({ M: responseItem }));
}
exports.unwrapBatchGetOutput = unwrapBatchGetOutput;
/**
 * Extract the JSON objects from a response to `scan`.
 * @param response result of scan
 * @returns the objects returned
 */
function unwrapScanOutput(response) {
    const responseTableItems = response.Items;
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({ M: responseItem }));
}
exports.unwrapScanOutput = unwrapScanOutput;
