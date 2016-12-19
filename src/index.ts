import * as aws from "aws-sdk";
import * as babysitter from "./babysitter";

export {babysitter};

/**
 * The maximum number of items that can be gotten in a single request.
 */
export const getLimit = 100;

/**
 * The maximum number of items that can be put in a single request.
 */
export const putLimit = 25;

/**
 * Build a put object for a single item.
 * @param item
 * @returns the put item
 */
export function buildRequestPutItem(item: any): aws.DynamoDB.Types.AttributeValue {
    switch (typeof item) {
        case "boolean":
            return {BOOL: item};
        case "string":
            return {S: item};
        case "number":
            return {N: item.toString()};
        case "undefined":
        case "object": {
            if (!item) {
                return {NULL: true};
            } else if (item instanceof Buffer) {
                return {B: item.toString("base64")};
            } else if (Array.isArray(item)) {
                if (item.length > 0) {
                    const firstItemType = typeof item[0];
                    if (firstItemType === "string" && item.every(x => typeof x === "string")) {
                        return {SS: item.map(s => s.toString())};
                    }
                    if (firstItemType === "number" && item.every(x => typeof x === "number")) {
                        return {NS: item.map(n => n.toString())};
                    }
                }
                return {L: item.map(i => buildRequestPutItem(i))};
            } else {
                const valueMap: any = {};
                Object.keys(item).forEach(key => valueMap[key] = buildRequestPutItem(item[key]));
                return {M: valueMap};
            }
        }
        default:
            // "symbol", "function"
            throw new Error(`Type ${typeof item} cannot be serialized into a request object.`);
    }
}

/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableName
 * @param items the items to put
 * @returns the put request object
 */
export function buildBatchPutInput(tableName: string, items: Object[]): aws.DynamoDB.Types.BatchWriteItemInput {
    if (items.length > putLimit) {
        throw new Error(`A single put request cannot exceed ${putLimit} items.`);
    }
    return {
        RequestItems: {
            [tableName]: items.map(item => ({PutRequest: {Item: buildRequestPutItem(item).M}}))
        }
    };
}

/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableName
 * @param keyField the name of the field that is the key
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
export function buildBatchGetInput(tableName: string, keyField: string, keys: any[]): aws.DynamoDB.Types.BatchGetItemInput {
    if (keys.length > getLimit) {
        throw new Error(`A single get request cannot exceed ${getLimit} items.`)
    }
    return {
        RequestItems: {
            [tableName]: {
                Keys: keys.map(key => ({[keyField]: buildRequestPutItem(key)}))
            }
        }
    };
}

/**
 * Extract a single property from a get response.
 * @param item
 * @returns the extracted value
 */
export function unwrapResponseItem(item: aws.DynamoDB.Types.AttributeValue): any {
    if (item.B) {
        if (typeof item.B === "string") {
            return Buffer.from(item.B as string, "base64");
        }
        return item.B;
    } else if (item.BOOL) {
        return item.BOOL;
    } else if (item.L) {
        return item.L.map(i => unwrapResponseItem(i));
    } else if (item.N) {
        return parseFloat(item.N);
    } else if (item.NS) {
        return item.NS.map(n => parseFloat(n));
    } else if (item.NULL) {
        return null;
    } else if (item.S) {
        return item.S;
    } else if (item.SS) {
        return item.SS;
    } else if (item.M) {
        const resp: any = {};
        Object.keys(item).forEach(key => resp[key] = unwrapResponseItem((item as any)[key]));
    }
    throw new Error(`Unhandled response item ${JSON.stringify(item, null, 2)}`);
}

/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the name of the table requested from
 * @param response the batchGetItem response
 * @returns the objects extracted
 */
export function unwrapBatchGetOutput(tableName: string, response: aws.DynamoDB.Types.BatchGetItemOutput): any[] {
    const responseTableItems = response.Responses[tableName];
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}
