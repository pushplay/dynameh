import * as aws from "aws-sdk";
import {TableSchema} from "./TableSchema";

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
        Object.keys(item.M).forEach(key => resp[key] = unwrapResponseItem((item.M as any)[key]));
        return resp;
    }
    throw new Error(`Unhandled response item ${JSON.stringify(item, null, 2)}`);
}

/**
 * Extract the JSON objects from a response to `getItem`.
 * @param response result of getItem
 * @returns the object returned
 */
export function unwrapGetOutput(response: aws.DynamoDB.Types.GetItemOutput): any {
    if (!response.Item) {
        return null;
    }
    return unwrapResponseItem({M: response.Item});
}

/**
 * Extract the JSON objects from a response to `batchGetItem`.
 * @param tableName the string table name or TableSchema
 * @param response result of batchGetItem
 * @returns the objects returned
 */
export function unwrapBatchGetOutput(tableName: string | TableSchema, response: aws.DynamoDB.Types.BatchGetItemOutput): any[] {
    if ((tableName as TableSchema).tableName) {
        tableName = (tableName as TableSchema).tableName;
    }
    const responseTableItems = response.Responses[tableName as string];
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}

/**
 * Extract the JSON objects from a response to `scan`.
 * @param response result of scan
 * @returns the objects returned
 */
export function unwrapScanOutput(response: aws.DynamoDB.Types.ScanOutput): any[] {
    const responseTableItems = response.Items;
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}

/**
 * Extract the JSON objects from a response to `query`.
 * @param response result of query
 * @returns the objects returned
 */
export function unwrapQueryOutput(response: aws.DynamoDB.Types.QueryOutput): any[] {
    const responseTableItems = response.Items;
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}
