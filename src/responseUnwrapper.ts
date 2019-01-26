import * as aws from "aws-sdk";

/**
 * Extract a single property from a get response.
 * @param item
 * @returns The deserialized value.
 */
export function unwrapResponseItem(item: aws.DynamoDB.AttributeValue): any {
    if (item.hasOwnProperty("B")) {
        if (typeof item.B === "string") {
            return Buffer.from(item.B as string, "base64");
        }
        return item.B;
    } else if (item.hasOwnProperty("BS")) {
        return new Set(item.BS.map(b => {
            if (typeof b === "string") {
                return Buffer.from(b as string, "base64");
            }
            return b;
        }));
    } else if (item.hasOwnProperty("BOOL")) {
        return item.BOOL;
    } else if (item.hasOwnProperty("L")) {
        return item.L.map(i => unwrapResponseItem(i));
    } else if (item.hasOwnProperty("N")) {
        return parseFloat(item.N);
    } else if (item.hasOwnProperty("NS")) {
        return new Set(item.NS.map(n => parseFloat(n)));
    } else if (item.hasOwnProperty("NULL")) {
        return null;
    } else if (item.hasOwnProperty("S")) {
        return item.S;
    } else if (item.hasOwnProperty("SS")) {
        return new Set(item.SS);
    } else if (item.hasOwnProperty("M")) {
        const resp: any = {};
        Object.keys(item.M).forEach(key => resp[key] = unwrapResponseItem(item.M[key]));
        return resp;
    }
    throw new Error(`Unhandled response item ${JSON.stringify(item, null, 2)}`);
}

/**
 * Extract the JSON objects from a response to `getItem`.
 * @param response Result of getItem.
 * @returns Rhe object returned by get.
 */
export function unwrapGetOutput(response: aws.DynamoDB.GetItemOutput): any {
    if (!response.Item) {
        return null;
    }
    return unwrapResponseItem({M: response.Item});
}

/**
 * Extract the JSON objects from a response to `batchGetItem`.  If multiple
 * tables were fetched from they will all be in the resulting array.
 * @param response Result of batchGetItem.
 * @returns The objects returned by the batch get.
 */
export function unwrapBatchGetOutput(response: aws.DynamoDB.BatchGetItemOutput): any[] {
    if (!response.Responses) {
        return [];
    }

    return Object.keys(response.Responses)
        .map(key => response.Responses[key])
        .map(responseTableItems => responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem})))
        .reduce((acc, cur) => [...acc, ...cur], []);
}

/**
 * Extract the JSON objects from a response to `scan`.
 * @param response Result of scan.
 * @returns The objects returned by the scan.
 */
export function unwrapScanOutput(response: aws.DynamoDB.ScanOutput): any[] {
    const responseTableItems = response.Items;
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}

/**
 * Extract the JSON objects from a response to `query`.
 * @param response Result of query.
 * @returns The objects returned by the query.
 */
export function unwrapQueryOutput(response: aws.DynamoDB.QueryOutput): any[] {
    const responseTableItems = response.Items;
    if (!responseTableItems) {
        return [];
    }
    return responseTableItems.map(responseItem => unwrapResponseItem({M: responseItem}));
}
