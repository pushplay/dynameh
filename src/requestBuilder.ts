import * as aws from "aws-sdk";
import {TableSchema} from "./TableSchema";
import {
    checkSchemaKeyAgreement, checkSchema, checkSchemaItemAgreement, checkSchemaItemsAgreement,
    DynamoKeyPair, checkSchemaKeysAgreement, DynamoKey
} from "./validation";

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

export function buildGetInput(tableSchema: TableSchema, primaryKey: DynamoKey, sortKey?: DynamoKey): aws.DynamoDB.Types.GetItemInput {
    checkSchema(tableSchema);
    checkSchemaKeyAgreement(tableSchema, primaryKey, sortKey);

    const request: aws.DynamoDB.Types.GetItemInput = {
        Key: {
            [tableSchema.primaryKeyField]: buildRequestPutItem(primaryKey)
        },
        TableName: tableSchema.tableName
    };

    if (sortKey) {
        request.Key[tableSchema.sortKeyField] = buildRequestPutItem(sortKey);
    }

    return request;
}

export function buildPutInput(tableSchema: TableSchema, item: Object): aws.DynamoDB.Types.PutItemInput {
    checkSchema(tableSchema);
    checkSchemaItemAgreement(tableSchema, item);

    const request: aws.DynamoDB.Types.PutItemInput = {
        Item: buildRequestPutItem(item).M,
        TableName: tableSchema.tableName
    };

    if (tableSchema.versionKeyField) {
        if (item[tableSchema.versionKeyField]) {
            // Require the existing table item to have the same old value, and increment
            // the value we're putting.  This is the crux of the optimistic locking.
            request.ExpressionAttributeNames = {
                "#V": tableSchema.versionKeyField
            };
            request.ExpressionAttributeValues = {
                ":v": {
                    N: request.Item[tableSchema.versionKeyField].N
                }
            };
            request.ConditionExpression = `#V = :v`;
            request.Item[tableSchema.versionKeyField] = {
                N: (parseInt(request.Item[tableSchema.versionKeyField].N, 10) + 1).toString()
            };
        } else {
            // If the version key isn't set (or is 0) then we must be putting a brand new item.
            // Since every item in the table must have a partition key, this condition will prevent
            // any existing item from being overwritten.
            request.ExpressionAttributeNames = {
                "#P": tableSchema.primaryKeyField
            };
            request.ConditionExpression = "attribute_not_exists(#P)";
            request.Item[tableSchema.versionKeyField] = {
                N: "1"
            };
        }
    }

    return request;
}

export function buildDeleteInput(tableSchema: TableSchema, primaryKey: DynamoKey, sortKey?: DynamoKey): aws.DynamoDB.Types.DeleteItemInput {
    checkSchema(tableSchema);
    checkSchemaKeyAgreement(tableSchema, primaryKey, sortKey);

    const request: aws.DynamoDB.Types.DeleteItemInput = {
        Key: {
            [tableSchema.primaryKeyField]: buildRequestPutItem(primaryKey)
        },
        TableName: tableSchema.tableName
    };

    if (sortKey) {
        request.Key[tableSchema.sortKeyField] = buildRequestPutItem(sortKey);
    }

    return request;
}

/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param items the items to put
 * @returns the BatchWriteItemInput
 */
export function buildBatchPutInput(tableSchema: TableSchema, items: Object[]): aws.DynamoDB.Types.BatchWriteItemInput {
    checkSchema(tableSchema);
    checkSchemaItemsAgreement(tableSchema, items);

    if (tableSchema.versionKeyField) {
        throw new Error("TableSchema defines a versionKeyField.  Optimistic locking can not be supported in this request.");
    }

    return {
        RequestItems: {
            [tableSchema.tableName]: items.map(item => ({
                PutRequest: {
                    Item: buildRequestPutItem(item).M
                }
            }))
        }
    };
}

/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to delete
 * @param clobber whether to clobber the previous value if it has changed
 * @returns the BatchWriteItemInput
 */
export function buildBatchDeleteInput(tableSchema: TableSchema, keys: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchWriteItemInput {
    checkSchema(tableSchema);
    checkSchemaKeysAgreement(tableSchema, keys);

    if (tableSchema.sortKeyType) {
        const keyPairs = keys as DynamoKeyPair[];
        return {
            RequestItems: {
                [tableSchema.tableName]: keyPairs.map(keyPair => ({
                    DeleteRequest: {
                        Key: {
                            [tableSchema.primaryKeyField]: buildRequestPutItem(keyPair[0]),
                            [tableSchema.sortKeyField]: buildRequestPutItem(keyPair[1])
                        }
                    }
                }))
            }
        };
    } else {
        const flatKeys = keys as DynamoKey[];
        return {
            RequestItems: {
                [tableSchema.tableName]: flatKeys.map(key => ({
                    DeleteRequest: {
                        Key: {
                            [tableSchema.primaryKeyField]: buildRequestPutItem(key)
                        }
                    }
                }))
            }
        };
    }
}

/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
export function buildBatchGetInput(tableSchema: TableSchema, keys: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.Types.BatchGetItemInput {
    checkSchema(tableSchema);
    checkSchemaKeysAgreement(tableSchema, keys);

    if (tableSchema.sortKeyType) {
        const keyPairs = keys as DynamoKeyPair[];
        return {
            RequestItems: {
                [tableSchema.tableName]: {
                    Keys: keyPairs.map(keyPair => ({
                        [tableSchema.primaryKeyField]: buildRequestPutItem(keyPair[0]),
                        [tableSchema.sortKeyField]: buildRequestPutItem(keyPair[1])
                    }))
                }
            }
        };
    } else {
        const flatKeys = keys as KeyType[];
        return {
            RequestItems: {
                [tableSchema.tableName]: {
                    Keys: flatKeys.map(key => ({
                        [tableSchema.primaryKeyField]: buildRequestPutItem(key)
                    }))
                }
            }
        };
    }
}
