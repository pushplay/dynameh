"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const validation_1 = require("./validation");
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
function buildGetInput(tableSchema, primaryKey, sortKey) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaKeyAgreement(tableSchema, primaryKey, sortKey);
    const request = {
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
exports.buildGetInput = buildGetInput;
function buildPutInput(tableSchema, item) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaItemAgreement(tableSchema, item);
    const request = {
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
        }
        else {
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
exports.buildPutInput = buildPutInput;
function buildDeleteInput(tableSchema, primaryKey, sortKey) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaKeyAgreement(tableSchema, primaryKey, sortKey);
    const request = {
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
exports.buildDeleteInput = buildDeleteInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param items the items to put
 * @returns the BatchWriteItemInput
 */
function buildBatchPutInput(tableSchema, items) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaItemsAgreement(tableSchema, items);
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
exports.buildBatchPutInput = buildBatchPutInput;
/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to delete
 * @param clobber whether to clobber the previous value if it has changed
 * @returns the BatchWriteItemInput
 */
function buildBatchDeleteInput(tableSchema, keys) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaKeysAgreement(tableSchema, keys);
    if (tableSchema.sortKeyType) {
        const keyPairs = keys;
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
    }
    else {
        const flatKeys = keys;
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
exports.buildBatchDeleteInput = buildBatchDeleteInput;
/**
 * Build a request object that can be passed into `batchGetItem`.
 * @param tableSchema
 * @param keys an array of the key values for each item to request
 * @returns the get request object
 */
function buildBatchGetInput(tableSchema, keys) {
    validation_1.checkSchema(tableSchema);
    validation_1.checkSchemaKeysAgreement(tableSchema, keys);
    if (tableSchema.sortKeyType) {
        const keyPairs = keys;
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
    }
    else {
        const flatKeys = keys;
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
exports.buildBatchGetInput = buildBatchGetInput;
/**
 * Build a request object that can be passed into `createTable`.
 * ProvisionedThroughput takes on default values of 1 and 1 and
 * should probably be edited.
 * @param tableSchema
 * @returns the create table request object
 */
function buildCreateTableRequest(tableSchema) {
    validation_1.checkSchema(tableSchema);
    const request = {
        AttributeDefinitions: [
            {
                AttributeName: tableSchema.primaryKeyField,
                AttributeType: jsTypeToDdbType(tableSchema.primaryKeyType)
            }
        ],
        KeySchema: [
            {
                AttributeName: tableSchema.primaryKeyField,
                KeyType: "HASH"
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
        },
        TableName: tableSchema.tableName,
    };
    if (tableSchema.sortKeyField) {
        request.AttributeDefinitions.push({
            AttributeName: tableSchema.sortKeyField,
            AttributeType: jsTypeToDdbType(tableSchema.sortKeyType)
        });
        request.KeySchema.push({
            AttributeName: tableSchema.sortKeyField,
            KeyType: "RANGE"
        });
    }
    return request;
}
exports.buildCreateTableRequest = buildCreateTableRequest;
function jsTypeToDdbType(t) {
    switch (t) {
        case "string":
            return "S";
        case "number":
            return "N";
    }
    throw new Error("Unhandled key type.");
}
