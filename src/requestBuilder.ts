import * as aws from "aws-sdk";
import {TableSchema} from "./TableSchema";
import {
    checkCondition,
    checkConditions,
    checkSchema,
    checkSchemaItemAgreement,
    checkSchemaItemsAgreement,
    checkSchemaKeyAgreement,
    checkSchemaKeysAgreement,
    checkSchemaPartitionKeyAgreement,
    checkSchemaSortKeyAgreement,
    DynamoKey,
    DynamoKeyPair,
    DynamoQueryConditionOperator,
    operatorIsFunction
} from "./validation";
import {dynamoDbReservedWords} from "./dynamoDbReservedWords";
import {Condition} from "./Condition";
import {UpdateExpressionAction} from "./UpdateExpressionAction";

/**
 * Build a serialized item that can be put in DynamoDB.  This syntax is also used for
 * expression and key values.
 * @param tableSchema
 * @param item The item to serialize.
 * @returns A put item.
 */
export function buildRequestPutItem(tableSchema: TableSchema, item: any): aws.DynamoDB.AttributeValue {
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
            } else if (item instanceof Uint8Array) {
                return {B: Buffer.from(item as any).toString("base64")};
            } else if (item instanceof Date) {
                if (tableSchema.dateSerializationFunction) {
                    return buildRequestPutItem(tableSchema, tableSchema.dateSerializationFunction(item));
                } else {
                    return {S: item.toISOString()};
                }
            } else if (item instanceof Set) {
                const items = Array.from(item);
                if (item.size === 0) {
                    throw new Error("Empty Sets are not supported.");
                }

                if (items.every(x => typeof x === "string")) {
                    return {SS: items.map(s => s.toString())};
                }
                if (items.every(x => typeof x === "number")) {
                    return {NS: items.map(n => n.toString())};
                }
                if (items.every(x => x instanceof Buffer)) {
                    return {BS: items.map(b => b.toString("base64"))};
                }
                if (items.every(x => x instanceof Uint8Array)) {
                    return {BS: items.map(b => Buffer.from(b).toString("base64"))};
                }

                throw new Error(`Set [${items.slice(0, 10).map(i => typeof i).join(",")}] cannot be serialized into a request object.`);
            } else if (Array.isArray(item)) {
                return {L: item.map(i => buildRequestPutItem(tableSchema, i))};
            } else {
                const valueMap: aws.DynamoDB.MapAttributeValue = {};
                Object.keys(item).forEach(key => valueMap[key] = buildRequestPutItem(tableSchema, item[key]));
                return {M: valueMap};
            }
        }
        default:
            // "symbol", "function"
            throw new Error(`Type ${typeof item} cannot be serialized into a request object.`);
    }
}

/**
 * Build a request object that can be passed into `getItem`.
 * @param tableSchema
 * @param partitionKeyValue The key of the item to get.
 * @param sortKeyValue Sort key of the item to get, if set in the schema.
 * @returns Input for the `getItem` method.
 */
export function buildGetInput(tableSchema: TableSchema, partitionKeyValue: DynamoKey, sortKeyValue?: DynamoKey): aws.DynamoDB.GetItemInput {
    checkSchema(tableSchema);
    checkSchemaKeyAgreement(tableSchema, partitionKeyValue, sortKeyValue);
    if (tableSchema.indexName) {
        throw new Error("DynamoDB does not support secondary indexes for 'get'.  Use 'query' instead.");
    }

    return {
        Key: getKey(tableSchema, partitionKeyValue, sortKeyValue),
        TableName: tableSchema.tableName
    };
}

/**
 * Build a request object that can be passed into `putItem`.
 *
 * If `TableSchema.versionKeyField` is set the put will only succeed
 * if the item in the database has the same value (and then the version
 * will be incremented).
 * @param tableSchema
 * @param item The item to put to the database.
 * @returns Input for the `putItem` method.
 */
export function buildPutInput(tableSchema: TableSchema, item: object): aws.DynamoDB.PutItemInput {
    checkSchema(tableSchema);
    checkSchemaItemAgreement(tableSchema, item);

    const request: aws.DynamoDB.PutItemInput = {
        Item: buildRequestPutItem(tableSchema, item).M,
        TableName: tableSchema.tableName
    };

    if (tableSchema.versionKeyField) {
        const expressionAttributeNames = {};
        const expressionAttributeValues = {};
        const versionAttributeName = getExpressionAttributeName(expressionAttributeNames, tableSchema.versionKeyField);

        if (item[tableSchema.versionKeyField] != null) {
            // Require the existing table item to have the same old value, and increment
            // the value we're putting.  This is the crux of the optimistic locking.
            request.ConditionExpression = `${versionAttributeName} = ${getExpressionValueName(tableSchema, expressionAttributeValues, item[tableSchema.versionKeyField])}`;
            request.Item[tableSchema.versionKeyField] = buildRequestPutItem(tableSchema, item[tableSchema.versionKeyField] + 1);
        } else {
            // If the version key isn't set then we must be putting a brand new item,
            // or versioning has just been enabled.
            request.ConditionExpression = `attribute_not_exists(${versionAttributeName})`;
            request.Item[tableSchema.versionKeyField] = buildRequestPutItem(tableSchema, 1);
        }

        if (Object.keys(expressionAttributeNames).length) {
            request.ExpressionAttributeNames = expressionAttributeNames;
        }
        if (Object.keys(expressionAttributeValues).length) {
            request.ExpressionAttributeValues = expressionAttributeValues;
        }
    }

    if (tableSchema.ttlField && Object.prototype.hasOwnProperty.call(item, tableSchema.ttlField)) {
        if (item === null || typeof item[tableSchema.ttlField] === "number") {
            // No-op because it was already serialized correctly.
            // I'm waffling on trying to re-interpret unix-epoch millisecond dates as seconds since
            // the difference is 1000.  I'm worried about the potential for confusing results though.
        } else if (item[tableSchema.ttlField] instanceof Date) {
            request.Item[tableSchema.ttlField] = buildRequestPutItem(tableSchema, Math.round((item[tableSchema.ttlField] as Date).getTime() / 1000));
        } else {
            // We could try to interpret strings as Dates and then use that, but is that really
            // a convenience?  In what scenario would someone really want to put strings here?
            // It seems easier to hit the validation error once and then use Dates.
            throw new Error("Unhandled case that should have been caught in validation.");
        }
    }

    return request;
}

/**
 * Build a request object that can be passed into `updateItem` based upon a
 * set of {@link UpdateExpressionAction}s.  Each {@link UpdateExpressionAction} defines
 * an operation to take as part of updating in the database.
 *
 * If `TableSchema.versionKeyField` is set the update will only succeed
 * if the item in the database has the same value (and then the version
 * will be incremented).
 * @param tableSchema
 * @param itemToUpdate The item being updated.  This item is only used for its
 *        keys and may already be updated.  This item will not be modified.
 * @param updateActions An array of actions to turn into an UpdateExpression.
 * @returns Input for the `updateItem` method.
 */
export function buildUpdateInputFromActions(tableSchema: TableSchema, itemToUpdate: object, ...updateActions: UpdateExpressionAction[]): aws.DynamoDB.UpdateItemInput {
    checkSchema(tableSchema);
    checkSchemaItemAgreement(tableSchema, itemToUpdate);
    if (updateActions.length === 0) {
        throw new Error("No UpdateExpressionActions specified.");
    }

    const expressionAttributeNames: aws.DynamoDB.ExpressionAttributeNameMap = {};
    const expressionAttributeValues: aws.DynamoDB.ExpressionAttributeValueMap = {};
    let conditionExpression: string = undefined;

    if (tableSchema.versionKeyField) {
        if (itemToUpdate[tableSchema.versionKeyField] == null) {
            throw new Error("The tableSchema defines a versionKeyField but itemToUpdate does not have a value for that field.  As an existing item to update it should already have a version.");
        }

        updateActions = [...updateActions, {
            action: "number_add",
            attribute: tableSchema.versionKeyField,
            value: 1
        }];
        conditionExpression = `${getExpressionAttributeName(expressionAttributeNames, tableSchema.versionKeyField)} = ${getExpressionValueName(tableSchema, expressionAttributeValues, itemToUpdate[tableSchema.versionKeyField])}`;
    }

    const setActions = updateActions
        .filter(action => getUpdateExpressionActionClauseKey(action) === "SET")
        .map(action => {
            const attributeName = getExpressionAttributeName(expressionAttributeNames, action.attribute);
            switch (action.action) {
                case "put":
                    return `${attributeName} = ${getExpressionValueName(tableSchema, expressionAttributeValues, action.value)}`;
                case "put_if_not_exists":
                    return `${attributeName} = if_not_exists(${attributeName}, ${getExpressionValueName(tableSchema, expressionAttributeValues, action.value)})`;
                case "number_add":
                    // This could also be handled by the "ADD" clause but I think that's more likely
                    // to have unexpected side-effects if the item's value is not a number.
                    return `${attributeName} = ${attributeName} + ${getExpressionValueName(tableSchema, expressionAttributeValues, action.value)}`;
                case "number_subtract":
                    return `${attributeName} = ${attributeName} - ${getExpressionValueName(tableSchema, expressionAttributeValues, action.value)}`;
                case "list_append":
                    return `${attributeName} = list_append(${attributeName}, ${getExpressionValueName(tableSchema, expressionAttributeValues, action.values)})`;
                case "list_prepend":
                    return `${attributeName} = list_append(${getExpressionValueName(tableSchema, expressionAttributeValues, action.values)}, ${attributeName})`;
                case "list_set_at_index":
                    return `${attributeName}[${action.index}] = ${getExpressionValueName(tableSchema, expressionAttributeValues, action.value)}`;
                default:
                    throw new Error(`Unhandled SET update '${action.action}'.`);
            }
        })
        .join(", ");

    const removeActions = updateActions
        .filter(action => getUpdateExpressionActionClauseKey(action) === "REMOVE")
        .map(action => {
            const attributeName = getExpressionAttributeName(expressionAttributeNames, action.attribute);
            switch (action.action) {
                case "remove":
                    return attributeName;
                case "list_remove_at_index":
                    return `${attributeName}[${action.index}]`;
                default:
                    throw new Error(`Unhandled REMOVE update '${action.action}'.`);
            }
        })
        .join(", ");

    const addActions = updateActions
        .filter(action => getUpdateExpressionActionClauseKey(action) === "ADD")
        .map(action => {
            const attributeName = getExpressionAttributeName(expressionAttributeNames, action.attribute);
            switch (action.action) {
                case "set_add":
                    return `${attributeName} ${getExpressionValueName(tableSchema, expressionAttributeValues, action.values)}`;
                default:
                    throw new Error(`Unhandled ADD update '${action.action}'.`);
            }
        })
        .join(", ");

    const deleteActions = updateActions
        .filter(action => getUpdateExpressionActionClauseKey(action) === "DELETE")
        .map(action => {
            const attributeName = getExpressionAttributeName(expressionAttributeNames, action.attribute);
            switch (action.action) {
                case "set_delete":
                    return `${attributeName} ${getExpressionValueName(tableSchema, expressionAttributeValues, action.values)}`;
                default:
                    throw new Error(`Unhandled DELETE update '${action.action}'.`);
            }
        })
        .join(", ");

    const updateExpression = [
        setActions && "SET " + setActions,
        removeActions && "REMOVE " + removeActions,
        addActions && "ADD " + addActions,
        deleteActions && "DELETE " + deleteActions
    ].filter(e => !!e).join(" ");

    const request: aws.DynamoDB.UpdateItemInput = {
        UpdateExpression: updateExpression,
        Key: getKey(tableSchema, itemToUpdate[tableSchema.partitionKeyField], tableSchema.sortKeyField && itemToUpdate[tableSchema.sortKeyField]),
        TableName: tableSchema.tableName
    };
    if (Object.keys(expressionAttributeNames).length) {
        request.ExpressionAttributeNames = expressionAttributeNames;
    }
    if (Object.keys(expressionAttributeValues).length) {
        request.ExpressionAttributeValues = expressionAttributeValues;
    }
    if (conditionExpression) {
        request.ConditionExpression = conditionExpression;
    }

    return request;
}

function getUpdateExpressionActionClauseKey(action: UpdateExpressionAction): "SET" | "REMOVE" | "ADD" | "DELETE" {
    const actions: (UpdateExpressionAction["action"])[] = ["put", "put_if_not_exists", "number_add", "number_subtract", "list_append", "list_prepend", "list_set_at_index", "remove", "list_remove_at_index", "set_add", "set_delete"];

    switch (action.action) {
        case "put":
        case "put_if_not_exists":
        case "number_add":
        case "number_subtract":
        case "list_append":
        case "list_prepend":
        case "list_set_at_index":
            return "SET";
        case "remove":
        case "list_remove_at_index":
            return "REMOVE";
        case "set_add":
            return "ADD";
        case "set_delete":
            return "DELETE";
        default:
            throw new Error(`UpdateExpression action must be one of: ${actions.join(", ")}.`);
    }
}

/**
 * Build a request object that can be passed into `deleteItem`
 *
 * If `TableSchema.versionKeyField` is set the delete will only succeed
 * if the item in the database has the same value.
 * @param tableSchema
 * @param itemToDelete The item to delete.  Must at least have the partition
 *        key, the sort key if applicable, and the version field if applicable.
 * @returns Input for the `deleteItem` method.
 */
export function buildDeleteInput(tableSchema: TableSchema, itemToDelete: object): aws.DynamoDB.DeleteItemInput {
    checkSchema(tableSchema);
    checkSchemaItemAgreement(tableSchema, itemToDelete);

    const request: aws.DynamoDB.DeleteItemInput = {
        Key: getKey(tableSchema, itemToDelete[tableSchema.partitionKeyField], tableSchema.sortKeyField && itemToDelete[tableSchema.sortKeyField]),
        TableName: tableSchema.tableName
    };

    if (tableSchema.versionKeyField) {
        const expressionAttributeNames = {};
        const expressionAttributeValues = {};
        request.ConditionExpression = `${getExpressionAttributeName(expressionAttributeNames, tableSchema.versionKeyField)} = ${getExpressionValueName(tableSchema, expressionAttributeValues, itemToDelete[tableSchema.versionKeyField])}`;
        if (Object.keys(expressionAttributeNames).length) {
            request.ExpressionAttributeNames = expressionAttributeNames;
        }
        if (Object.keys(expressionAttributeValues).length) {
            request.ExpressionAttributeValues = expressionAttributeValues;
        }
    }

    return request;
}

/**
 * Build a request object that can be passed into `query`.  The query operation performs
 * an efficient search on one partition key value with an optional condition on the sort
 * key.
 *
 * If `tableSchema.indexName` is set the query will be performed on the secondary index
 * with that name.
 * @param tableSchema
 * @param partitionKeyValue The hash key of the item to get.
 * @param sortKeyOp The operator that can be used to constrain results.  Must be one of:
 *                  `"=", "<", "<=", ">", ">=", "BETWEEN", "begins_with"`.  If not defined
 *                  all sort key values will be returned.
 * @param sortKeyValues Values the sortKeyOp works on.  This must be 2 values for
 *                      `BETWEEN` and 1 for all other operators.
 * @returns Input for the `query` method.
 */
export function buildQueryInput(tableSchema: TableSchema, partitionKeyValue: DynamoKey, sortKeyOp?: DynamoQueryConditionOperator, ...sortKeyValues: DynamoKey[]): aws.DynamoDB.QueryInput {
    checkSchema(tableSchema);
    checkSchemaPartitionKeyAgreement(tableSchema, partitionKeyValue);
    if (sortKeyOp) {
        if (!tableSchema.sortKeyField) {
            throw new Error("TableSchema doesn't define a sortKeyField but the query defines a sort key operator.");
        }
        checkCondition({
            attribute: tableSchema.sortKeyField,
            operator: sortKeyOp,
            values: sortKeyValues
        }, "query");
    }

    const queryInput: aws.DynamoDB.QueryInput = {
        TableName: tableSchema.tableName,
        ExpressionAttributeNames: {
            "#P": tableSchema.partitionKeyField
        },
        ExpressionAttributeValues: {
            ":p": buildRequestPutItem(tableSchema, partitionKeyValue)
        },
        KeyConditionExpression: `#P = :p`
    };

    if (tableSchema.indexName) {
        queryInput.IndexName = tableSchema.indexName;
    }

    if (sortKeyOp) {
        for (const val of sortKeyValues) {
            checkSchemaSortKeyAgreement(tableSchema, val);
        }
        if (sortKeyOp === "begins_with" && tableSchema.sortKeyType !== "string") {
            throw new Error("The begins_with query operator can only be used when sortKeyType is 'string'.");
        }

        queryInput.ExpressionAttributeNames["#S"] = tableSchema.sortKeyField;
        const valueNames = getExpressionValueNames(tableSchema, queryInput.ExpressionAttributeValues, sortKeyValues);

        if (sortKeyOp === "BETWEEN") {
            // This isn't worth generalizing because it's not like other operators.
            queryInput.KeyConditionExpression += ` AND #S BETWEEN ${valueNames[0]} AND ${valueNames[1]}`;
        } else if (operatorIsFunction(sortKeyOp)) {
            queryInput.KeyConditionExpression += ` AND ${sortKeyOp}(#S, ${valueNames[0]})`;
        } else {
            queryInput.KeyConditionExpression += ` AND #S ${sortKeyOp} ${valueNames[0]}`;
        }
    }

    return queryInput;
}

/**
 * Build a request object that can be passed into `scan`.  The scan operation performs
 * a linear search through all objects in the table.  It can be filtered to only return
 * some values, though all objects in the database will still be read and your account
 * billed accordingly.
 *
 * If `tableSchema.indexName` is set the scan will be performed on the secondary index
 * with that name.
 * @see addFilter
 * @param tableSchema
 * @param filters One or more filters to turn into a filter expression.
 * @returns Input for the `scan` method.
 */
export function buildScanInput(tableSchema: TableSchema, ...filters: Condition[]): aws.DynamoDB.ScanInput {
    checkSchema(tableSchema);

    const scanInput: aws.DynamoDB.ScanInput = {
        TableName: tableSchema.tableName
    };

    if (tableSchema.indexName) {
        scanInput.IndexName = tableSchema.indexName;
    }

    if (filters.length) {
        addFilter(tableSchema, scanInput, ...filters);
    }

    return scanInput;
}

/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param items The items to put.
 * @returns Input for the `batchWriteItem` method.
 */
export function buildBatchPutInput(tableSchema: TableSchema, items: object[]): aws.DynamoDB.BatchWriteItemInput {
    checkSchema(tableSchema);
    checkSchemaItemsAgreement(tableSchema, items);

    if (tableSchema.versionKeyField) {
        throw new Error("TableSchema defines a versionKeyField.  Optimistic locking can not be supported in this request.");
    }

    return {
        RequestItems: {
            [tableSchema.tableName]: items.map(item => ({
                PutRequest: {
                    Item: buildRequestPutItem(tableSchema, item).M
                }
            }))
        }
    };
}

/**
 * Build a request object that can be passed into `batchWriteItem`.
 * @param tableSchema
 * @param keyValues An array of the key values for each item to delete.
 * @returns Input for the `batchWriteItem` method.
 */
export function buildBatchDeleteInput(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.BatchWriteItemInput {
    checkSchema(tableSchema);
    checkSchemaKeysAgreement(tableSchema, keyValues);

    if (tableSchema.sortKeyType) {
        const keyPairs = keyValues as DynamoKeyPair[];
        return {
            RequestItems: {
                [tableSchema.tableName]: keyPairs.map(keyPair => ({
                    DeleteRequest: {
                        Key: {
                            [tableSchema.partitionKeyField]: buildRequestPutItem(tableSchema, keyPair[0]),
                            [tableSchema.sortKeyField]: buildRequestPutItem(tableSchema, keyPair[1])
                        }
                    }
                }))
            }
        };
    } else {
        const flatKeys = keyValues as DynamoKey[];
        return {
            RequestItems: {
                [tableSchema.tableName]: flatKeys.map(key => ({
                    DeleteRequest: {
                        Key: {
                            [tableSchema.partitionKeyField]: buildRequestPutItem(tableSchema, key)
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
 * @param keyValues An array of the key values for each item to request.
 * @returns Input for the `batchGetItem` method.
 */
export function buildBatchGetInput(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): aws.DynamoDB.BatchGetItemInput {
    checkSchema(tableSchema);
    checkSchemaKeysAgreement(tableSchema, keyValues);

    if (tableSchema.sortKeyType) {
        const keyPairs = keyValues as DynamoKeyPair[];
        return {
            RequestItems: {
                [tableSchema.tableName]: {
                    Keys: keyPairs.map(keyPair => ({
                        [tableSchema.partitionKeyField]: buildRequestPutItem(tableSchema, keyPair[0]),
                        [tableSchema.sortKeyField]: buildRequestPutItem(tableSchema, keyPair[1])
                    }))
                }
            }
        };
    } else {
        const flatKeys = keyValues as KeyType[];
        return {
            RequestItems: {
                [tableSchema.tableName]: {
                    Keys: flatKeys.map(key => ({
                        [tableSchema.partitionKeyField]: buildRequestPutItem(tableSchema, key)
                    }))
                }
            }
        };
    }
}

/**
 * Build a request object that can be passed into `transactWriteItems`.
 * @param input Any combination of inputs into `putItem`, `deleteItem` and `updateItem`.
 */
export function buildTransactWriteItemsInput(...input: (aws.DynamoDB.PutItemInput | aws.DynamoDB.DeleteItemInput | aws.DynamoDB.UpdateItemInput)[]): aws.DynamoDB.TransactWriteItemsInput {
    const request: aws.DynamoDB.TransactWriteItemsInput = {TransactItems: []};
    addTransactWriteItems(request, ...input);
    return request;
}

/**
 * Add more items to an existing TransactWriteItemsInput object.
 * @param request An existing TransactWriteItemsInput object.
 * @param input Any combination of inputs into `putItem`, `deleteItem` and `updateItem`.
 */
export function addTransactWriteItems(request: aws.DynamoDB.TransactWriteItemsInput, ...input: (aws.DynamoDB.PutItemInput | aws.DynamoDB.DeleteItemInput | aws.DynamoDB.UpdateItemInput)[]): void {
    const newItems: aws.DynamoDB.TransactWriteItemList = input.map(i => {
        if ((i as aws.DynamoDB.PutItemInput).Item) {
            return {
                Put: i as aws.DynamoDB.PutItemInput
            };
        }
        if ((i as aws.DynamoDB.UpdateItemInput).UpdateExpression) {
            return {
                Update: i as (aws.DynamoDB.UpdateItemInput & { UpdateExpression: string })
            };
        }
        if ((i as aws.DynamoDB.DeleteItemInput).Key) {
            return {
                Delete: i as aws.DynamoDB.DeleteItemInput
            };
        }
        throw new Error("Invalid input to buildTransactWriteItemsInput.  Each item must be a PutItemInput, DeleteItemInput or UpdateItemInput (with UpdateExpression set).");
    });
    request.TransactItems = [...request.TransactItems, ...newItems];
}

/**
 * Build a request object that can be passed into `createTable`.
 * This is most useful for setting up testing.
 * @param tableSchema A single TableSchema or an array of TableSchemas when including secondary indexes.
 * @param readCapacity Represents one strongly consistent read per second, or two
 *                     eventually consistent reads per second, for an item up to 4 KB in size.
 * @param writeCapacity Represents one write per second for an item up to 1 KB in size.
 * @returns Input for the `createTable` method.
 */
export function buildCreateTableInput(tableSchema: TableSchema | TableSchema[], readCapacity: number = 1, writeCapacity: number = 1): aws.DynamoDB.CreateTableInput {
    if (!Number.isInteger(readCapacity) || readCapacity < 1) {
        throw new Error("'readCapacity' must be a positive integer.");
    }
    if (!Number.isInteger(writeCapacity) || writeCapacity < 1) {
        throw new Error("'writeCapacity' must be a positive integer.");
    }

    let primarySchema: TableSchema;
    let secondarySchemas: TableSchema[];
    if (Array.isArray(tableSchema)) {
        const primarySchemaIndex = tableSchema.findIndex(t => !t.indexName);
        if (primarySchemaIndex === -1) {
            throw new Error("When passing an array of TableSchemas exactly one must be a primary schema (no 'indexName') and the other TableSchemas must be secondary indexes (with 'indexName').");
        }
        primarySchema = tableSchema[primarySchemaIndex];
        secondarySchemas = tableSchema.filter((v, i) => i !== primarySchemaIndex);
        if (secondarySchemas.find(t => !t.indexName)) {
            throw new Error("When passing an array of TableSchemas exactly one must be a primary schema (no 'indexName') and the other TableSchemas must be secondary indexes (with 'indexName').");
        }
        if (secondarySchemas.find(t => !t.indexProperties)) {
            throw new Error("One or more secondary TableSchemas are missing 'indexProperties'.");
        }
    } else {
        if (tableSchema.indexName) {
            if (tableSchema.indexName) {
                throw new Error("tableSchema.indexName is set, implying this is a schema for a secondary index.  To create a table with a secondary index pass in an array of TableSchemas and include the primary schema.");
            }
        }
        primarySchema = tableSchema;
        secondarySchemas = [];
    }

    checkSchema(primarySchema);

    const request: aws.DynamoDB.CreateTableInput = {
        AttributeDefinitions: [
            {
                AttributeName: primarySchema.partitionKeyField,
                AttributeType: jsTypeToDynamoKeyType(primarySchema.partitionKeyType)
            }
        ],
        KeySchema: [
            {
                AttributeName: primarySchema.partitionKeyField,
                KeyType: "HASH"
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: readCapacity,
            WriteCapacityUnits: writeCapacity
        },
        TableName: primarySchema.tableName,
    };

    if (primarySchema.sortKeyField) {
        request.AttributeDefinitions.push({
            AttributeName: primarySchema.sortKeyField,
            AttributeType: jsTypeToDynamoKeyType(primarySchema.sortKeyType)
        });
        request.KeySchema.push({
            AttributeName: primarySchema.sortKeyField,
            KeyType: "RANGE"
        });
    }

    for (const secondarySchema of secondarySchemas) {
        if (secondarySchema.tableName !== primarySchema.tableName) {
            throw new Error("Not all TableSchemas have the same TableName.");
        }

        if (!request.AttributeDefinitions.find(ad => ad.AttributeName === secondarySchema.partitionKeyField)) {
            request.AttributeDefinitions.push({
                AttributeName: secondarySchema.partitionKeyField,
                AttributeType: jsTypeToDynamoKeyType(secondarySchema.partitionKeyType)
            });
        }

        const requestSecondaryIndex: aws.DynamoDB.GlobalSecondaryIndex | aws.DynamoDB.LocalSecondaryIndex = {
            IndexName: secondarySchema.indexName,
            KeySchema: [
                {
                    AttributeName: secondarySchema.partitionKeyField,
                    KeyType: "HASH"
                }
            ],
            Projection: {
                ProjectionType: secondarySchema.indexProperties.projectionType
            },
            ProvisionedThroughput: {
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1
            }
        };
        if (secondarySchema.indexProperties.projectedAttributes && secondarySchema.indexProperties.projectedAttributes.length > 0) {
            requestSecondaryIndex.Projection.NonKeyAttributes = secondarySchema.indexProperties.projectedAttributes;
        }
        if (secondarySchema.sortKeyField) {
            if (!request.AttributeDefinitions.find(ad => ad.AttributeName === secondarySchema.sortKeyField)) {
                request.AttributeDefinitions.push({
                    AttributeName: secondarySchema.sortKeyField,
                    AttributeType: jsTypeToDynamoKeyType(secondarySchema.sortKeyType)
                });
            }
            requestSecondaryIndex.KeySchema.push({
                AttributeName: secondarySchema.sortKeyField,
                KeyType: "RANGE"
            });
        }

        if (secondarySchema.indexProperties.type === "GLOBAL") {
            if (request.GlobalSecondaryIndexes) {
                request.GlobalSecondaryIndexes.push(requestSecondaryIndex);
            } else {
                request.GlobalSecondaryIndexes = [requestSecondaryIndex];
            }
        } else if (secondarySchema.indexProperties.type === "LOCAL") {
            if (request.LocalSecondaryIndexes) {
                request.LocalSecondaryIndexes.push(requestSecondaryIndex);
            } else {
                request.LocalSecondaryIndexes = [requestSecondaryIndex];
            }
        }

    }

    return request;
}

/**
 * Build a request object that can be passed into `deleteTable`.
 * This is most useful for setting up testing.
 * @param tableSchema
 * @returns Input for the `deleteTable` method.
 */
export function buildDeleteTableInput(tableSchema: TableSchema): aws.DynamoDB.DeleteTableInput {
    if (tableSchema.indexName) {
        throw new Error("tableSchema.indexName is set, implying this is a schema for a secondary index.  Pass in the schema for the primary index to delete the table and all secondary indexes will be deleted with it.");
    }

    checkSchema(tableSchema);

    return {
        TableName: tableSchema.tableName,
    };
}

/**
 * Build a request object that can be passed into `updateTimeToLive`.
 * Time to live settings will be enabled if `tableSchema.ttlField`
 * is defined and disabled otherwise.
 * @param tableSchema
 * @returns Input for the `updateTimeToLive` method.
 */
export function buildUpdateTimeToLiveInput(tableSchema: TableSchema): aws.DynamoDB.UpdateTimeToLiveInput {
    checkSchema(tableSchema);

    if (tableSchema.ttlField) {
        return {
            TableName: tableSchema.tableName,
            TimeToLiveSpecification: {
                Enabled: true,
                AttributeName: tableSchema.ttlField
            }
        };
    } else {
        return {
            TableName: tableSchema.tableName,
            TimeToLiveSpecification: {
                Enabled: false,
                AttributeName: ""
            }
        };
    }
}

/**
 * Add a projection expression to an input object.  A projection expression
 * defines what attributes are returned in the result.  This can save
 * on bandwidth.
 *
 * For documentation on attribute names see: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
 * @param tableSchema
 * @param projectableRequest The input to add a projection expression to.
 * @param attributes An array of attribute names to fetch.
 * @returns A copy of projectableRequest with the projection expression set.
 */
export function addProjection<T extends { ProjectionExpression?: aws.DynamoDB.ProjectionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap }>(tableSchema: TableSchema, projectableRequest: T, attributes: string[]): void {
    checkSchema(tableSchema);

    const projection: string[] = projectableRequest.ProjectionExpression ? projectableRequest.ProjectionExpression.split(",").map(p => p.trim()) : [];
    const nameMap: aws.DynamoDB.ExpressionAttributeNameMap = {...projectableRequest.ExpressionAttributeNames};

    for (const attribute of attributes) {
        const existingName = Object.keys(nameMap).filter(name => nameMap[name] === attribute)[0];
        if (existingName) {
            if (projection.indexOf(existingName) === -1) {
                projection.push(existingName);
            }
        } else {
            let name = "#" + attribute.toUpperCase();
            while (projection.indexOf(name) !== -1) {
                name += "A";
            }
            projection.push(name);
            nameMap[name] = attribute;
        }
    }

    if (projection.length) {
        projectableRequest.ProjectionExpression = projection.join(",");
    }
    if (Object.keys(nameMap).length) {
        projectableRequest.ExpressionAttributeNames = nameMap;
    }
}

/**
 * Adds a condition expression to an input object.  A condition expression
 * defines under what conditions the item can be put/deleted.
 *
 * Any existing condition expression will be amended.
 *
 * @param tableSchema
 * @param conditionableRequest The input to add a condition expression to.
 * @param conditions One or more conditions to turn into a condition expression.
 * @returns A copy of conditionableRequest with the condition expression set.
 */
export function addCondition<T extends { ConditionExpression?: aws.DynamoDB.ConditionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap, ExpressionAttributeValues?: aws.DynamoDB.ExpressionAttributeValueMap }>(tableSchema: TableSchema, conditionableRequest: T, ...conditions: Condition[]): void {
    addExpression("ConditionExpression", tableSchema, conditionableRequest, ...conditions);
}

/**
 * Adds a filter expression to an input object.  A filter expression
 * refines results returned from a scan or query.  The filter applies
 * after the search and you will be billed for the bandwidth of all results
 * before the filter is applied.
 *
 * Any existing filter expression will be amended.
 *
 * @param tableSchema
 * @param filterableRequest The input to add a filter expression to.
 * @param filters One or more filters to turn into a filter expression.
 * @returns A copy of filterableRequest with the condition expression set.
 */
export function addFilter<T extends { FilterExpression?: aws.DynamoDB.ConditionExpression, ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap, ExpressionAttributeValues?: aws.DynamoDB.ExpressionAttributeValueMap }>(tableSchema: TableSchema, filterableRequest: T, ...filters: Condition[]): void {
    addExpression("FilterExpression", tableSchema, filterableRequest, ...filters);
}

function addExpression<T extends { ExpressionAttributeNames?: aws.DynamoDB.ExpressionAttributeNameMap, ExpressionAttributeValues?: aws.DynamoDB.ExpressionAttributeValueMap }, K extends keyof T>(expressionKey: string, tableSchema: TableSchema, conditionableRequest: T, ...conditions: Condition[]): void {
    checkSchema(tableSchema);
    checkConditions(conditions, "default");
    let exp: aws.DynamoDB.ConditionExpression = conditionableRequest[expressionKey] || undefined;
    const expressionAttributeNames: aws.DynamoDB.ExpressionAttributeNameMap = {...(conditionableRequest.ExpressionAttributeNames || {})};
    const expressionAttributeValues: aws.DynamoDB.ExpressionAttributeValueMap = {...(conditionableRequest.ExpressionAttributeValues || {})};

    for (const condition of conditions) {
        const attributeName = getExpressionAttributeName(expressionAttributeNames, condition.attribute);
        const valueNames = getExpressionValueNames(tableSchema, expressionAttributeValues, condition.values);

        if (exp) {
            exp += " AND ";
        } else {
            exp = "";
        }
        if (condition.operator === "BETWEEN") {
            // This isn't worth generalizing because it's not like other operators.
            exp += `${attributeName} BETWEEN ${valueNames[0]} AND ${valueNames[1]}`;
        } else if (condition.operator === "IN") {
            exp += `${attributeName} IN (${valueNames.join(", ")})`;
        } else if (operatorIsFunction(condition.operator)) {
            exp += `${condition.operator}(${[attributeName, ...valueNames].join(", ")})`;
        } else {
            exp += `${attributeName} ${condition.operator} ${valueNames[0]}`;
        }
    }

    if (exp) {
        conditionableRequest[expressionKey] = exp;
    }
    if (Object.keys(expressionAttributeNames).length) {
        conditionableRequest.ExpressionAttributeNames = expressionAttributeNames;
    }
    if (Object.keys(expressionAttributeValues).length) {
        conditionableRequest.ExpressionAttributeValues = expressionAttributeValues;
    }
}

/**
 * Get the serialized request key object for the partition and sort key.
 */
function getKey(tableSchema: TableSchema, partitionKeyValue: DynamoKey, sortKeyValue?: DynamoKey): aws.DynamoDB.Key {
    const key: aws.DynamoDB.Key = {
        [tableSchema.partitionKeyField]: buildRequestPutItem(tableSchema, partitionKeyValue)
    };
    if (sortKeyValue != null) {
        key[tableSchema.sortKeyField] = buildRequestPutItem(tableSchema, sortKeyValue);
    }
    return key;
}

/**
 * Get a name that is not currently used in the given ExpressionAttributeValueMap
 * and add it.
 */
function getExpressionValueName(tableSchema: TableSchema, valueMap: aws.DynamoDB.ExpressionAttributeValueMap, value: any): string {
    let name: string;
    for (let i = 0; valueMap[name = `:${indexToAlias(i, false)}`]; i++) {
        // no-op
    }
    valueMap[name] = buildRequestPutItem(tableSchema, value);
    return name;
}

/**
 * Get names for values that are not currently used in the ExpressionAttributeValueMap
 * and add them.
 */
function getExpressionValueNames(tableSchema: TableSchema, valueMap: aws.DynamoDB.ExpressionAttributeValueMap, values: any[] = []): string[] {
    const valueNames: string[] = [];
    for (let i = 0; i < values.length; i++) {
        valueNames[i] = getExpressionValueName(tableSchema, valueMap, values[i]);
    }
    return valueNames;
}

/**
 * Get the attribute name that can be used in expressions.  If it is a reserved word
 * or has a literal `.` (as indicated by a backslash)
 *
 * If a new name is required it is added to attributeMap as a side effect.
 */
function getExpressionAttributeName(attributeMap: aws.DynamoDB.ExpressionAttributeNameMap, attribute: string): string {
    const attributeParts = attribute.split(/\./);

    // 'a\\.b' was split into ['a\\', 'b'] and needs to be joined into 'a.b'
    for (let i = 0; i < attributeParts.length - 1; i++) {
        if (attributeParts[i].endsWith("\\")) {
            attributeParts.splice(i, 2, attributeParts[i].substring(0, attributeParts[i].length - 1) + "." + attributeParts[i + 1]);
            i--;
        }
    }

    return attributeParts
        .map(attributePart => {
            if (/^[a-zA-Z][^\s#:.]*$/.test(attributePart) && dynamoDbReservedWords.indexOf(attributePart.toUpperCase()) === -1) {
                // This name is clean for use as is.
                return attributePart;
            }

            const existingName = Object.keys(attributeMap).find(existingKey => attributeMap[existingKey] === attributePart);
            if (existingName) {
                return existingName;
            }

            let newName: string = null;
            for (let i = 0; attributeMap[newName = `#${indexToAlias(i, true)}`]; i++) {
                // no-op
            }
            attributeMap[newName] = attributePart;
            return newName;

        })
        .join(".");
}

/**
 * Turn an index number into an alias for a value or attribute.
 * @param ix
 * @param caps Whether to use capital letters.
 */
function indexToAlias(ix: number, caps: boolean): string {
    const asciiOffset = caps ? 65 : 97;
    if (ix < 26) {
        return String.fromCharCode(ix + asciiOffset);
    } else if (ix < 65536) {
        return String.fromCharCode(ix / 26 - 1 + asciiOffset) + String.fromCharCode(ix % 26 + asciiOffset);
    } else {
        throw new Error("Seriously?");
    }
}

function jsTypeToDynamoKeyType(t: "string" | "number"): "S" | "N" {
    switch (t) {
        case "string":
            return "S";
        case "number":
            return "N";
    }
    throw new Error("Unhandled key type.");
}
