import {TableSchema} from "./TableSchema";

export type DynamoKey = string | number;
export type DynamoKeyPair = [DynamoKey, DynamoKey];

export function checkSchema(tableSchema: TableSchema): void {
    if (!tableSchema) {
        throw new Error("Missing TableSchema.");
    }
    if (!tableSchema.tableName) {
        throw new Error("TableSchema missing tableName.");
    }
    if (!tableSchema.primaryKeyField) {
        throw new Error("TableSchema missing primaryKeyField.");
    }
    if (!tableSchema.primaryKeyType) {
        throw new Error("TableSchema missing primaryKeyType.");
    }
    if (tableSchema.primaryKeyType !== "string" && tableSchema.primaryKeyType !== "number") {
        throw new Error(`TableSchema primaryKeyType must be 'string' or 'number'.  Got ${tableSchema.primaryKeyType}.`);
    }
    if (tableSchema.sortKeyField && !tableSchema.sortKeyType) {
        throw new Error("TableSchema defines sortKeyField but missing sortKeyType.");
    }
    if (!tableSchema.sortKeyField && tableSchema.sortKeyType) {
        throw new Error("TableSchema defines sortKeyType but missing sortKeyField.");
    }
    if (tableSchema.sortKeyField && tableSchema.sortKeyType !== "string" && tableSchema.sortKeyType !== "number") {
        throw new Error(`TableSchema defines sortKeyField, sortKeyType must be 'string' or 'number'.  Got ${tableSchema.sortKeyType}.`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaKeyAgreement(tableSchema: TableSchema, primaryKeyValue: DynamoKey, sortKeyValue?: DynamoKey): void {
    if (tableSchema.sortKeyField && !sortKeyValue) {
        throw new Error("TableSchema defines a sortKeyField but the value is missing.");
    }
    if (!tableSchema.sortKeyField && sortKeyValue) {
        throw new Error("TableSchema doesn't define a sortKeyField but one was given.");
    }

    if (typeof primaryKeyValue !== tableSchema.primaryKeyType) {
        throw new Error(`TableSchema defines primaryKeyType ${tableSchema.primaryKeyType} which does not match the primaryKeyValue ${typeof primaryKeyValue}.`);
    }
    if (sortKeyValue && typeof sortKeyValue !== tableSchema.sortKeyType) {
        throw new Error(`TableSchema defines sortKeyType ${tableSchema.sortKeyType} which does not match the sortKeyValue ${typeof sortKeyValue}.`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaKeysAgreement(tableSchema: TableSchema, keyValues: DynamoKey[] | DynamoKeyPair[]): void {
    if (!Array.isArray(keyValues) || !keyValues.length) {
        throw new Error("keyValues must be a non-empty array.");
    }

    if (tableSchema.sortKeyType) {
        let i = 0;
        try {
            for (i = 0; i < keyValues.length; i++) {
                const keyPair = keyValues[i];
                if (!Array.isArray(keyPair) || keyPair.length !== 2) {
                    throw new Error("Key value must be an array of length 2.");
                }
                checkSchemaKeyAgreement(tableSchema, keyPair[0], keyPair[1]);
            }
        } catch (err) {
            throw new Error(`${err.message} Key index ${i}.`);
        }
    } else {
        let i = 0;
        try {
            for (i = 0; i < keyValues.length; i++) {
                checkSchemaKeyAgreement(tableSchema, keyValues[i] as DynamoKey);
            }
        } catch (err) {
            throw new Error(`${err.message} Key index ${i}.`);
        }
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaItemAgreement(tableSchema: TableSchema, item: Object): void {
    if (!item) {
        throw new Error("The item is null.");
    }
    if (!item[tableSchema.primaryKeyField]) {
        throw new Error(`TableSchema defines a primaryKeyField ${tableSchema.primaryKeyField} which is not on the object.`);
    }
    if (typeof item[tableSchema.primaryKeyField] !== tableSchema.primaryKeyType) {
        throw new Error(`TableSchema defines primaryKeyType ${tableSchema.primaryKeyType} which does not match the object's ${typeof item[tableSchema.primaryKeyField]}.`);
    }
    if (tableSchema.sortKeyField && !item[tableSchema.sortKeyField]) {
        throw new Error(`TableSchema defines a sortKeyField ${tableSchema.sortKeyField} which is not on the object.`);
    }
    if (tableSchema.sortKeyField && typeof item[tableSchema.sortKeyField] !== tableSchema.sortKeyType) {
        throw new Error(`TableSchema defines sortKeyType ${tableSchema.sortKeyType} which does not match the object's ${typeof item[tableSchema.sortKeyField]}.`);
    }
    if (tableSchema.versionKeyField && item[tableSchema.versionKeyField] && typeof item[tableSchema.versionKeyField] !== "number") {
        throw new Error(`TableSchema defines versionKeyField which must be a number and does not match the object's ${typeof item[tableSchema.versionKeyField]}.`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaItemsAgreement(tableSchema: TableSchema, items: Object[]): void {
    if (!Array.isArray(items) || !items.length) {
        throw new Error("items must be a non-empty array.");
    }

    let i = 0;
    try {
        for (i = 0; i < items.length; i++) {
            checkSchemaItemAgreement(tableSchema, items[i]);
        }
    } catch (err) {
        throw new Error(`${err.message} Item index ${i}.`);
    }
}