"use strict";
function checkSchema(tableSchema) {
    if (!tableSchema) {
        throw new Error("Missing TableSchema.");
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
    if (tableSchema.sortKeyField && tableSchema.sortKeyType !== "string" && tableSchema.sortKeyType !== "number") {
        throw new Error(`TableSchema defines sortKeyField, sortKeyType must be 'string' or 'number'.  Got ${tableSchema.sortKeyType}.`);
    }
}
exports.checkSchema = checkSchema;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
function checkSchemaKeyAgreement(tableSchema, primaryKey, sortKey) {
    if (tableSchema.sortKeyField && !sortKey) {
        throw new Error("TableSchema defines a sortKeyField but the value is missing.");
    }
    if (!tableSchema.sortKeyField && sortKey) {
        throw new Error("TableSchema doesn't define a sortKeyField but one was given.");
    }
    if (typeof primaryKey !== tableSchema.primaryKeyType) {
        throw new Error(`TableSchema defines primaryKeyType ${tableSchema.primaryKeyType} which does not match the primaryKey ${typeof primaryKey}.`);
    }
    if (sortKey && typeof sortKey !== tableSchema.sortKeyType) {
        throw new Error(`TableSchema defines sortKeyType ${tableSchema.sortKeyType} which does not match the sortKey ${typeof sortKey}.`);
    }
}
exports.checkSchemaKeyAgreement = checkSchemaKeyAgreement;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
function checkSchemaKeysAgreement(tableSchema, keys) {
    if (!Array.isArray(keys) || !keys.length) {
        throw new Error("keys must be a non-empty array.");
    }
    if (tableSchema.sortKeyType) {
        let i = 0;
        try {
            for (i = 0; i < keys.length; i++) {
                const keyPair = keys[i];
                if (!Array.isArray(keyPair) || keyPair.length !== 2) {
                    throw new Error("Key must be an array of length 2.");
                }
                checkSchemaKeyAgreement(tableSchema, keyPair[0], keyPair[1]);
            }
        }
        catch (err) {
            throw new Error(`${err.message} Key index ${i}.`);
        }
    }
    else {
        let i = 0;
        try {
            for (i = 0; i < keys.length; i++) {
                checkSchemaKeyAgreement(tableSchema, keys[i]);
            }
        }
        catch (err) {
            throw new Error(`${err.message} Key index ${i}.`);
        }
    }
}
exports.checkSchemaKeysAgreement = checkSchemaKeysAgreement;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
function checkSchemaItemAgreement(tableSchema, item) {
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
    if (typeof item[tableSchema.sortKeyField] !== tableSchema.sortKeyType) {
        throw new Error(`TableSchema defines sortKeyType ${tableSchema.sortKeyType} which does not match the object's ${typeof item[tableSchema.sortKeyField]}.`);
    }
    if (tableSchema.versionKeyField && item[tableSchema.versionKeyField] && typeof item[tableSchema.versionKeyField] !== "number") {
        throw new Error(`TableSchema defines versionKeyField which must be a number and does not match the object's ${typeof item[tableSchema.versionKeyField]}.`);
    }
}
exports.checkSchemaItemAgreement = checkSchemaItemAgreement;
/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
function checkSchemaItemsAgreement(tableSchema, items) {
    if (!Array.isArray(items) || !items.length) {
        throw new Error("items must be a non-empty array.");
    }
    let i = 0;
    try {
        for (i = 0; i < items.length; i++) {
            checkSchemaItemAgreement(tableSchema, items[i]);
        }
    }
    catch (err) {
        throw new Error(`${err.message} Item index ${i}.`);
    }
}
exports.checkSchemaItemsAgreement = checkSchemaItemsAgreement;
