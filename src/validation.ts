import {TableSchema} from "./TableSchema";
import {Condition} from "./Condition";

export type DynamoKey = string | number;
export type DynamoKeyPair = [DynamoKey, DynamoKey];
export type DynamoQueryConditionOperator = "=" | "<" | "<=" | ">" | ">=" | "BETWEEN" | "begins_with";
const queryConditionOperators: DynamoQueryConditionOperator[] = ["=", "<", "<=", ">", ">=", "BETWEEN", "begins_with"];
export type DynamoConditionOperator =
    "="
    | "<>"
    | "<"
    | "<="
    | ">"
    | ">="
    | "BETWEEN"
    | "IN"
    | "attribute_exists"
    | "attribute_not_exists"
    | "attribute_type"
    | "begins_with"
    | "contains"
    | "size";
const conditionOperators: DynamoConditionOperator[] = ["=", "<>",  "<",  "<=",  ">",  ">=",  "BETWEEN", "IN",  "attribute_exists",  "attribute_not_exists",  "attribute_type",  "begins_with",  "contains",  "size"];
const attributeTypes: string[] = ["S", "SS", "N", "NS", "B", "BS", "BOOL", "NULL", "L", "M"];

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
    checkSchemaPrimaryKeyAgreement(tableSchema, primaryKeyValue);
    if (tableSchema.sortKeyField && sortKeyValue == null) {
        throw new Error("TableSchema defines a sortKeyField but the value is missing.");
    }
    if (!tableSchema.sortKeyField && sortKeyValue != null) {
        throw new Error("TableSchema doesn't define a sortKeyField but one was given.");
    }
    if (sortKeyValue != null) {
        checkSchemaSortKeyAgreement(tableSchema, sortKeyValue);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaPrimaryKeyAgreement(tableSchema: TableSchema, primaryKeyValue: DynamoKey): void {
    if (typeof primaryKeyValue !== tableSchema.primaryKeyType) {
        throw new Error(`TableSchema defines primaryKeyType ${tableSchema.primaryKeyType} which does not match the primaryKeyValue ${typeof primaryKeyValue}.`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaSortKeyAgreement(tableSchema: TableSchema, sortKeyValue: DynamoKey): void {
    if (sortKeyValue != null && typeof sortKeyValue !== tableSchema.sortKeyType) {
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
export function checkSchemaItemAgreement(tableSchema: TableSchema, item: object, paramName: string = "the object"): void {
    if (!item) {
        throw new Error(`${paramName} must not be null.`);
    }
    if (typeof item !== "object") {
        throw new Error(`${paramName} must be an object.`);
    }
    if (!item[tableSchema.primaryKeyField]) {
        throw new Error(`TableSchema defines a primaryKeyField ${tableSchema.primaryKeyField} which is not on ${paramName}.`);
    }
    if (typeof item[tableSchema.primaryKeyField] !== tableSchema.primaryKeyType) {
        throw new Error(`TableSchema defines primaryKeyType ${tableSchema.primaryKeyType} which does not match ${paramName}'s ${typeof item[tableSchema.primaryKeyField]}.`);
    }
    if (tableSchema.sortKeyField && !item[tableSchema.sortKeyField]) {
        throw new Error(`TableSchema defines a sortKeyField ${tableSchema.sortKeyField} which is not on ${paramName}.`);
    }
    if (tableSchema.sortKeyField && typeof item[tableSchema.sortKeyField] !== tableSchema.sortKeyType) {
        throw new Error(`TableSchema defines sortKeyType ${tableSchema.sortKeyType} which does not match ${paramName}'s ${typeof item[tableSchema.sortKeyField]}.`);
    }
    if (tableSchema.versionKeyField && item[tableSchema.versionKeyField] && typeof item[tableSchema.versionKeyField] !== "number") {
        throw new Error(`TableSchema defines versionKeyField which must be a number and does not match ${paramName}'s ${typeof item[tableSchema.versionKeyField]}.`);
    }
    if (tableSchema.ttlField && item[tableSchema.ttlField] != null && typeof item[tableSchema.ttlField] !== "number" && !(item[tableSchema.ttlField] instanceof Date)) {
        throw new Error(`TableSchema defines ttlField ${tableSchema.ttlField} which must be a number or Date and does not match ${paramName}'s ${typeof item[tableSchema.ttlField]}.`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkSchemaItemsAgreement(tableSchema: TableSchema, items: object[]): void {
    if (!Array.isArray(items) || !items.length) {
        throw new Error("items must be a non-empty array.");
    }
    for (let i = 0; i < items.length; i++) {
        checkSchemaItemAgreement(tableSchema, items[i], `items[${i}]`);
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkCondition(condition: Condition, operatorSet: "default" | "query" = "default"): void {
    if (!condition) {
        throw new Error("Condition is null.");
    }

    if (!condition.operator) {
        throw new Error("Condition operator is not defined.");
    }
    if (operatorSet === "default" && conditionOperators.indexOf(condition.operator) === -1) {
        throw new Error(`Condition operator must be one of: ${conditionOperators.join(", ")}.`);
    } else if (operatorSet === "query" && queryConditionOperators.indexOf(condition.operator as DynamoQueryConditionOperator) === -1) {
        throw new Error(`Query condition operator must be one of: ${queryConditionOperators.join(", ")}.`);
    }

    if (condition.operator === "IN") {
        if (!condition.values || !condition.values.length) {
            throw new Error("The IN operator requires at least 1 value to operate on.");
        }
    } else {
        const paramCount = operatorParamValueCount(condition.operator);
        if (paramCount !== (condition.values || []).length) {
            throw new Error(`The ${condition.operator} operator requires ${paramCount} ${paramCount === 1 ? "value" : "values"} to operate on.`);
        }
    }
    switch (condition.operator) {
        case "attribute_type": {
            if (attributeTypes.indexOf(condition.values[0]) === -1) {
                throw new Error(`The attribute_type operator requires a valid attribute type.  Valid types are: ${attributeTypes.join(", ")}.`);
            }
            break;
        }
        case "begins_with":
        case "contains":
            if (typeof condition.values[0] !== "string") {
                throw new Error(`The ${condition.operator} requires a string value.`);
            }
            break;
    }
}

/**
 * Assumes checkSchema(tableSchema) has already been run.
 */
export function checkConditions(conditions: Condition[], operatorSet: "default" | "query" = "default"): void {
    if (!Array.isArray(conditions) || !conditions.length) {
        throw new Error("conditions must be a non-empty array.");
    }

    let i: number;
    try {
        for (i = 0; i < conditions.length; i++) {
            checkCondition(conditions[i], operatorSet);
        }
    } catch (err) {
        throw new Error(`${err.message} Item index ${i}.`);
    }
}

/**
 * Get the number of value parameters required for the operator.
 */
export function operatorParamValueCount(op: DynamoConditionOperator): number {
    switch (op) {
        case "=":
        case "<>":
        case "<":
        case "<=":
        case ">":
        case ">=":
            return 1;
        case "BETWEEN":
            return 2;
        case "attribute_exists":
        case "attribute_not_exists":
            // The first param is always the attribute name, which isn't in the param values.
            return 0;
        case "attribute_type":
        case "begins_with":
        case "contains":
            return 1;
        case "size":
            return 0;
    }
    return -1;
}

/**
 * Get whether the operator is expressed as a function in the condition expression syntax.
 */
export function operatorIsFunction(op: DynamoConditionOperator): boolean {
    switch (op) {
        case "attribute_exists":
        case "attribute_not_exists":
        case "attribute_type":
        case "begins_with":
        case "contains":
        case "size":
            return true;
    }
    return false;
}
